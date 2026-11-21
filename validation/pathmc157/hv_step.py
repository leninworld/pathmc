#!/usr/bin/env python3
"""Stdlib-only phase runner and evidence collector for PathMC #157 attempt 2."""
from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from pathlib import Path

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from hv_common import (atomic_json, build_test_command, coverage_proof, junit_result,
                       load_durations, load_json, make_plan, parse_collection,
                       sha256_file, verify_makefile_test_recipe)

BASE = "558fa990ca569708a542284e9c166dae136be4bd"
HEAD = "6665d43108a0b7f8bf68c52c60ff484c886500b6"
TREE = "325a6992dd92ce03b8fcf3c29851285b108fafc7"
STREAM_CAP = 16 * 1024 * 1024
GIT_TIMEOUT_S = 120
GIT_FETCH_TIMEOUT_S = 300
DEFAULT_RESERVE_S = 240
PHASES = {
    "freeze-docs": ("prepare", "sync", "freeze", "editable", "docs"),
    "merge-lint": ("prepare", "sync", "early-help", "early-tests", "smoke", "merge-preview", "worktree-sync", "lint", "post-lint", "full-collection"),
    "test-shard": ("prepare", "sync", "merge-preview", "worktree-sync", "full-collection", "shard-collection", "counted", "post-test"),
    "coverage-combine": ("prepare", "sync", "merge-preview", "worktree-sync", "coverage-proof", "coverage-combine", "coverage-report"),
    "post-test": ("prepare", "post-test"),
}


class CaptureError(RuntimeError):
    """A bounded capture failed and retains the exact command for evidence."""

    def __init__(self, message, argv, *, returncode=None):
        super().__init__(message)
        self.argv = list(argv)
        self.returncode = returncode


class CaptureTimeout(CaptureError):
    pass


class CaptureCap(CaptureError):
    pass


def build_make_command(target, variables, evidence=None):
    """Backward-compatible builder for non-test Make phases."""
    return ["make", target, *[f"{key}={value}" for key, value in variables]], {}


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _request(evidence, name, **extra):
    os.makedirs(evidence, exist_ok=True)
    value = {"name": name, "start_time": utc_now(), **extra}
    atomic_json(os.path.join(evidence, "request.json"), value)
    return value


def _effective_timeout(timeout_s, evidence=None, job_budget_s=None, reserve_s=None):
    effective = float(timeout_s)
    if job_budget_s is None and reserve_s is None:
        return effective
    if job_budget_s is None:
        raise CaptureError("job budget and reserve must be paired", [])
    reserve_s = DEFAULT_RESERVE_S if reserve_s is None else float(reserve_s)
    try:
        marker = load_json(os.path.join(os.path.dirname(os.path.abspath(evidence)), "job-start.json"))
        effective = min(effective, float(marker["epoch"]) + float(job_budget_s) - reserve_s - time.time())
    except CaptureError:
        raise
    except Exception as exc:
        raise CaptureError(f"insufficient job budget: {exc}", []) from exc
    if effective <= 0:
        raise CaptureError("insufficient job budget", [])
    return effective


def _capture(repo, *argv, env=None, timeout_s=None, evidence=None,
             job_budget_s=None, reserve_s=None, byte_cap=STREAM_CAP):
    """Run a command with bounded time, output, stdin, and process-group lifetime."""
    command = list(argv)
    timeout_s = timeout_s or (GIT_FETCH_TIMEOUT_S if "fetch" in command[1:] else GIT_TIMEOUT_S)
    try:
        effective = _effective_timeout(timeout_s, evidence, job_budget_s, reserve_s)
    except CaptureError as exc:
        exc.argv = command
        raise
    temp = None
    if evidence:
        calls = Path(evidence, "captures"); calls.mkdir(parents=True, exist_ok=True)
        number = len(list(calls.glob("call-*.json"))) + 1
        prefix = calls / f"call-{number:02d}"
        stdout_path, stderr_path = str(prefix) + ".stdout.log", str(prefix) + ".stderr.log"
        meta_path = str(prefix) + ".json"
    else:
        temp = tempfile.TemporaryDirectory(prefix="pathmc157-capture-")
        stdout_path, stderr_path = os.path.join(temp.name, "stdout.log"), os.path.join(temp.name, "stderr.log")
        meta_path = None
    try:
        child_env = os.environ.copy()
        if env: child_env.update(env)
        meta = _supervise(command, repo, child_env, effective, stdout_path, stderr_path, byte_cap)
        meta.update(timeout_s=timeout_s, effective_timeout_s=effective)
        if meta_path: atomic_json(meta_path, meta)
        if meta["timed_out"]:
            raise CaptureTimeout("bounded capture timed out", command, returncode=meta["exit_code"])
        if meta["capped"]:
            raise CaptureCap("bounded capture stream byte cap exceeded", command, returncode=meta["exit_code"])
        stdout = Path(stdout_path).read_text(encoding="utf-8", errors="replace").strip()
        if meta["exit_code"]:
            stderr = Path(stderr_path).read_text(encoding="utf-8", errors="replace").strip()
            raise CaptureError(f"command exited {meta['exit_code']}: {stderr}", command, returncode=meta["exit_code"])
        return stdout
    except (CaptureError, CaptureTimeout, CaptureCap):
        raise
    except Exception as exc:
        raise CaptureError(repr(exc), command) from exc
    finally:
        if temp is not None: temp.cleanup()


def _record_exception(record, exc, default_status="HARNESS_ERROR"):
    if isinstance(exc, CaptureTimeout):
        status = "TIMEOUT"
    elif isinstance(exc, CaptureCap):
        status = "HARNESS_ERROR"
    else:
        status = default_status
    record.update(status=status, error=repr(exc), traceback=traceback.format_exc())
    if isinstance(exc, CaptureError):
        record["argv"] = exc.argv
        if exc.returncode is not None: record["exit_code"] = exc.returncode


def prepare_candidate(repo, patch, evidence, runner=None, job_budget_s=None, reserve_s=None):
    record = _request(evidence, "prepare", repo=os.path.abspath(repo), patch=os.path.abspath(patch))
    try:
        def git(*args):
            if runner is not None: return runner(["git", *args], cwd=repo)
            return _capture(repo, "git", *args, evidence=evidence,
                            job_budget_s=job_budget_s, reserve_s=reserve_s)
        if git("rev-parse", "HEAD") != BASE:
            raise RuntimeError("upstream HEAD does not equal pinned base")
        git("-c", "user.name=Lenin Mookiah", "-c", "user.email=2639584+leninworld@users.noreply.github.com",
            "am", "--committer-date-is-author-date", os.path.abspath(patch))
        head, tree = git("rev-parse", "HEAD"), git("rev-parse", "HEAD^{tree}")
        if (head, tree) != (HEAD, TREE):
            raise RuntimeError(f"candidate identity mismatch: {head} {tree}")
        if git("status", "--porcelain"):
            raise RuntimeError("candidate checkout is dirty")
        try:
            git("remote", "get-url", "upstream")
        except (CaptureTimeout, CaptureCap):
            raise
        except CaptureError as exc:
            if exc.returncode is None:
                raise
            git("remote", "add", "upstream", "https://github.com/pymc-labs/pathmc.git")
        record.update(status="PASS", head=head, tree=tree)
    except Exception as exc:
        _record_exception(record, exc)
    record["finished_at"] = utc_now()
    atomic_json(os.path.join(evidence, "phase.json"), record)
    return 0 if record["status"] == "PASS" else 1


def _read_pipe(pipe, path, state, key, cap, over):
    total = 0
    with open(path, "wb") as out:
        while True:
            block = pipe.read(65536)
            if not block:
                break
            room = max(0, cap - total)
            if room:
                out.write(block[:room])
            total += len(block)
            if total > cap:
                over.set()
    state[key] = total


def _kill_tree(proc):
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


def _supervise(argv, cwd, env, timeout_s, stdout_path, stderr_path, byte_cap):
    """Shared bounded subprocess engine for phases and captures."""
    started = time.monotonic(); state = {}; over = threading.Event(); timed_out = False
    proc = subprocess.Popen(argv, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            start_new_session=True)
    threads = [threading.Thread(target=_read_pipe, args=(stream, path, state, key, byte_cap, over), daemon=True)
               for stream, path, key in ((proc.stdout, stdout_path, "stdout"), (proc.stderr, stderr_path, "stderr"))]
    for thread in threads: thread.start()
    deadline = time.monotonic() + timeout_s
    while proc.poll() is None:
        if over.is_set():
            _kill_tree(proc); break
        if time.monotonic() >= deadline:
            timed_out = True; _kill_tree(proc); break
        time.sleep(.05)
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        _kill_tree(proc); proc.wait(timeout=15)
    for thread in threads: thread.join(timeout=15)
    capped = any(state.get(key, 0) > byte_cap for key in ("stdout", "stderr"))
    return {"argv": list(argv), "cwd": os.path.abspath(cwd), "byte_cap": byte_cap,
            "exit_code": proc.returncode, "timed_out": timed_out, "capped": capped,
            "stdout_bytes": state.get("stdout", 0), "stderr_bytes": state.get("stderr", 0),
            "elapsed_s": round(time.monotonic() - started, 3)}


def mark_start(evidence):
    os.makedirs(evidence, exist_ok=True)
    atomic_json(os.path.join(evidence, "job-start.json"), {"epoch": time.time(), "started_at": utc_now()})
    return 0


def run_phase(name, cwd, timeout_s, evidence, argv, env_delta=None, byte_cap=STREAM_CAP,
              job_budget_s=None, reserve_s=None):
    env_delta = dict(env_delta or {})
    try:
        effective = _effective_timeout(timeout_s, evidence, job_budget_s, reserve_s)
        budget_error = None
    except CaptureError as exc:
        effective = 0
        budget_error = exc
    phase = _request(evidence, name, argv=list(argv), cwd=os.path.abspath(cwd), env_deltas=env_delta,
                     timeout_s=timeout_s, requested_timeout_s=timeout_s, effective_timeout_s=effective,
                     job_budget_s=job_budget_s, reserve_s=reserve_s, byte_cap=byte_cap)
    stdout, stderr = os.path.join(evidence, "stdout.log"), os.path.join(evidence, "stderr.log")
    started = time.monotonic()
    try:
        if budget_error:
            raise budget_error
        env = os.environ.copy(); env.update(env_delta)
        result = _supervise(argv, cwd, env, effective, stdout, stderr, byte_cap)
        phase.update(result)
        if result["timed_out"]: phase["status"] = "TIMEOUT"
        elif result["capped"]: phase.update(status="HARNESS_ERROR", error="stream byte cap exceeded")
        elif result["exit_code"] == 0: phase["status"] = "PASS"
        else: phase["status"] = "NONZERO_RECORDED"
    except Exception as exc:
        for path in (stdout, stderr): Path(path).touch(exist_ok=True)
        phase.update(status="HARNESS_ERROR", error=repr(exc), traceback=traceback.format_exc())
    phase.update(elapsed_s=round(time.monotonic() - started, 3), finished_at=utc_now())
    atomic_json(os.path.join(evidence, "phase.json"), phase)
    return 0 if phase["status"] == "PASS" else 1


def _load_check_pr(repo):
    path = os.path.join(repo, "scripts", "check_pr.py")
    spec = importlib.util.spec_from_file_location("pathmc_candidate_check_pr", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def merge_preview_inner(repo, worktree, evidence, module=None, git_capture=None):
    """Perform merge preview inside the outer phase's supervised child process."""
    try:
        module = module or _load_check_pr(repo)
        # These calls deliberately inherit the inner process group.  The outer
        # merge-preview supervisor owns the timeout/cap and kills this whole tree.
        def child_git(*args):
            return subprocess.run(["git", *args], cwd=repo, stdin=subprocess.DEVNULL,
                                  text=True, capture_output=True, check=True).stdout.strip()
        git_capture = git_capture or child_git
        head = module._clean_head(Path(repo))
        url = git_capture("remote", "get-url", "upstream")
        base = module._fetch(Path(repo), url, "refs/heads/main")
        date = git_capture("show", "-s", "--format=%cI", head)
        old_author, old_committer = os.environ.get("GIT_AUTHOR_DATE"), os.environ.get("GIT_COMMITTER_DATE")
        os.environ["GIT_AUTHOR_DATE"] = os.environ["GIT_COMMITTER_DATE"] = date
        try:
            merge = module._preview(Path(repo), base, head)
        finally:
            for key, old in (("GIT_AUTHOR_DATE", old_author), ("GIT_COMMITTER_DATE", old_committer)):
                if old is None: os.environ.pop(key, None)
                else: os.environ[key] = old
        tree = git_capture("rev-parse", f"{merge}^{{tree}}")
        report = {"pr": None, "repository": "upstream", "target_branch": "main", "mode": "local merge preview",
                  "head": head, "base": base, "merge": merge, "tree": tree, "status": "running"}
        receipt = os.path.join(repo, ".scratch", "ci-preflight", "last-run.json")
        atomic_json(receipt, report)
        os.makedirs(os.path.dirname(worktree), exist_ok=True)
        git_capture("worktree", "add", "--detach", os.path.abspath(worktree), merge)
        atomic_json(os.path.join(evidence, "receipt.json"), report)
        return 0
    except Exception as exc:
        value = {"error": repr(exc), "traceback": traceback.format_exc()}
        if isinstance(exc, CaptureError): value["argv"] = exc.argv
        atomic_json(os.path.join(evidence, "inner-error.json"), value)
        return 1


def merge_preview(repo, worktree, evidence, job_budget_s=None, reserve_s=None,
                  timeout_s=600, module=None, git_capture=None):
    # Dependency injection is retained for the small unit test; production always
    # uses the isolated child so check_pr's own subprocess.run calls are bounded.
    if module is not None or git_capture is not None:
        _request(evidence, "merge-preview", repo=os.path.abspath(repo), worktree=os.path.abspath(worktree))
        rc = merge_preview_inner(repo, worktree, evidence, module, git_capture)
        record = load_json(os.path.join(evidence, "request.json"))
        if rc == 0:
            record["status"] = "PASS"
        else:
            record.update(status="NONZERO_RECORDED", **load_json(os.path.join(evidence, "inner-error.json")))
        record["finished_at"] = utc_now(); atomic_json(os.path.join(evidence, "phase.json"), record)
        return rc
    argv = [sys.executable, "-I", "-B", os.path.abspath(__file__), "merge-preview-inner",
            "--repo", os.path.abspath(repo), "--worktree", os.path.abspath(worktree),
            "--evidence", os.path.abspath(evidence)]
    return run_phase("merge-preview", repo, timeout_s, evidence, argv,
                     job_budget_s=job_budget_s, reserve_s=reserve_s)


def collect(repo, evidence, durations, files, plan_n=6, name="full-collection",
            job_budget_s=None, reserve_s=None):
    args = ["uv", "run", "--locked", "--all-extras", "pytest", "--collect-only", "-qq", "-m", "not nightly"]
    args.extend(files)
    rc = run_phase(name, repo, 600, evidence, args,
                   {"OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1"},
                   job_budget_s=job_budget_s, reserve_s=reserve_s)
    if rc: return rc
    phase = load_json(os.path.join(evidence, "phase.json"))
    try:
        nodes = parse_collection(Path(evidence, "stdout.log").read_bytes())
        atomic_json(os.path.join(evidence, "collection.json"), {"nodeids": nodes, "count": len(nodes)})
        if not files:
            atomic_json(os.path.join(evidence, "shard-plan.json"), make_plan(nodes, load_durations(durations), plan_n))
        phase["status"] = "PASS"
    except Exception as exc:
        phase.update(status="HARNESS_ERROR", error=repr(exc), traceback=traceback.format_exc())
    atomic_json(os.path.join(evidence, "phase.json"), phase)
    return 0 if phase["status"] == "PASS" else 1


def test_run(repo, evidence, shard, files, name="counted", smoke=False,
             job_budget_s=None, reserve_s=None):
    _request(evidence, name, files=files, shard=shard)
    try:
        verify_makefile_test_recipe(os.path.join(repo, "Makefile"))
    except Exception as exc:
        phase = {**load_json(os.path.join(evidence, "request.json")), "status": "HARNESS_ERROR",
                 "error": repr(exc), "traceback": traceback.format_exc(), "finished_at": utc_now()}
        atomic_json(os.path.join(evidence, "phase.json"), phase); return 1
    argv, env = build_test_command(files, os.path.join(evidence, "junit.xml"), shard=shard, evidence=evidence)
    if smoke:
        return run_phase(name, repo, 180, evidence, argv, env,
                         job_budget_s=job_budget_s, reserve_s=reserve_s)
    return run_phase(name, repo, 2700, evidence, argv, env,
                     job_budget_s=job_budget_s or 3180,
                     reserve_s=DEFAULT_RESERVE_S if reserve_s is None else reserve_s)


def plan_files(plan_path, shard):
    plan = load_json(plan_path)
    selected = plan["shards"][int(shard) - 1]
    if selected["index"] != int(shard):
        raise ValueError("plan shard index mismatch")
    for path in selected["files"]:
        print(path)
    return 0


def final_receipt(downloads, repo, evidence, job_budget_s=None, reserve_s=None):
    record = _request(evidence, "post-test", downloads=os.path.abspath(downloads))
    try:
        run_id = os.environ.get("GITHUB_RUN_ID", "RUN")
        names = ["merge-lint"] + [f"test-shard-{i}" for i in range(1, 7)]
        receipts = {}
        raw = Path(evidence, "raw-inputs"); raw.mkdir(parents=True, exist_ok=True)
        for name in names:
            root = Path(downloads, f"pathmc157-hosted-{name}-{run_id}")
            shutil.copy2(root / "merge-preview" / "receipt.json", raw / f"{name}-receipt.json")
            receipts[name] = load_json(root / "merge-preview" / "receipt.json")
        bases = {x["base"] for x in receipts.values()}; trees = {x["tree"] for x in receipts.values()}
        if bases != {BASE}: raise RuntimeError(f"base receipts differ: {bases}")
        if trees != {TREE}: raise RuntimeError(f"merge trees differ: {trees}")
        capture = lambda *args: _capture(repo, "git", *args, evidence=evidence,
                                         job_budget_s=job_budget_s, reserve_s=reserve_s)
        url = capture("remote", "get-url", "upstream")
        capture("fetch", "--no-tags", url, "refs/heads/main")
        if capture("rev-parse", "FETCH_HEAD") != BASE:
            raise RuntimeError("final base ref differs")
        jobs = {name: {"base": value["base"], "merge": value["merge"], "tree": value["tree"], "status": "passed"}
                for name, value in receipts.items()}
        final = {"pr": None, "repository": "upstream", "target_branch": "main", "mode": "local merge preview",
                 "head": HEAD, "base": BASE, "merge": receipts["merge-lint"]["merge"], "tree": TREE,
                 "status": "passed", "jobs": jobs}
        atomic_json(os.path.join(evidence, "last-run.json"), final)
        record.update(status="PASS", receipt=final)
    except Exception as exc:
        _record_exception(record, exc, "NONZERO_RECORDED")
    record["finished_at"] = utc_now(); atomic_json(os.path.join(evidence, "phase.json"), record)
    return 0 if record["status"] == "PASS" else 1


def checks(repo, worktree, evidence, receipt, final=False, job_budget_s=None, reserve_s=None):
    record = _request(evidence, "post-test" if final else "post-lint")
    try:
        data = load_json(receipt)
        worktree_capture = lambda *args: _capture(worktree, "git", *args, evidence=evidence,
                                                  job_budget_s=job_budget_s, reserve_s=reserve_s)
        repo_capture = lambda *args: _capture(repo, "git", *args, evidence=evidence,
                                              job_budget_s=job_budget_s, reserve_s=reserve_s)
        if worktree_capture("status", "--porcelain"):
            raise RuntimeError("worktree dirty")
        if worktree_capture("rev-parse", "HEAD") != data["merge"]:
            raise RuntimeError("worktree HEAD changed")
        if final:
            if repo_capture("status", "--porcelain") or repo_capture("rev-parse", "HEAD") != HEAD:
                raise RuntimeError("root changed")
            url = repo_capture("remote", "get-url", "upstream")
            repo_capture("fetch", "--no-tags", url, "refs/heads/main")
            if repo_capture("rev-parse", "FETCH_HEAD") != data["base"]:
                raise RuntimeError("base advanced")
        record.update(status="PASS", receipt=data, clean=True)
    except Exception as exc:
        _record_exception(record, exc, "NONZERO_RECORDED")
    record["finished_at"] = utc_now(); atomic_json(os.path.join(evidence, "phase.json"), record)
    return 0 if record["status"] == "PASS" else 1


def proof(downloads, evidence, job_budget_s=None, reserve_s=None):
    record = _request(evidence, "coverage-proof", downloads=os.path.abspath(downloads),
                      job_budget_s=job_budget_s, reserve_s=reserve_s)
    try:
        if job_budget_s is not None or reserve_s is not None:
            record["effective_timeout_s"] = _effective_timeout(600, evidence, job_budget_s, reserve_s)
        roots = [Path(downloads, f"pathmc157-hosted-merge-lint-{os.environ.get('GITHUB_RUN_ID','RUN')}")]
        roots += [Path(downloads, f"pathmc157-hosted-test-shard-{i}-{os.environ.get('GITHUB_RUN_ID','RUN')}") for i in range(1, 7)]
        raw = Path(evidence, "raw-inputs"); raw.mkdir(parents=True, exist_ok=True)
        for index, root in enumerate(roots):
            shutil.copy2(root / "full-collection" / "collection.json", raw / f"full-{index}.json")
            shutil.copy2(root / "merge-preview" / "receipt.json", raw / f"receipt-{index}.json")
            if index:
                shutil.copy2(root / "shard-collection" / "collection.json", raw / f"shard-{index}.json")
                shutil.copy2(root / "counted" / "junit.xml", raw / f"junit-{index}.xml")
        full_sets = [load_json(root / "full-collection" / "collection.json")["nodeids"] for root in roots]
        shard_sets = [load_json(root / "shard-collection" / "collection.json")["nodeids"] for root in roots[1:]]
        junits = [junit_result(root / "counted" / "junit.xml") for root in roots[1:]]
        result = coverage_proof(full_sets, shard_sets, junits)
        atomic_json(os.path.join(evidence, "coverage-proof.json"), result)
        record.update(status="PASS", proof=result)
    except CaptureError as exc:
        _record_exception(record, exc)
    except Exception as exc:
        record.update(status="NONZERO_RECORDED", error=repr(exc), traceback=traceback.format_exc())
    record["finished_at"] = utc_now(); atomic_json(os.path.join(evidence, "phase.json"), record)
    return 0 if record["status"] == "PASS" else 1


def summarize(root, job):
    summary = {"schema": "pathmc157-summary/2", "job": job, "generated_at": utc_now(), "phases": {}}
    for name in PHASES[job]:
        try:
            data = load_json(os.path.join(root, name, "phase.json")); status = data["status"]
            if status not in ("PASS", "NONZERO_RECORDED", "TIMEOUT", "HARNESS_ERROR"): raise ValueError("bad status")
            summary["phases"][name] = {"status": status, "phase": data}
        except Exception as exc:
            summary["phases"][name] = {"status": "HARNESS_ERROR", "error": repr(exc)}
    summary["job_outcome"] = "COMPLETE" if all(x["status"] == "PASS" for x in summary["phases"].values()) else "HARNESS_ERROR"
    atomic_json(os.path.join(root, "summary.json"), summary)
    rows = []
    for current, dirs, files in os.walk(root, followlinks=False):
        dirs[:] = sorted(x for x in dirs if not os.path.islink(os.path.join(current, x)))
        for name in sorted(files):
            path = os.path.join(current, name); rel = os.path.relpath(path, root).replace(os.sep, "/")
            if rel != "sha256.txt" and not os.path.islink(path): rows.append(f"{sha256_file(path)}  {rel}")
    Path(root, "sha256.txt").write_text("\n".join(rows) + "\n", encoding="utf-8")
    return 0 if summary["job_outcome"] == "COMPLETE" else 1


def record_freeze(repo, evidence, job_budget_s=None, reserve_s=None):
    record = _request(evidence, "freeze-output", repo=os.path.abspath(repo))
    try:
        capture = lambda *args: _capture(repo, "git", *args, evidence=evidence,
                                         job_budget_s=job_budget_s, reserve_s=reserve_s)
        status = capture("status", "--porcelain", "--untracked-files=all")
        diff = capture("diff", "--binary")
        Path(evidence, "status.txt").write_text(status + ("\n" if status else ""), encoding="utf-8")
        Path(evidence, "diff.patch").write_text(diff + ("\n" if diff else ""), encoding="utf-8")
        paths = []
        for line in status.splitlines():
            rel = line[3:].split(" -> ")[-1]; paths.append(rel); src = Path(repo, rel)
            if src.is_file():
                dst = Path(evidence, "files", rel); dst.parent.mkdir(parents=True, exist_ok=True); shutil.copy2(src, dst)
        atomic_json(Path(evidence, "changed-paths.json"), paths); record["status"] = "PASS"
    except Exception as exc:
        _record_exception(record, exc)
        atomic_json(Path(evidence, "capture-error.json"), {"error": repr(exc), "traceback": traceback.format_exc()})
    record["finished_at"] = utc_now(); atomic_json(Path(evidence, "phase.json"), record)
    return 0 if record["status"] == "PASS" else 1


def main():
    p = argparse.ArgumentParser(); sub = p.add_subparsers(dest="command", required=True)
    def budget_options(parser):
        parser.add_argument("--job-budget-s", type=float)
        parser.add_argument("--reserve-s", type=float, default=None)
        return parser
    q=sub.add_parser("mark-start"); q.add_argument("--evidence", required=True)
    q=budget_options(sub.add_parser("prepare-candidate")); q.add_argument("--repo", required=True); q.add_argument("--patch", required=True); q.add_argument("--evidence", required=True)
    q=sub.add_parser("run-phase"); q.add_argument("--name", required=True); q.add_argument("--cwd", required=True); q.add_argument("--timeout-s", type=float, required=True); q.add_argument("--job-budget-s", type=float); q.add_argument("--reserve-s", type=float); q.add_argument("--evidence", required=True); q.add_argument("--env", action="append", default=[]); q.add_argument("--make-target"); q.add_argument("--make-var", action="append", default=[]); q.add_argument("argv", nargs=argparse.REMAINDER)
    q=budget_options(sub.add_parser("merge-preview")); q.add_argument("--repo", required=True); q.add_argument("--worktree", required=True); q.add_argument("--evidence", required=True); q.add_argument("--timeout-s", type=float, default=600)
    q=sub.add_parser("merge-preview-inner"); q.add_argument("--repo", required=True); q.add_argument("--worktree", required=True); q.add_argument("--evidence", required=True)
    q=budget_options(sub.add_parser("collect")); q.add_argument("--repo", required=True); q.add_argument("--evidence", required=True); q.add_argument("--durations", required=True); q.add_argument("--plan-n", type=int, default=6); q.add_argument("--name", default="full-collection"); q.add_argument("--file", action="append", default=[])
    q=budget_options(sub.add_parser("test-run")); q.add_argument("--repo", required=True); q.add_argument("--evidence", required=True); q.add_argument("--shard", required=True); q.add_argument("--name", default="counted"); q.add_argument("--smoke", action="store_true"); q.add_argument("--file", action="append", default=[])
    q=sub.add_parser("plan-files"); q.add_argument("--plan", required=True); q.add_argument("--shard", required=True)
    q=budget_options(sub.add_parser("checks")); q.add_argument("--repo", required=True); q.add_argument("--worktree", required=True); q.add_argument("--evidence", required=True); q.add_argument("--receipt", required=True); q.add_argument("--final", action="store_true")
    q=budget_options(sub.add_parser("coverage-proof")); q.add_argument("--downloads", required=True); q.add_argument("--evidence", required=True)
    q=budget_options(sub.add_parser("final-receipt")); q.add_argument("--downloads", required=True); q.add_argument("--repo", required=True); q.add_argument("--evidence", required=True)
    q=sub.add_parser("summarize"); q.add_argument("--evidence", required=True); q.add_argument("--job", required=True, choices=tuple(PHASES))
    q=budget_options(sub.add_parser("record-freeze")); q.add_argument("--repo", required=True); q.add_argument("--evidence", required=True)
    a=p.parse_args()
    if os.environ.get("PATHMC157_PARSE_ONLY") == "1":
        evidence = getattr(a, "evidence", None)
        if evidence:
            _request(evidence, "parse-only", command=a.command)
        return 0
    if a.command=="mark-start": return mark_start(a.evidence)
    if a.command=="prepare-candidate": return prepare_candidate(a.repo,a.patch,a.evidence,job_budget_s=a.job_budget_s,reserve_s=a.reserve_s)
    if a.command=="merge-preview": return merge_preview(a.repo,a.worktree,a.evidence,a.job_budget_s,a.reserve_s,a.timeout_s)
    if a.command=="merge-preview-inner": return merge_preview_inner(a.repo,a.worktree,a.evidence)
    if a.command=="collect": return collect(a.repo,a.evidence,a.durations,a.file,a.plan_n,a.name,a.job_budget_s,a.reserve_s)
    if a.command=="test-run": return test_run(a.repo,a.evidence,a.shard,a.file,a.name,a.smoke,a.job_budget_s,a.reserve_s)
    if a.command=="plan-files": return plan_files(a.plan,a.shard)
    if a.command=="checks": return checks(a.repo,a.worktree,a.evidence,a.receipt,a.final,a.job_budget_s,a.reserve_s)
    if a.command=="coverage-proof": return proof(a.downloads,a.evidence,a.job_budget_s,a.reserve_s)
    if a.command=="final-receipt": return final_receipt(a.downloads,a.repo,a.evidence,a.job_budget_s,a.reserve_s)
    if a.command=="summarize": return summarize(a.evidence,a.job)
    if a.command=="record-freeze": return record_freeze(a.repo,a.evidence,a.job_budget_s,a.reserve_s)
    env=dict(item.split("=",1) for item in a.env)
    if a.make_target:
        argv, extra = build_make_command(a.make_target, [item.split("=",1) for item in a.make_var], a.evidence); env.update(extra)
    else:
        argv=a.argv[1:] if a.argv[:1]==["--"] else a.argv
    return run_phase(a.name,a.cwd,a.timeout_s,a.evidence,argv,env,job_budget_s=a.job_budget_s,reserve_s=a.reserve_s)


if __name__ == "__main__":
    raise SystemExit(main())
