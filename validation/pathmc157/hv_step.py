#!/usr/bin/env python3
"""Stdlib-only phase runner and evidence collector for PathMC #157."""
import argparse
import datetime as dt
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from hv_common import atomic_json, load_json, sha256_file

BASE = "558fa990ca569708a542284e9c166dae136be4bd"
HEAD = "6665d43108a0b7f8bf68c52c60ff484c886500b6"
TREE = "325a6992dd92ce03b8fcf3c29851285b108fafc7"
STREAM_CAP = 16 * 1024 * 1024

def utc_now(): return dt.datetime.now(dt.timezone.utc).isoformat()

def build_make_command(target, variables, evidence):
    """The sole builder used for every make phase."""
    argv = ["make", target] + [f"{key}={value}" for key, value in variables]
    env = {}
    if target == "check-merge":
        env["PYTEST_ADDOPTS"] = f"--junitxml={os.path.abspath(evidence)}/junit.xml"
    return argv, env

def _git(runner, repo, *args):
    return runner(["git", *args], cwd=repo)

def prepare_candidate(repo, patch, evidence, runner=None):
    """Apply and verify the candidate; runner is injectable for fake tests."""
    runner = runner or (lambda argv, cwd: subprocess.run(argv, cwd=cwd, text=True,
                    capture_output=True, check=True).stdout.strip())
    record = {"started_at": utc_now(), "repo": os.path.abspath(repo)}
    try:
        if _git(runner, repo, "rev-parse", "HEAD") != BASE:
            raise RuntimeError("upstream HEAD does not equal pinned base")
        _git(runner, repo, "-c", "user.name=Lenin Mookiah", "-c",
             "user.email=2639584+leninworld@users.noreply.github.com", "am",
             "--committer-date-is-author-date", os.path.abspath(patch))
        record["head"] = _git(runner, repo, "rev-parse", "HEAD")
        record["tree"] = _git(runner, repo, "rev-parse", "HEAD^{tree}")
        if record["head"] != HEAD: raise RuntimeError("candidate HEAD mismatch")
        if record["tree"] != TREE: raise RuntimeError("candidate tree mismatch")
        if _git(runner, repo, "status", "--porcelain"):
            raise RuntimeError("candidate checkout is dirty")
        try: _git(runner, repo, "remote", "get-url", "upstream")
        except Exception:
            _git(runner, repo, "remote", "add", "upstream", "https://github.com/pymc-labs/pathmc.git")
        record["status"] = "PASS"
        rc = 0
    except Exception as exc:
        record.update(status="HARNESS_ERROR", error=repr(exc), traceback=traceback.format_exc())
        rc = 1
    record["finished_at"] = utc_now()
    atomic_json(os.path.join(evidence, "phase.json"), record)
    return rc

def _read_pipe(pipe, path, state, key, cap, over):
    total = 0
    with open(path, "wb") as out:
        while True:
            block = pipe.read(65536)
            if not block: break
            room = max(0, cap - total)
            if room: out.write(block[:room])
            total += len(block)
            if total > cap: over.set()
    state[key] = total

def _kill_tree(proc):
    if os.name == "nt":
        subprocess.run(["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    else:
        try: os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError): pass

def mark_start(evidence):
    os.makedirs(evidence, exist_ok=True)
    atomic_json(os.path.join(evidence, "job-start.json"),
                {"epoch": time.time(), "started_at": utc_now()})
    return 0

def run_phase(name, cwd, timeout_s, evidence, argv, env_delta=None, byte_cap=STREAM_CAP,
              job_budget_s=None, reserve_s=None):
    os.makedirs(evidence, exist_ok=True)
    env_delta = dict(env_delta or {})
    requested_timeout_s = timeout_s
    effective_timeout_s = requested_timeout_s
    budget_error = None
    if job_budget_s is not None or reserve_s is not None:
        try:
            if job_budget_s is None or reserve_s is None:
                raise ValueError("job budget and reserve must be provided together")
            marker = load_json(os.path.join(os.path.dirname(os.path.abspath(evidence)),
                                            "job-start.json"))
            start_epoch = float(marker["epoch"])
            effective_timeout_s = min(requested_timeout_s,
                                      start_epoch + job_budget_s - reserve_s - time.time())
            if effective_timeout_s < 60:
                budget_error = "insufficient job budget"
        except Exception as exc:
            budget_error = f"insufficient job budget: {exc}"
    request = {"name": name, "argv": list(argv), "cwd": os.path.abspath(cwd),
               "env_deltas": env_delta, "start_time": utc_now(),
               "timeout_s": requested_timeout_s,
               "requested_timeout_s": requested_timeout_s,
               "effective_timeout_s": effective_timeout_s,
               "job_budget_s": job_budget_s, "reserve_s": reserve_s,
               "byte_cap": byte_cap}
    atomic_json(os.path.join(evidence, "request.json"), request)
    stdout = os.path.join(evidence, "stdout.log")
    stderr = os.path.join(evidence, "stderr.log")
    started = time.monotonic(); proc = None; state = {}; phase = dict(request)
    try:
        if budget_error:
            raise RuntimeError(budget_error)
        env = os.environ.copy(); env.update(env_delta)
        proc = subprocess.Popen(argv, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False,
            start_new_session=(os.name != "nt"),
            creationflags=(subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0))
        over = threading.Event()
        threads = [threading.Thread(target=_read_pipe, args=(proc.stdout, stdout, state, "stdout", byte_cap, over), daemon=True),
                   threading.Thread(target=_read_pipe, args=(proc.stderr, stderr, state, "stderr", byte_cap, over), daemon=True)]
        for thread in threads: thread.start()
        timed_out = False; deadline = time.monotonic() + effective_timeout_s
        while proc.poll() is None:
            if over.is_set(): _kill_tree(proc); break
            if time.monotonic() >= deadline: timed_out = True; _kill_tree(proc); break
            time.sleep(.03)
        if proc.poll() is None: proc.wait(timeout=15)
        for thread in threads: thread.join(timeout=15)
        capped = any(state.get(key, 0) > byte_cap for key in ("stdout", "stderr"))
        phase.update(exit_code=proc.returncode, timed_out=timed_out, capped=capped,
                     stdout_bytes=state.get("stdout", 0), stderr_bytes=state.get("stderr", 0))
        if timed_out: phase["status"] = "TIMEOUT"
        elif capped: phase.update(status="HARNESS_ERROR", error="stream byte cap exceeded")
        elif proc.returncode == 0: phase["status"] = "PASS"
        else: phase["status"] = "NONZERO_RECORDED"
    except Exception as exc:
        if proc is not None: _kill_tree(proc)
        for path in (stdout, stderr):
            if not os.path.exists(path): open(path, "ab").close()
        phase.update(status="HARNESS_ERROR", error=repr(exc), traceback=traceback.format_exc())
    phase["elapsed_s"] = round(time.monotonic() - started, 3)
    phase["finished_at"] = utc_now()
    atomic_json(os.path.join(evidence, "phase.json"), phase)
    return 0 if phase["status"] == "PASS" else 1

EXPECTED = {"check-merge": ("prepare", "sync", "early-help", "early-tests", "smoke", "counted"),
            "freeze-docs": ("prepare", "sync", "freeze", "editable", "docs")}

def summarize(root, job):
    os.makedirs(root, exist_ok=True)
    summary = {"schema": "pathmc157-summary/1", "job": job, "generated_at": utc_now(), "phases": {}}
    try:
        for name in EXPECTED.get(job, ()):
            try:
                data = load_json(os.path.join(root, name, "phase.json"))
                status = data.get("status")
                if status not in ("PASS", "NONZERO_RECORDED", "TIMEOUT", "HARNESS_ERROR"):
                    raise ValueError("invalid status")
                summary["phases"][name] = {"status": status, "phase": data}
            except Exception as exc:
                summary["phases"][name] = {"status": "HARNESS_ERROR", "error": repr(exc)}
        summary["job_outcome"] = "COMPLETE" if all(x["status"] == "PASS" for x in summary["phases"].values()) else "HARNESS_ERROR"
    except Exception as exc:
        summary.update(job_outcome="HARNESS_ERROR", exception=repr(exc), traceback=traceback.format_exc())
    atomic_json(os.path.join(root, "summary.json"), summary)
    rows = []
    for current, dirs, files in os.walk(root, followlinks=False):
        dirs[:] = sorted(d for d in dirs if not os.path.islink(os.path.join(current, d)))
        for name in sorted(files):
            path = os.path.join(current, name); rel = os.path.relpath(path, root).replace(os.sep, "/")
            if rel != "sha256.txt" and not os.path.islink(path): rows.append(f"{sha256_file(path)}  {rel}")
    with open(os.path.join(root, "sha256.txt"), "w", encoding="utf-8", newline="\n") as out:
        out.write("\n".join(rows) + "\n")
    return 0 if summary["job_outcome"] == "COMPLETE" else 1

def record_freeze(repo, evidence):
    os.makedirs(evidence, exist_ok=True)
    def capture(args): return subprocess.run(args, cwd=repo, text=True, capture_output=True, check=True).stdout
    try:
        status = capture(["git", "status", "--porcelain", "--untracked-files=all"])
        diff = capture(["git", "diff", "--binary"])
        open(os.path.join(evidence, "status.txt"), "w").write(status)
        open(os.path.join(evidence, "diff.patch"), "w").write(diff)
        paths = []
        for line in status.splitlines():
            rel = line[3:]
            if " -> " in rel: rel = rel.split(" -> ", 1)[1]
            paths.append(rel)
            src = os.path.join(repo, rel)
            if os.path.isfile(src):
                dst = os.path.join(evidence, "files", rel); os.makedirs(os.path.dirname(dst), exist_ok=True); shutil.copy2(src, dst)
        atomic_json(os.path.join(evidence, "changed-paths.json"), paths)
        return 0
    except Exception:
        atomic_json(os.path.join(evidence, "capture-error.json"), {"traceback": traceback.format_exc()}); return 1

def main():
    p = argparse.ArgumentParser(); sub = p.add_subparsers(dest="command", required=True)
    q=sub.add_parser("mark-start"); q.add_argument("--evidence", required=True)
    q=sub.add_parser("prepare-candidate"); q.add_argument("--repo", required=True); q.add_argument("--patch", required=True); q.add_argument("--evidence", required=True)
    q=sub.add_parser("run-phase"); q.add_argument("--name", required=True); q.add_argument("--cwd", required=True); q.add_argument("--timeout-s", type=float, required=True); q.add_argument("--job-budget-s", type=float); q.add_argument("--reserve-s", type=float); q.add_argument("--evidence", required=True); q.add_argument("--byte-cap", type=int, default=STREAM_CAP); q.add_argument("--env", action="append", default=[]); q.add_argument("--make-target"); q.add_argument("--make-var", action="append", default=[]); q.add_argument("argv", nargs=argparse.REMAINDER)
    q=sub.add_parser("summarize"); q.add_argument("--evidence", required=True); q.add_argument("--job", required=True, choices=tuple(EXPECTED))
    q=sub.add_parser("record-freeze"); q.add_argument("--repo", required=True); q.add_argument("--evidence", required=True)
    a=p.parse_args()
    if a.command=="mark-start": return mark_start(a.evidence)
    if a.command=="prepare-candidate": return prepare_candidate(a.repo,a.patch,a.evidence)
    if a.command=="summarize": return summarize(a.evidence,a.job)
    if a.command=="record-freeze": return record_freeze(a.repo,a.evidence)
    env=dict(item.split("=",1) for item in a.env)
    if a.make_target:
        variables=[item.split("=",1) for item in a.make_var]; argv, extra=build_make_command(a.make_target,variables,a.evidence); env.update(extra)
    else:
        argv=list(a.argv); argv=argv[1:] if argv[:1]==["--"] else argv
    return run_phase(a.name,a.cwd,a.timeout_s,a.evidence,argv,env,a.byte_cap,
                     a.job_budget_s,a.reserve_s)

if __name__ == "__main__": raise SystemExit(main())
