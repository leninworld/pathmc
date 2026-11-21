"""Shared stdlib-only contracts for the PathMC #157 hosted packet."""
from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import statistics
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

MAX_JSON = 32 * 1024 * 1024
ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
BASE_FLAGS = ["-x", "-v", "-n", "auto", "--dist", "loadscope", "-m", "not nightly", "--cov=pathmc", "--cov-report=term-missing"]


def atomic_json(path, value):
    path = os.fspath(path)
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=os.path.dirname(os.path.abspath(path)))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(tmp, path)
    finally:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def sha256_bytes(value):
    return hashlib.sha256(value).hexdigest()


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_json(path, limit=MAX_JSON):
    path = os.fspath(path)
    if os.path.islink(path) or not os.path.isfile(path):
        raise ValueError("not a regular file: " + path)
    if os.path.getsize(path) > limit:
        raise ValueError("JSON exceeds byte limit: " + path)
    with open(path, "rb") as handle:
        raw = handle.read(limit + 1)
    if len(raw) > limit:
        raise ValueError("JSON grew beyond byte limit: " + path)
    return json.loads(raw.decode("utf-8"))


def build_test_command(files, junit_path, *, shard, evidence=None):
    """Sole constructor for smoke and counted pytest commands."""
    evidence = os.path.abspath(evidence or os.path.dirname(junit_path))
    argv = ["uv", "run", "--locked", "--all-extras", "pytest", *BASE_FLAGS,
            "--cov-fail-under=0", "--junitxml", os.path.abspath(junit_path), *files]
    env = {"OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1",
           "COVERAGE_FILE": os.path.join(evidence, f".coverage.shard-{shard}")}
    return argv, env


def makefile_test_flags(text):
    lines = text.splitlines()
    start = next((i for i, line in enumerate(lines) if line.startswith("test:")), None)
    if start is None:
        raise ValueError("Makefile has no test target")
    recipe = []
    for line in lines[start + 1:]:
        if line and not line[0].isspace():
            break
        if line.startswith("\t"):
            recipe.append(line.strip())
    command = " ".join(recipe).replace("$(TEST_ENV)", "").replace("$(UV_RUN)", "uv run --locked --all-extras")
    tokens = shlex.split(command)
    try:
        pos = tokens.index("pytest")
    except ValueError as exc:
        raise ValueError("test recipe has no pytest command") from exc
    return tokens[pos + 1:]


def verify_makefile_test_recipe(path):
    text = Path(path).read_text(encoding="utf-8")
    variables = {}
    for name in ("TEST_ENV", "UV_RUN"):
        matches = re.findall(rf"(?m)^{name}\s*=\s*(.*?)\s*$", text)
        if len(matches) != 1:
            raise ValueError(f"Makefile must define {name} exactly once")
        variables[name] = matches[0]
    expected = {
        "TEST_ENV": "OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1",
        "UV_RUN": "uv run --locked --all-extras",
    }
    if variables != expected:
        raise ValueError(f"Makefile test environment drift: {variables!r} != {expected!r}")
    actual = makefile_test_flags(text)
    if actual != BASE_FLAGS:
        raise ValueError(f"Makefile test recipe drift: {actual!r} != {BASE_FLAGS!r}")
    return actual


def parse_collection(raw):
    text = ANSI.sub("", raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw)
    nodeids = [line.strip() for line in text.splitlines() if "::" in line and not line.lstrip().startswith(("<", "="))]
    if len(nodeids) != len(set(nodeids)):
        raise ValueError("duplicate collected node IDs")
    matches = re.findall(r"(?m)^\s*(\d+)(?:/\d+)? tests? collected(?: \(\d+ deselected\))? in [^\n]+$", text)
    if not matches:
        raise ValueError("missing collection summary")
    count = int(matches[-1])
    if count != len(nodeids):
        raise ValueError(f"collection count mismatch: summary={count} parsed={len(nodeids)}")
    return nodeids


def load_durations(path):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return {str(k): float(v) for k, v in data.items()}


def make_plan(nodeids, durations, n=6):
    if n < 1 or not nodeids:
        raise ValueError("invalid shard plan input")
    missing_default = statistics.median(durations.values()) if durations else 1.0
    weights = {}
    for node in nodeids:
        file = node.split("::", 1)[0]
        weights[file] = weights.get(file, 0.0) + float(durations.get(node, missing_default))
    buckets = [{"index": i + 1, "files": [], "serial_weight_s": 0.0} for i in range(n)]
    for file, weight in sorted(weights.items(), key=lambda item: (-item[1], item[0])):
        bucket = min(buckets, key=lambda item: (item["serial_weight_s"], item["index"]))
        bucket["files"].append(file)
        bucket["serial_weight_s"] += weight
    for bucket in buckets:
        bucket["files"].sort()
        largest = max((weights[x] for x in bucket["files"]), default=0.0)
        bucket["serial_weight_s"] = round(bucket["serial_weight_s"], 6)
        bucket["estimated_wall_s"] = round(1.52 * max(bucket["serial_weight_s"] / 2.0, largest) * 1.25, 3)
        bucket["upper_bound_wall_s"] = round(3.0 * max(bucket["serial_weight_s"] / 4.0, largest) * 1.25, 3)
    body = {"N": n, "shards": buckets}
    body["sha256"] = sha256_bytes(canonical(body))
    return body


def validate_plan_binding(plans, full_sets, shard_sets):
    """Bind seven independently recorded plans to full and shard collections."""
    if len(plans) != 7 or len(full_sets) != 7 or len(shard_sets) != 6:
        raise ValueError("plan binding requires seven plans/full sets and six shards")
    bodies = []
    def file_parts(nodes, label):
        if not isinstance(nodes, list) or not nodes:
            raise ValueError(label + " must be a non-empty node ID list")
        if not all(isinstance(node, str) and "::" in node and node.split("::", 1)[0] for node in nodes):
            raise ValueError(label + " contains malformed node IDs")
        return {node.split("::", 1)[0] for node in nodes}

    for number, plan in enumerate(plans, 1):
        if not isinstance(plan, dict) or "sha256" not in plan:
            raise ValueError(f"plan {number} missing sha256")
        body = dict(plan)
        recorded = body.pop("sha256")
        computed = sha256_bytes(canonical(body))
        if recorded != computed:
            raise ValueError(f"plan {number} canonical sha256 mismatch")
        bodies.append(body)
    first = canonical(bodies[0])
    if any(canonical(body) != first for body in bodies[1:]):
        raise ValueError("plan bodies differ between jobs")
    body = bodies[0]
    if type(body.get("N")) is not int or body["N"] != 6:
        raise ValueError("plan N must equal 6")
    shards = body.get("shards")
    if not isinstance(shards, list) or len(shards) != 6:
        raise ValueError("plan must contain six shards")
    by_index = {}
    assigned = []
    for shard in shards:
        if not isinstance(shard, dict) or not {"index", "files"}.issubset(shard):
            raise ValueError("plan shard is missing index or files")
        index = shard["index"]
        files = shard["files"]
        if (type(index) is not int or index in by_index or not isinstance(files, list)
                or not files or not all(isinstance(x, str) and x for x in files)):
            raise ValueError("plan shard index/files are malformed or duplicated")
        by_index[index] = files
        assigned.extend(files)
    if set(by_index) != set(range(1, 7)):
        raise ValueError("plan shard indices must be 1..6")
    if len(assigned) != len(set(assigned)):
        raise ValueError("plan assigns a file more than once")
    full_files = [file_parts(nodes, f"full collection {index}") for index, nodes in enumerate(full_sets, 1)]
    if any(files != full_files[0] for files in full_files[1:]):
        raise ValueError("full collection file sets differ between jobs")
    if set(assigned) != full_files[0]:
        raise ValueError("planned files do not equal full collection files")
    for index, nodes in enumerate(shard_sets, 1):
        collected = file_parts(nodes, f"shard {index} collection")
        if collected != set(by_index[index]):
            raise ValueError(f"shard {index} collection files do not match plan")
    return {"sha256": plans[0]["sha256"], "body": bodies[0]}


def junit_result(path):
    root = ET.parse(path).getroot()
    cases = root.findall(".//testcase")
    failures = sum(case.find("failure") is not None for case in cases)
    errors = sum(case.find("error") is not None for case in cases)
    return {"testcases": len(cases), "failures": failures, "errors": errors}


def coverage_proof(full_sets, shard_sets, junits):
    if len(full_sets) != 7 or len(shard_sets) != 6 or len(junits) != 6:
        raise ValueError("coverage proof requires seven full sets and six shards")
    canonical_full = list(full_sets[0])
    if any(list(nodes) != canonical_full for nodes in full_sets[1:]):
        raise ValueError("full-set mismatch between jobs")
    full = set(canonical_full)
    union = set()
    for index, nodes in enumerate(shard_sets):
        current = set(nodes)
        if len(current) != len(nodes):
            raise ValueError(f"duplicate in shard {index + 1}")
        if union & current:
            raise ValueError("shard overlap")
        union |= current
    if union != full:
        raise ValueError("coverage gap or extra node")
    total = 0
    for index, nodes in enumerate(shard_sets):
        result = junits[index]
        if result["failures"] or result["errors"]:
            raise ValueError("JUnit failures/errors")
        if result["testcases"] != len(nodes):
            raise ValueError("JUnit count mismatch")
        total += result["testcases"]
    if total != len(full):
        raise ValueError("total JUnit count mismatch")
    return {"status": "PASS", "full_count": len(full), "shard_counts": [len(x) for x in shard_sets],
            "junit_total": total, "full_sha256": sha256_bytes(canonical(canonical_full))}
