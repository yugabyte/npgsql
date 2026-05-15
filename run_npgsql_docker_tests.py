#!/usr/bin/env python3
"""
Npgsql Docker Test Runner
--------------------------
Runs YugabyteDB in one container and the Npgsql .NET test suite in another.

Architecture
============
  ┌─────────────────────────┐   YSQL 5433   ┌─────────────────────────┐
  │  yugabytedb container   │ ◄──────────── │  test-runner container  │
  │  yugabytedb/yugabyte    │               │  dotnet/sdk:9.0         │
  │  bin/yugabyted start    │               │  dotnet test (3 suites) │
  └─────────────────────────┘               └─────────────────────────┘

The test-runner image is built from docker/yugabyte/Dockerfile.testrunner and
executes docker/yugabyte/run-tests.sh (which calls init-yugabyte.sh then dotnet
test on Npgsql.Tests, Npgsql.DependencyInjection.Tests, Npgsql.PluginTests).

Environment variables
=====================
  YB_IMAGE               YugabyteDB Docker image            default: yugabytedb/yugabyte:latest
  YB_ENABLE_YSQL_CONN_MGR  Enable YSQL Connection Manager  default: 0 (disabled)
                            Set to 1/true/yes to enable
  NPGSQL_DIR             Path to npgsql repo root           default: directory of this script
  NPGSQL_TEST_OUTPUT_LOG Path to log file                   default: <NPGSQL_DIR>/npgsql_test_output[_connmgr|_noconnmgr].log
  YB_WAIT_SEC            Initial sleep before polling YSQL  default: 55
  YB_VERIFY_RETRIES      Max poll retries (5s each)         default: 24
  TEST_TFM               .NET target framework moniker       default: net9.0
  CONFIG                 Build configuration                 default: Release
  TEST_TIMEOUT_SEC       Hard cap on the whole test run (s) default: 7200 (2 h); 0 = no cap

Connection Manager
==================
  When YB_ENABLE_YSQL_CONN_MGR=1 (or true/yes), yugabyted is started with
  --tserver_flags=enable_ysql_conn_mgr=true. This enables the Odyssey connection
  pooler. The log filename is automatically suffixed with _connmgr to distinguish
  runs.

  When YB_ENABLE_YSQL_CONN_MGR=0 (default), the connection manager is disabled
  and the log is suffixed with _noconnmgr.

Usage
=====
  python run_npgsql_docker_tests.py                           # CM off (default)
  YB_ENABLE_YSQL_CONN_MGR=1 python run_npgsql_docker_tests.py  # CM on
  YB_IMAGE=yugabytedb/yugabyte:2.25.0.0-b123 python run_npgsql_docker_tests.py
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run(cmd: list[str], *, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    """Run a subprocess, streaming output by default."""
    return subprocess.run(cmd, check=check, capture_output=capture, text=True)


def _run_quiet(cmd: list[str]) -> subprocess.CompletedProcess:
    """Run a subprocess, ignoring failures and suppressing output."""
    return subprocess.run(cmd, capture_output=True, text=True)


def _strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


# ---------------------------------------------------------------------------
# Failure parsing (dotnet test --logger "console;verbosity=detailed")
# ---------------------------------------------------------------------------

def extract_failures_from_output(output: str) -> list[tuple[str, str]]:
    """
    Parse dotnet test verbose output for failure lines.

    dotnet test emits:
      - "  Failed TestName [X ms]" per failed test
      - "    Error Message: ..."  indented context after each failure
      - Summary: "Failed!  - Failed:     N, Passed:    M, ..."

    Returns list of (test_name, error_message) tuples.
    """
    failures: list[tuple[str, str]] = []
    seen: set[str] = set()
    lines = output.splitlines()

    i = 0
    while i < len(lines):
        plain = _strip_ansi(lines[i]).rstrip()

        # "  Failed SomeName.TestMethod [123 ms]"
        m = re.match(r"^\s+Failed\s+(.+?)\s+\[\d+", plain)
        if m:
            test_name = m.group(1).strip()
            if test_name not in seen:
                seen.add(test_name)
                # Gather indented context lines that follow
                ctx: list[str] = []
                j = i + 1
                while j < len(lines):
                    next_plain = _strip_ansi(lines[j]).rstrip()
                    # Context lines are indented deeper or start with "Error"/"Assert"
                    if next_plain and (next_plain.startswith("      ") or next_plain.strip().startswith("Error") or next_plain.strip().startswith("Assert")):
                        ctx.append(next_plain.strip())
                        j += 1
                    else:
                        break
                failures.append((test_name, "\n".join(ctx[:5])))
            i += 1
            continue

        # Class-level: "X Failed, Y Passed" in run summary (for class-level errors)
        m2 = re.match(r"^Failed!\s+-\s+Failed:\s+(\d+)", plain)
        if m2 and not failures:
            # No individual test lines captured yet — record a generic entry
            failures.append((f"(run-level failure — {m2.group(0).strip()})", ""))

        i += 1

    return failures


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def main() -> int:
    script_dir = Path(__file__).resolve().parent
    npgsql_dir = Path(os.environ.get("NPGSQL_DIR", str(script_dir)))

    yb_image = os.environ.get("YB_IMAGE", "yugabytedb/yugabyte:latest")
    conn_mgr = os.environ.get("YB_ENABLE_YSQL_CONN_MGR", "0").lower() in ("1", "true", "yes")
    yb_wait_sec = int(os.environ.get("YB_WAIT_SEC", "55"))
    verify_retries = int(os.environ.get("YB_VERIFY_RETRIES", "24"))
    test_tfm = os.environ.get("TEST_TFM", "net9.0")
    config = os.environ.get("CONFIG", "Release")
    test_timeout = int(os.environ.get("TEST_TIMEOUT_SEC", "7200"))

    conn_mgr_suffix = "_connmgr" if conn_mgr else "_noconnmgr"
    default_log = str(npgsql_dir / f"npgsql_test_output{conn_mgr_suffix}.log")
    log_file_path = os.environ.get("NPGSQL_TEST_OUTPUT_LOG", default_log)

    network_name = "npgsql-test-net"
    yb_container = "npgsql-yugabytedb"
    runner_image = "npgsql-test-runner"
    runner_name = "npgsql-test-runner"

    conn_mgr_label = "yes" if conn_mgr else "no"
    print("=== Npgsql Docker Test Runner ===")
    print(f"Npgsql dir    : {npgsql_dir}")
    print(f"YugabyteDB    : {yb_image}")
    print(f"Conn mgr      : {conn_mgr_label}  (YB_ENABLE_YSQL_CONN_MGR)")
    print(f"Target FW     : {test_tfm}")
    print(f"Config        : {config}")
    print(f"Log file      : {log_file_path}")
    if test_timeout > 0:
        print(f"Test timeout  : {test_timeout}s  (set TEST_TIMEOUT_SEC=0 to disable)")
    print()

    # ------------------------------------------------------------------
    # [0/6] Cleanup
    # ------------------------------------------------------------------
    print("[0/6] Cleaning up any leftover containers / network...")
    _run_quiet(["docker", "rm", "-f", runner_name, yb_container])
    _run_quiet(["docker", "network", "rm", network_name])
    print()

    # ------------------------------------------------------------------
    # [1/6] Docker network
    # ------------------------------------------------------------------
    print(f"[1/6] Creating Docker network '{network_name}'...")
    _run(["docker", "network", "create", network_name])
    print()

    # ------------------------------------------------------------------
    # [2/6] YugabyteDB container
    # ------------------------------------------------------------------
    print(f"[2/6] Starting YugabyteDB container '{yb_container}'...")
    yugabyted_args = ["bin/yugabyted", "start", "--background=false"]
    if conn_mgr:
        yugabyted_args.append("--tserver_flags=enable_ysql_conn_mgr=true")
        print("  (YSQL Connection Manager enabled)")
    else:
        print("  (YSQL Connection Manager disabled)")

    _run([
        "docker", "run", "-d",
        "--name", yb_container,
        "--hostname", "yugabytedb",
        "--network", network_name,
        "-p", "5433:5433",
        "-p", "7000:7000",
        "-p", "9000:9000",
        yb_image,
        *yugabyted_args,
    ])
    print()

    # ------------------------------------------------------------------
    # [3/6] Wait for YSQL readiness
    # ------------------------------------------------------------------
    print(f"[3/6] Sleeping {yb_wait_sec}s then polling YSQL readiness "
          f"(up to {verify_retries} retries × 5s)...")
    time.sleep(yb_wait_sec)
    for attempt in range(1, verify_retries + 1):
        result = subprocess.run(
            [
                "docker", "exec", yb_container,
                "/home/yugabyte/bin/ysqlsh",
                "-h", "yugabytedb", "-p", "5433", "-U", "yugabyte",
                "-c", "SELECT 1;",
            ],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            print("YSQL is ready.")
            break
        if attempt < verify_retries:
            print(f"  YSQL not ready ({attempt}/{verify_retries}), retrying in 5s...")
            time.sleep(5)
        else:
            print("ERROR: YSQL did not become ready in time.")
            print(result.stderr or result.stdout)
            _run_quiet(["docker", "rm", "-f", yb_container])
            _run_quiet(["docker", "network", "rm", network_name])
            return 1
    print()

    # ------------------------------------------------------------------
    # [4/6] Build test-runner image
    # ------------------------------------------------------------------
    print(f"[4/6] Building Npgsql test-runner image '{runner_image}'...")
    print(f"       (from {npgsql_dir}/docker/yugabyte/Dockerfile.testrunner)")
    _run([
        "docker", "build",
        "-f", str(npgsql_dir / "docker" / "yugabyte" / "Dockerfile.testrunner"),
        "-t", runner_image,
        str(npgsql_dir),
    ])
    print()

    # ------------------------------------------------------------------
    # [5/6] Run tests
    # ------------------------------------------------------------------
    print("[5/6] Running Npgsql tests (streaming output + writing log)...")
    print(f"       Log: {log_file_path}")
    print()

    npgsql_test_db = (
        f"Host=yugabytedb;Port=5433;"
        f"Username=npgsql_tests;Password=npgsql_tests;"
        f"Database=npgsql_tests;"
        f"Timeout=0;Command Timeout=0;"
        f"SSL Mode=Disable;Multiplexing=False"
    )

    cmd = [
        "docker", "run", "--rm",
        "--name", runner_name,
        "--network", network_name,
        "-e", f"NPGSQL_TEST_DB={npgsql_test_db}",
        "-e", "CI=true",
        "-e", f"TEST_TFM={test_tfm}",
        "-e", f"CONFIG={config}",
        "-e", f"YSQL_HOST=yugabytedb",
        "-e", f"YSQL_PORT=5433",
        "-v", f"{npgsql_dir}:/workspace",
        "-w", "/workspace",
        runner_image,
        "bash", "/workspace/docker/yugabyte/run-tests.sh",
    ]

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    exit_code = 1

    def _stream() -> None:
        with open(log_file_path, "w", encoding="utf-8") as logf:
            assert proc.stdout is not None
            for line in proc.stdout:
                logf.write(line)
                logf.flush()
                print(line, end="")

    reader = threading.Thread(target=_stream, daemon=True)
    reader.start()

    try:
        if test_timeout > 0:
            proc.wait(timeout=test_timeout)
            exit_code = proc.returncode
        else:
            exit_code = proc.wait()
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        print()
        print(f"Tests timed out after {test_timeout}s.")
        print("Increase TEST_TIMEOUT_SEC or set it to 0 to disable the cap.")
        exit_code = -1
    except KeyboardInterrupt:
        proc.terminate()
        proc.wait()
        exit_code = -1

    reader.join(timeout=10)

    # Summary
    try:
        captured = Path(log_file_path).read_text(encoding="utf-8")
    except OSError:
        captured = ""

    failures = extract_failures_from_output(captured)

    print()
    print("=" * 70)
    print(f"Full test log: {log_file_path}")
    print("=" * 70)

    if failures:
        print()
        print("FAILURE SUMMARY")
        print("-" * 70)
        for idx, (test_name, msg) in enumerate(failures, 1):
            print(f"  {idx}. {test_name}")
            if msg.strip():
                for m in msg.splitlines():
                    print(f"     {m}")
        print()
        print("-" * 70)
        print(f"Total failures: {len(failures)}")
    else:
        print("All tests passed (or no failure lines detected).")

    print()

    # ------------------------------------------------------------------
    # [6/6] Cleanup
    # ------------------------------------------------------------------
    print("[6/6] Cleaning up containers and network...")
    _run_quiet(["docker", "rm", "-f", yb_container])
    _run_quiet(["docker", "network", "rm", network_name])
    print("Done.")

    return exit_code if exit_code is not None else 0


if __name__ == "__main__":
    sys.exit(main())
