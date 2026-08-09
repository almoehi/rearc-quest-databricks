#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# ///
"""Build the pipeline test image and run pytest inside it.

Usage:
    uv run scripts/test-in-docker.py                   # run all tests
    uv run scripts/test-in-docker.py -v -k schema      # pass pytest args
    uv run scripts/test-in-docker.py --no-cache        # force Docker layer rebuild
"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
IMAGE = "bls-pipeline-test:local"


def main() -> None:
    args = sys.argv[1:]
    no_cache = "--no-cache" in args
    pytest_args = [a for a in args if a != "--no-cache"]

    build_cmd = ["docker", "build", "-t", IMAGE]
    if no_cache:
        build_cmd.append("--no-cache")
    build_cmd.append(str(ROOT))

    result = subprocess.run(build_cmd)
    if result.returncode != 0:
        sys.exit(result.returncode)

    run_cmd = [
        "docker", "run", "--rm", IMAGE,
        "uv", "run", "--no-project", "pytest", "-v",
        *pytest_args,
    ]
    result = subprocess.run(run_cmd)
    sys.exit(result.returncode)


main()
