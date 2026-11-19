"""Subprocess driver for crash/restart tests.

Runs a single job to completion (or until CRASH_AT kills the process) and
prints the job id on stdout. Kept outside the test module so it can be launched
as an isolated service process (``python -s``) that dies abruptly.
"""
from __future__ import annotations

import json
import sys

from app.jobs import STORE, run_job
from app.models import GenerateRequest


def main() -> int:
    req = GenerateRequest(**json.loads(sys.argv[1]))
    job_id = STORE.create()
    print(job_id, flush=True)
    try:
        run_job(job_id, req)
    except SystemExit:
        raise
    final = STORE.get(job_id)
    print(f"final_status={final.status.value if final else 'MISSING'}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
