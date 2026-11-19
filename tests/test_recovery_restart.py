"""End-to-end crash/restart tests with real process termination.

A small driver process runs :func:`app.jobs.run_job` against the live source
and verification PostgreSQL clusters (provisioned by the session fixtures),
with ``CRASH_AT`` selected to ``os._exit`` the process at a specific phase:

* ``generated``      - killed after introspection, before the z3 solve
* ``verified``       - killed after verification, before durable publish
* ``publish_staged`` - killed after files were fsynced into .staging
* ``publish_renamed``- killed after the package rename, before SUCCEEDED json
* ``published``      - killed right after the SUCCEEDED state was committed

A second process (or a fresh ``JobStore`` in-process, which runs the same
recovery code) then proves the recovery rules.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import zipfile
from io import BytesIO
from pathlib import Path

import pytest

from app.jobs import JobStore, package_zip
from app.models import JobStatus

ROOT = Path(__file__).resolve().parent.parent
DRIVER = Path(__file__).resolve().parent / "_crash_driver.py"

CYCLIC_REQUEST = json.loads(
    (ROOT / "examples" / "request_cyclic_four_tables.json").read_text(encoding="utf-8")
)


def _env(persist_dir: Path, crash_at: str, source_dsn: str, verify_dsn: str) -> dict:
    env = dict(os.environ)
    env.update(
        {
            "PERSIST_DIR": str(persist_dir),
            "CRASH_AT": crash_at,
            "SOURCE_DSN": source_dsn,
            "VERIFY_DSN": verify_dsn,
            "VERIFY_CLEANUP": "detach",
            "PYTHONPATH": str(ROOT),
            "PYTHONDONTWRITEBYTECODE": "1",
        }
    )
    return env


def _run_driver(persist_dir: Path, crash_at: str, source_dsn: str, verify_dsn: str):
    # -s keeps sys.path (PYTHONPATH is honoured) while disabling user-site
    # packages; the driver CWD is the repo root so `import app` resolves.
    return subprocess.run(
        [sys.executable, "-s", str(DRIVER), json.dumps(CYCLIC_REQUEST)],
        env=_env(persist_dir, crash_at, source_dsn, verify_dsn),
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=180,
    )


@pytest.mark.parametrize(
    "crash_at",
    ["generated", "verified", "publish_staged", "publish_renamed"],
)
def test_crash_mid_flight_marks_interrupted_and_blocks_download(
    isolated_persistent_store, source_dsn, verify_dsn, crash_at
):
    persist_dir = isolated_persistent_store
    proc = _run_driver(persist_dir, crash_at, source_dsn, verify_dsn)
    # The driver deliberately terminates itself with os._exit(137).
    assert proc.returncode != 0, proc.stderr
    assert "crash injection" in proc.stderr

    # Emulate a service restart with a brand-new store over the same volume.
    restarted = JobStore(str(persist_dir))
    jobs = list(restarted.get(j) for j in _job_ids(persist_dir))
    assert len(jobs) == 1
    job = jobs[0]
    assert job.status == JobStatus.INTERRUPTED
    assert job.conflict is not None and job.conflict.kind == "interrupted"
    assert restarted.package(job.job_id) is None
    assert not (persist_dir / "jobs" / job.job_id / "package").exists()
    # No hidden staging scratch survives the restart.
    assert not (persist_dir / "jobs" / job.job_id / ".staging").exists()

    # The interruption is durable and stays non-downloadable after another
    # restart; the old temporary result is never picked up.
    again = JobStore(str(persist_dir))
    assert again.get(job.job_id).status == JobStatus.INTERRUPTED
    assert again.package(job.job_id) is None


def test_successful_package_survives_restart_and_downloads(
    isolated_persistent_store, source_dsn, verify_dsn
):
    persist_dir = isolated_persistent_store
    proc = _run_driver(persist_dir, "published", source_dsn, verify_dsn)
    assert proc.returncode != 0, proc.stderr  # killed just after commit
    assert "crash injection" in proc.stderr

    restarted = JobStore(str(persist_dir))
    (job,) = [restarted.get(j) for j in _job_ids(persist_dir)]
    assert job.status == JobStatus.SUCCEEDED
    package = restarted.package(job.job_id)
    assert package is not None
    assert set(package) == {
        "01_schema.sql",
        "02_data.sql",
        "03_constraints.sql",
        "manifest.json",
        "README.md",
    }
    zf = zipfile.ZipFile(BytesIO(package_zip(package)))
    assert set(zf.namelist()) == set(package)
    manifest = json.loads(package["manifest.json"])
    assert manifest["contains_real_patient_data"] is False

    # A further restart keeps the same package bytes available.
    again = JobStore(str(persist_dir))
    assert again.get(job.job_id).status == JobStatus.SUCCEEDED
    assert again.package(job.job_id) == package


def test_clean_run_without_crash_persists_success(
    isolated_persistent_store, source_dsn, verify_dsn
):
    persist_dir = isolated_persistent_store
    proc = _run_driver(persist_dir, "", source_dsn, verify_dsn)
    assert proc.returncode == 0, proc.stderr
    restarted = JobStore(str(persist_dir))
    (job,) = [restarted.get(j) for j in _job_ids(persist_dir)]
    assert job.status == JobStatus.SUCCEEDED
    assert restarted.package(job.job_id) is not None


def _job_ids(persist_dir: Path) -> list[str]:
    return sorted(
        entry.name
        for entry in (persist_dir / "jobs").iterdir()
        if entry.is_dir() and (entry / "job.json").is_file()
    )
