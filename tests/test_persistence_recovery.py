"""Persistent publish and restart-recovery behaviour.

These tests exercise the durable commit protocol and the recovery rules
without any database: jobs are manipulated directly on the store. End-to-end
restart/crash coverage that needs live PostgreSQL lives in
``test_recovery_restart.py``.
"""
from __future__ import annotations

import json

import pytest

from app.jobs import JobStore
from app.models import ConflictReport, JobStatus
from app.persistence import PACKAGE_FILES, PackageExists, Persistence

PKG = {name: f"-- contents of {name}\n" for name in PACKAGE_FILES}


def _make_job(store: JobStore, status: JobStatus) -> str:
    jid = store.create()
    store.update(jid, status=status)
    return jid


# --------------------------------------------------------------------- state
def test_state_is_written_through_to_disk(isolated_persistent_store):
    store = JobStore(str(isolated_persistent_store))
    jid = store.create()
    store.append_log(jid, "hello durable world")
    job_file = isolated_persistent_store / "jobs" / jid / "job.json"
    assert job_file.is_file()
    on_disk = json.loads(job_file.read_text(encoding="utf-8"))
    assert on_disk["status"] == JobStatus.PENDING.value
    assert any("hello durable world" in line for line in on_disk["log"])


def test_failed_job_keeps_conflict_report_across_restart(isolated_persistent_store):
    store = JobStore(str(isolated_persistent_store))
    jid = _make_job(store, JobStatus.FAILED)
    store.update(
        jid,
        conflict=ConflictReport(
            kind="no_solution", message="无解", details={"kinds": ["fk"]}
        ),
    )

    # A brand-new store over the same directory emulates a process restart.
    restarted = JobStore(str(isolated_persistent_store))
    job = restarted.get(jid)
    assert job is not None
    assert job.status == JobStatus.FAILED
    assert job.conflict is not None
    assert job.conflict.kind == "no_solution"
    assert job.conflict.details == {"kinds": ["fk"]}
    assert restarted.package(jid) is None


# ------------------------------------------------------------- interrupted
@pytest.mark.parametrize(
    "status", [JobStatus.PENDING, JobStatus.GENERATING, JobStatus.VERIFYING]
)
def test_running_jobs_become_interrupted_and_not_downloadable(
    isolated_persistent_store, status
):
    store = JobStore(str(isolated_persistent_store))
    jid = _make_job(store, status)

    restarted = JobStore(str(isolated_persistent_store))
    job = restarted.get(jid)
    assert job.status == JobStatus.INTERRUPTED
    assert job.conflict is not None
    assert job.conflict.kind == "interrupted"
    assert job.conflict.details["previous_status"] == status.value
    assert job.conflict.details["downloadable"] is False
    assert restarted.package(jid) is None
    # The INTERRUPTED verdict itself is durable: a second restart stays put.
    again = JobStore(str(isolated_persistent_store))
    assert again.get(jid).status == JobStatus.INTERRUPTED


def test_staging_scratch_is_deleted_on_recovery(isolated_persistent_store):
    store = JobStore(str(isolated_persistent_store))
    jid = _make_job(store, JobStatus.VERIFYING)
    persistence: Persistence = store.persistence
    staging = persistence.staging_dir(jid)
    staging.mkdir(parents=True)
    (staging / "half_written.sql").write_text("BEGIN;\n", encoding="utf-8")

    JobStore(str(isolated_persistent_store))  # restart
    assert not staging.exists()
    # Nothing was promoted into a visible package directory.
    assert not persistence.package_dir(jid).exists()


def test_package_left_by_interrupted_publish_is_quarantined_not_reused(
    isolated_persistent_store,
):
    store = JobStore(str(isolated_persistent_store))
    jid = _make_job(store, JobStatus.VERIFYING)
    persistence: Persistence = store.persistence
    # Simulate death after the staging->package rename but before the
    # SUCCEEDED job.json commit.
    pkg_dir = persistence.package_dir(jid)
    pkg_dir.mkdir(parents=True)
    for name, content in PKG.items():
        (pkg_dir / name).write_text(content, encoding="utf-8")

    restarted = JobStore(str(isolated_persistent_store))
    assert restarted.get(jid).status == JobStatus.INTERRUPTED
    assert restarted.package(jid) is None
    assert not pkg_dir.exists()
    quarantine = persistence.quarantine_dir(jid)
    assert quarantine.is_dir()
    assert (quarantine / "01_schema.sql").is_file()
    # The quarantined temp result must never come back on a further restart.
    again = JobStore(str(isolated_persistent_store))
    assert again.package(jid) is None
    assert again.get(jid).status == JobStatus.INTERRUPTED


def test_succeeded_without_complete_package_is_interrupted(
    isolated_persistent_store,
):
    store = JobStore(str(isolated_persistent_store))
    jid = _make_job(store, JobStatus.SUCCEEDED)
    # No package directory at all: the durable commit did not finish.
    restarted = JobStore(str(isolated_persistent_store))
    assert restarted.get(jid).status == JobStatus.INTERRUPTED
    assert restarted.package(jid) is None


def test_succeeded_with_partial_package_directory_is_interrupted(
    isolated_persistent_store,
):
    store = JobStore(str(isolated_persistent_store))
    jid = _make_job(store, JobStatus.SUCCEEDED)
    persistence: Persistence = store.persistence
    # A package directory missing files: never treat it as downloadable.
    pkg_dir = persistence.package_dir(jid)
    pkg_dir.mkdir(parents=True)
    (pkg_dir / "01_schema.sql").write_text("BEGIN;\n", encoding="utf-8")

    restarted = JobStore(str(isolated_persistent_store))
    assert restarted.get(jid).status == JobStatus.INTERRUPTED
    assert restarted.package(jid) is None
    # The partial directory was moved aside, not left visible.
    assert not pkg_dir.exists()
    assert persistence.quarantine_dir(jid).is_dir()


# --------------------------------------------------------------- committed
def test_succeeded_job_and_package_survive_restart(isolated_persistent_store):
    store = JobStore(str(isolated_persistent_store))
    jid = _make_job(store, JobStatus.VERIFYING)
    store.stage_package(jid, PKG)
    store.commit_published(jid)
    store.update(jid, status=JobStatus.SUCCEEDED)

    restarted = JobStore(str(isolated_persistent_store))
    job = restarted.get(jid)
    assert job.status == JobStatus.SUCCEEDED
    package = restarted.package(jid)
    assert package is not None
    assert set(package) == set(PACKAGE_FILES)
    assert package["01_schema.sql"] == PKG["01_schema.sql"]


def test_package_files_are_readable_directly_from_volume(isolated_persistent_store):
    store = JobStore(str(isolated_persistent_store))
    jid = _make_job(store, JobStatus.VERIFYING)
    store.stage_package(jid, PKG)
    store.commit_published(jid)
    pkg_dir = isolated_persistent_store / "jobs" / jid / "package"
    assert sorted(p.name for p in pkg_dir.iterdir()) == sorted(PACKAGE_FILES)
    # The committed bytes are exactly what was staged.
    assert (
        pkg_dir.joinpath("manifest.json").read_text(encoding="utf-8")
        == PKG["manifest.json"]
    )


def test_new_package_never_overwrites_existing_one(isolated_persistent_store):
    store = JobStore(str(isolated_persistent_store))
    jid = _make_job(store, JobStatus.VERIFYING)
    store.stage_package(jid, PKG)
    store.commit_published(jid)

    # A second publish attempt for the same job id must fail loudly rather
    # than clobber the already committed package.
    with pytest.raises(PackageExists):
        store.stage_package(jid, {**PKG, "01_schema.sql": "-- MALICIOUS REWRITE\n"})
    with pytest.raises(PackageExists):
        store.commit_published(jid)
    # Original bytes are untouched.
    persistence: Persistence = store.persistence
    assert (
        persistence.package_dir(jid)
        .joinpath("01_schema.sql")
        .read_text(encoding="utf-8")
        == PKG["01_schema.sql"]
    )

    # Separate jobs get separate immutable directories; ids are unique anyway.
    other = store.create()
    store.stage_package(other, PKG)
    store.commit_published(other)
    assert store.package(other) is not None


def test_staging_never_appears_as_downloadable_package(isolated_persistent_store):
    store = JobStore(str(isolated_persistent_store))
    jid = _make_job(store, JobStatus.VERIFYING)
    store.stage_package(jid, PKG)
    # Staged but not renamed: nothing readable through the package API.
    assert store.package(jid) is None
    assert store.persistence.read_package(jid) is None


def test_purge_removes_job_directory_from_volume(isolated_persistent_store):
    from app.config import settings

    store = JobStore(str(isolated_persistent_store))
    jid = store.create()
    job_dir = isolated_persistent_store / "jobs" / jid
    assert job_dir.exists()
    # Backdate the job beyond the TTL.
    old = store.get(jid).model_copy(
        update={"created_at": "2000-01-01T00:00:00+00:00"}
    )
    store.persistence.write_job(old)
    store._jobs[jid] = old
    assert settings.job_ttl_seconds >= 0
    store.purge_expired()
    assert not job_dir.exists()
    assert store.get(jid) is None


def test_recovery_tolerates_directory_without_job_json(isolated_persistent_store):
    store = JobStore(str(isolated_persistent_store))
    store.create()
    # Junk directory that is not a job must not break startup recovery.
    junk = isolated_persistent_store / "jobs" / "not-a-job"
    junk.mkdir(parents=True)
    (junk / "something.tmp").write_text("x", encoding="utf-8")
    restarted = JobStore(str(isolated_persistent_store))
    assert restarted.get("not-a-job") is None


def test_lifespan_recovers_and_api_blocks_interrupted_download(
    isolated_persistent_store,
):
    """The FastAPI lifespan must run the same recovery over the volume."""
    from fastapi.testclient import TestClient

    from app.jobs import STORE
    from app.main import app

    jid = STORE.create()
    STORE.update(jid, status=JobStatus.VERIFYING)

    # Triggering lifespan startup on the already-configured store re-runs
    # recovery against the same persistent directory.
    with TestClient(app) as client:
        r = client.get(f"/jobs/{jid}")
        assert r.status_code == 200
        assert r.json()["status"] == JobStatus.INTERRUPTED.value
        dl = client.get(f"/jobs/{jid}/download")
        assert dl.status_code == 409
        assert "中断" in dl.json()["detail"]

        # A recovered SUCCEEDED job remains downloadable through the API.
        good = STORE.create()
        STORE.stage_package(good, PKG)
        STORE.commit_published(good)
        STORE.update(good, status=JobStatus.SUCCEEDED)
        ok = client.get(f"/jobs/{good}/download")
        assert ok.status_code == 200
        assert ok.headers["content-type"] == "application/zip"
        assert ok.headers["x-contains-real-patient-data"] == "false"
