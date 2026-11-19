"""Durable, crash-safe persistence for job state and SQL packages.

On-disk layout under the persistent volume (``settings.persist_dir``)::

    jobs/
      <job_id>/
        job.json              # full JobResult (status, conflict, logs, ...)
        package/              # exists ONLY for durably committed packages
          01_schema.sql
          02_data.sql
          03_constraints.sql
          manifest.json
          README.md
        .staging/             # scratch area while a package is being written
        .quarantine/          # package leftovers from an interrupted publish
        ...

Commit protocol (the only way a package becomes visible)
--------------------------------------------------------
1. Package files are written under ``.staging`` and fsynced one by one.
2. The staging directory is atomically ``rename``\\ d to ``package``.
   A package directory therefore never exists in a half-written state.
3. Only afterwards is ``job.json`` rewritten with ``status=SUCCEEDED`` via
   a temp-file + fsync + atomic rename.

Consequences after an abrupt process kill at any point:

* death before the staging rename -> only a ``.staging`` scratch dir remains,
  which recovery deletes; no package is visible, no temp result is reused;
* death after the rename but before the SUCCEEDED json -> the package exists
  while the job is still VERIFYING/etc.; recovery quarantines the package and
  marks the job INTERRUPTED (still not downloadable);
* death after the SUCCEEDED json -> both state and package are on disk and
  the job is recovered as SUCCEEDED with its download intact.

No package directory is ever overwritten: committing a package for a job that
already has one raises ``PackageExists``.
"""
from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

from .models import JobResult

#: Files every committed SQL package must contain.
PACKAGE_FILES: tuple[str, ...] = (
    "01_schema.sql",
    "02_data.sql",
    "03_constraints.sql",
    "manifest.json",
    "README.md",
)


class PackageExists(RuntimeError):
    """A committed package already exists for this job; it must not be overwritten."""


class Persistence:
    """File-backed job store rooted at ``root`` (a persistent-volume path)."""

    def __init__(self, root: str | os.PathLike) -> None:
        self.root = Path(root)
        self.jobs_dir = self.root / "jobs"

    # ------------------------------------------------------------------ paths
    def job_dir(self, job_id: str) -> Path:
        return self.jobs_dir / job_id

    def job_file(self, job_id: str) -> Path:
        return self.job_dir(job_id) / "job.json"

    def package_dir(self, job_id: str) -> Path:
        return self.job_dir(job_id) / "package"

    def staging_dir(self, job_id: str) -> Path:
        return self.job_dir(job_id) / ".staging"

    def quarantine_dir(self, job_id: str) -> Path:
        return self.job_dir(job_id) / ".quarantine"

    def ensure_layout(self) -> None:
        """Create the top-level directories; safe to call repeatedly."""
        self.jobs_dir.mkdir(parents=True, exist_ok=True)

    # ----------------------------------------------------------------- state
    def write_job(self, job: JobResult) -> None:
        """Durably persist a full job state (atomic temp-file + rename)."""
        job_dir = self.job_dir(job.job_id)
        job_dir.mkdir(parents=True, exist_ok=True)
        target = self.job_file(job.job_id)
        tmp = job_dir / ".job.json.tmp"
        payload = json.dumps(job.model_dump(), ensure_ascii=False, indent=2)
        _atomic_write_text(tmp, target, payload)

    def read_job(self, job_id: str) -> JobResult | None:
        path = self.job_file(job_id)
        if not path.is_file():
            return None
        with path.open("r", encoding="utf-8") as fh:
            return JobResult(**json.load(fh))

    def iter_job_ids(self) -> list[str]:
        if not self.jobs_dir.is_dir():
            return []
        return sorted(
            entry.name
            for entry in self.jobs_dir.iterdir()
            if entry.is_dir() and (entry / "job.json").is_file()
        )

    # --------------------------------------------------------------- package
    def stage_package(self, job_id: str, package: dict[str, str]) -> Path:
        """Write a package into the hidden staging area, fsyncing every file.

        Raises ``PackageExists`` if a committed package already exists, so a
        new run can never overwrite a previously published package.
        """
        if self.package_dir(job_id).exists():
            raise PackageExists(f"package already committed for job {job_id}")
        staging = self.staging_dir(job_id)
        if staging.exists():
            # Leftover from a previous aborted attempt; never reuse it.
            shutil.rmtree(staging)
        staging.mkdir(parents=True, exist_ok=True)
        for name, content in package.items():
            _atomic_write_text(staging / f".{name}.tmp", staging / name, content)
        _fsync_dir(staging)
        return staging

    def commit_package(self, job_id: str) -> Path:
        """Atomically promote the staged package to its visible directory."""
        staging = self.staging_dir(job_id)
        target = self.package_dir(job_id)
        if target.exists():
            raise PackageExists(f"package already committed for job {job_id}")
        if not staging.is_dir():
            raise FileNotFoundError(f"no staged package for job {job_id}")
        os.rename(staging, target)
        _fsync_dir(self.job_dir(job_id))
        return target

    def quarantine_package(self, job_id: str) -> bool:
        """Move a package directory aside so it can never be served.

        Used by recovery when a package exists without a SUCCEEDED state, and
        on any other case where an unfinished job left package files behind.
        Returns ``True`` when something was quarantined.
        """
        pkg = self.package_dir(job_id)
        if not pkg.exists():
            return False
        quarantine = self.quarantine_dir(job_id)
        if quarantine.exists():
            shutil.rmtree(quarantine)
        os.rename(pkg, quarantine)
        _fsync_dir(self.job_dir(job_id))
        return True

    def remove_staging(self, job_id: str) -> bool:
        staging = self.staging_dir(job_id)
        if not staging.exists():
            return False
        shutil.rmtree(staging)
        return True

    def read_package(self, job_id: str) -> dict[str, str] | None:
        pkg_dir = self.package_dir(job_id)
        if not pkg_dir.is_dir():
            return None
        package: dict[str, str] = {}
        for name in PACKAGE_FILES:
            path = pkg_dir / name
            if not path.is_file():
                return None
            package[name] = path.read_text(encoding="utf-8")
        return package

    def package_is_complete(self, job_id: str) -> bool:
        pkg_dir = self.package_dir(job_id)
        return pkg_dir.is_dir() and all(
            (pkg_dir / name).is_file() for name in PACKAGE_FILES
        )

    # --------------------------------------------------------------- cleanup
    def remove_job_dir(self, job_id: str) -> None:
        job_dir = self.job_dir(job_id)
        if job_dir.exists():
            shutil.rmtree(job_dir)


def _atomic_write_text(tmp: Path, target: Path, content: str) -> None:
    """Write ``content`` to ``tmp``, fsync, then atomically rename to ``target``."""
    tmp.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(content)
            fh.flush()
            os.fsync(fh.fileno())
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    os.rename(tmp, target)
    _fsync_dir(target.parent)


def _fsync_dir(path: Path) -> None:
    """Fsync a directory so renames/unlinks inside it survive a power loss."""
    flags = os.O_RDONLY
    dir_fd = os.open(path, flags)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)
