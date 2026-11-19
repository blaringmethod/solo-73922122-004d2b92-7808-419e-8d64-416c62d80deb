"""Job orchestration: introspect -> solve -> render -> verify -> publish.

A package becomes downloadable only after the verification cluster reports a
successful load of the full SQL package **and** both the package and the
SUCCEEDED state have been durably committed to the persistent volume
(see :mod:`app.persistence` for the crash-safe commit protocol). Failures
retain a structured conflict report and logs, never a partial package.

Restart recovery
----------------
On process start :meth:`JobStore.recover` scans the persistent volume:

* jobs still in a non-terminal state (PENDING/GENERATING/VERIFYING) are marked
  INTERRUPTED, their staging scratch is deleted and any package leftovers are
  quarantined — temporary results are never resumed or reused;
* a SUCCEEDED job whose package is missing/incomplete is also marked
  INTERRUPTED (the durable commit never completed);
* fully committed SUCCEEDED jobs are loaded back with their packages, so
  engineers can keep downloading them under the original job id;
* FAILED jobs keep their no-solution / rejection reports and logs.
"""
from __future__ import annotations

import os
import sys
import threading
import time
import uuid
import zipfile
from datetime import datetime, timezone
from io import BytesIO

from .config import settings
from .db import source_connection
from .introspect import IntrospectionError, introspect_tables, read_fk_graph
from .models import (
    ConflictReport,
    GenerateRequest,
    JobResult,
    JobStatus,
)
from .persistence import PACKAGE_FILES, PackageExists, Persistence
from .selector import validate_request
from .solver import Generator, SolverTimeout, Unsatisfiable
from .sqlgen import build_package
from .verifier import verify_package

#: Non-terminal states: a job found in one of these on disk after a restart
#: died mid-flight and must be reported as INTERRUPTED.
_RUNNING_STATES = frozenset(
    {JobStatus.PENDING, JobStatus.GENERATING, JobStatus.VERIFYING}
)


def _crash_if(phase: str) -> None:
    """Test-only abrupt-kill hook (``CRASH_AT`` env var). Disabled if unset."""
    if settings.crash_at and settings.crash_at == phase:
        # Immediate termination without cleanup: exactly what SIGKILL looks
        # like to the persistent volume.
        print(
            f"crash injection: terminating process at phase {phase!r}",
            file=sys.stderr,
            flush=True,
        )
        os._exit(137)


class JobStore:
    """Thread-safe job registry whose authoritative state lives on disk."""

    def __init__(self, persist_dir: str | None = None) -> None:
        self._jobs: dict[str, JobResult] = {}
        self._packages: dict[str, dict[str, str]] = {}
        self._lock = threading.RLock()
        self.persistence = Persistence(persist_dir or settings.persist_dir)
        self.recover()

    # ------------------------------------------------------------- lifecycle
    def reconfigure(self, persist_dir: str) -> None:
        """Point the store at another persistent directory and recover it.

        Intended for the test suite; production keeps a single volume path.
        """
        with self._lock:
            self._jobs.clear()
            self._packages.clear()
            self.persistence = Persistence(persist_dir)
            self.recover()

    def recover(self) -> dict[str, JobResult]:
        """Reconcile on-disk state after a (re)start.

        * staging scratch of every job is removed;
        * unfinished jobs become INTERRUPTED and their package leftovers are
          quarantined (never reused, never downloadable);
        * committed SUCCEEDED jobs with a complete package are restored.
        """
        with self._lock:
            self._jobs.clear()
            self._packages.clear()
            self.persistence.ensure_layout()

            # Any staging directory is by definition an unfinished write.
            for job_id in self.persistence.iter_job_ids():
                self.persistence.remove_staging(job_id)

            recovered: dict[str, JobResult] = {}
            for job_id in self.persistence.iter_job_ids():
                job = self.persistence.read_job(job_id)
                if job is None:
                    continue
                package = self.persistence.read_package(job_id)

                if job.status == JobStatus.SUCCEEDED and package is not None:
                    # Fully durable commit: state and package both survived.
                    self._jobs[job_id] = job
                    self._packages[job_id] = package
                    recovered[job_id] = job
                    continue

                if job.status in _RUNNING_STATES:
                    # The process died during generation/verification/publish.
                    quarantined = self.persistence.quarantine_package(job_id)
                    job = self._mark_interrupted(
                        job,
                        reason=(
                            "服务重启时作业仍在运行（上一进程在生成/验证/发布途中终止），"
                            "已标记为中断；临时结果不会被复用，且不提供下载。"
                        ),
                        quarantined=quarantined,
                    )
                elif job.status == JobStatus.SUCCEEDED:
                    # SUCCEEDED json without a complete package means the
                    # durable commit was interrupted before it finished.
                    self.persistence.quarantine_package(job_id)
                    job = self._mark_interrupted(
                        job,
                        reason=(
                            "作业状态已成功但持久卷上的 SQL 包不完整，提交未完成；"
                            "已标记为中断且不提供下载。"
                        ),
                        quarantined=False,
                    )
                else:
                    # FAILED / previously INTERRUPTED: keep the durable record,
                    # but never let stray package files become downloadable.
                    self.persistence.quarantine_package(job_id)

                self.persistence.write_job(job)
                self._jobs[job_id] = job
                recovered[job_id] = job
            return recovered

    def _mark_interrupted(
        self, job: JobResult, reason: str, quarantined: bool
    ) -> JobResult:
        data = job.model_dump()
        data["status"] = JobStatus.INTERRUPTED
        data["log"] = list(data.get("log") or []) + [
            f"{datetime.now(timezone.utc).time().isoformat()} "
            f"recovery: {reason}"
            + (" 已隔离残留包文件。" if quarantined else "")
        ]
        data["conflict"] = ConflictReport(
            kind="interrupted",
            message=reason,
            details={
                "previous_status": job.status.value,
                "package_quarantined": quarantined,
                "downloadable": False,
            },
        ).model_dump()
        return JobResult(**data)

    # --------------------------------------------------------------- writes
    def create(self) -> str:
        job_id = uuid.uuid4().hex
        with self._lock:
            job = JobResult(
                job_id=job_id,
                status=JobStatus.PENDING,
                created_at=datetime.now(timezone.utc).isoformat(),
            )
            self.persistence.write_job(job)
            self._jobs[job_id] = job
        return job_id

    def get(self, job_id: str) -> JobResult | None:
        with self._lock:
            return self._jobs.get(job_id)

    def package(self, job_id: str) -> dict[str, str] | None:
        with self._lock:
            return self._packages.get(job_id)

    def update(self, job_id: str, **kw) -> None:
        with self._lock:
            job = self._jobs[job_id]
            data = job.model_dump()
            data.update(kw)
            job = JobResult(**data)
            self.persistence.write_job(job)
            self._jobs[job_id] = job

    def append_log(self, job_id: str, line: str) -> None:
        with self._lock:
            job = self._jobs[job_id]
            data = job.model_dump()
            data["log"] = list(data["log"]) + [
                f"{datetime.now(timezone.utc).time().isoformat()} {line}"
            ]
            job = JobResult(**data)
            self.persistence.write_job(job)
            self._jobs[job_id] = job

    # ------------------------------------------------------------- publish
    def stage_package(
        self, job_id: str, package: dict[str, str]
    ) -> None:
        """Durably write package files into the hidden staging area."""
        with self._lock:
            self.persistence.stage_package(job_id, package)

    def commit_published(self, job_id: str) -> None:
        """Promote the staged package and cache it for downloads.

        The package directory is made visible *before* the SUCCEEDED state is
        written (see :func:`run_job`), and an existing package can never be
        overwritten.
        """
        with self._lock:
            if self.persistence.package_dir(job_id).exists():
                raise PackageExists(
                    f"refusing to overwrite existing package for job {job_id}"
                )
            self.persistence.commit_package(job_id)
            package = self.persistence.read_package(job_id)
            # A complete rename guarantees all PACKAGE_FILES are present.
            assert package is not None and len(package) == len(PACKAGE_FILES)
            self._packages[job_id] = package

    def purge_expired(self) -> None:
        now = time.time()
        with self._lock:
            stale = [
                jid
                for jid, job in self._jobs.items()
                if now - datetime.fromisoformat(job.created_at).timestamp()
                > settings.job_ttl_seconds
            ]
            for jid in stale:
                self._jobs.pop(jid, None)
                self._packages.pop(jid, None)
                self.persistence.remove_job_dir(jid)


STORE = JobStore()


def _conflict_from_groups(groups) -> ConflictReport:
    kinds = sorted({g.kind for g in groups})
    table_hint = sorted({r for g in groups for r in g.refs if r})
    lines = [g.label for g in groups]
    summary = "、".join(
        {
            "rows": "行数",
            "range": "取值区间/可空性",
            "notnull": "NOT NULL",
            "unique": "主键/唯一键",
            "fk": "外键",
        }.get(k, k)
        for k in kinds
    )
    return ConflictReport(
        kind="no_solution",
        message=(
            f"无解：以下{summary}约束相互冲突，不存在同时满足指定行数、区间与全部约束的样本。"
            f"涉及对象: {', '.join(table_hint) if table_hint else '(见详情)'}"
        ),
        details={"conflicting_constraints": lines, "kinds": kinds},
    )


def run_job(job_id: str, req: GenerateRequest) -> None:
    store = STORE
    try:
        _crash_if("start")
        store.update(job_id, status=JobStatus.GENERATING)
        store.append_log(job_id, "opening read-only source connection (catalogs only)")
        with source_connection(settings.source_dsn) as conn:
            requested = req.table_names()

            # Step 1: compute the FK closure by expanding the catalog FK graph
            # one level at a time, so transitive targets (and cycles) are all
            # reached, not just direct references of the requested tables.
            fk_graph: dict[str, list[str]] = {}
            selected: list[str] = []
            seen: set[str] = set()
            queue = list(requested)
            closure_ok = True
            while queue:
                t = queue.pop(0)
                if t in seen:
                    continue
                seen.add(t)
                selected.append(t)
                fk_graph.update(read_fk_graph(conn, req.schema_name, [t]))
                for target in fk_graph.get(t, []):
                    if target not in seen:
                        queue.append(target)
                if len(selected) > settings.max_tables:
                    closure_ok = False
                    break
            if not closure_ok or len(selected) > settings.max_tables:
                store.update(
                    job_id,
                    status=JobStatus.FAILED,
                    conflict=ConflictReport(
                        kind="closure",
                        message=(
                            f"外键闭包需要 {len(set(selected) | set(queue))} 张表，"
                            f"超过至多 {settings.max_tables} 张表的限制；请缩小选择范围"
                        ),
                        details={"reached_so_far": selected, "pending": queue},
                    ),
                )
                return
            store.append_log(job_id, f"selected closure: {selected}")
            store.update(job_id, selected_tables=selected)

            # Step 2: full strict introspection of the whole closed set.
            try:
                schemas = introspect_tables(conn, req.schema_name, selected)
            except IntrospectionError as exc:
                store.update(
                    job_id,
                    status=JobStatus.FAILED,
                    conflict=ConflictReport(
                        kind="structure",
                        message="源表结构包含不受支持的对象，已明确拒绝（不会忽略任何结构）",
                        details={"rejections": [
                            {"object": o, "reason": r} for o, r in exc.rejections
                        ]},
                    ),
                )
                return

            # Step 3: validate the user-supplied specs for requested tables.
            requested_req = GenerateRequest(
                schema_name=req.schema_name,
                tables=[s for s in req.tables if s.table in requested],
            )
            problems = validate_request(
                requested_req, schemas, settings.max_rows_per_table
            )
            if problems:
                store.update(
                    job_id,
                    status=JobStatus.FAILED,
                    conflict=ConflictReport(
                        kind="request",
                        message="请求参数与目录结构不一致",
                        details={"problems": [
                            {"object": o, "reason": r} for o, r in problems
                        ]},
                    ),
                )
                return

            # Step 4: closure-added tables get bounded default specs so the
            # solver still constrains every cell, while user tables keep the
            # exact requested row counts and intervals.
            from .models import IntRange, TableSpec, TYPE_BOUNDS

            spec_by_name = {s.table: s for s in req.tables}
            for t in selected:
                if t in spec_by_name:
                    continue
                ts = schemas[t]
                spec_by_name[t] = TableSpec(
                    table=t,
                    rows=min(5, settings.max_rows_per_table),
                    ranges={
                        c.name: IntRange(
                            low=TYPE_BOUNDS[c.pg_type][0],
                            high=TYPE_BOUNDS[c.pg_type][1],
                        )
                        for c in ts.columns
                    },
                )
            req = GenerateRequest(
                schema_name=req.schema_name,
                tables=[spec_by_name[t] for t in selected],
            )

        specs = {s.table: s for s in req.tables}

        _crash_if("generated")
        store.append_log(job_id, "solving constraints with z3 (hard: rows, ranges, PK/UK/FK)")
        gen = Generator({t: schemas[t] for t in selected}, specs)
        try:
            data = gen.solve()
        except Unsatisfiable as unsat:
            store.update(
                job_id,
                status=JobStatus.FAILED,
                conflict=_conflict_from_groups(unsat.groups),
            )
            return
        except SolverTimeout as exc:
            store.append_log(job_id, f"solver timeout: {exc}")
            store.update(
                job_id,
                status=JobStatus.FAILED,
                conflict=ConflictReport(
                    kind="solver_timeout",
                    message=str(exc),
                    details={"note": "这不是无解结论，而是限时内未能判定"},
                ),
            )
            return

        store.append_log(job_id, "rendering SQL package")
        package = build_package(
            {t: schemas[t] for t in selected}, selected, specs, data
        )

        store.update(job_id, status=JobStatus.VERIFYING)
        store.append_log(job_id, "loading package into isolated empty verification database")
        # verify_package manages its own short-lived autocommit connections.
        ok, vlog = verify_package(
            settings.verify_dsn,
            settings.verify_db_prefix,
            req.schema_name,
            {t: schemas[t] for t in selected},
            selected,
            package,
        )
        for line in vlog:
            store.append_log(job_id, f"verify: {line}")
        if not ok:
            store.update(
                job_id,
                status=JobStatus.FAILED,
                conflict=ConflictReport(
                    kind="verification",
                    message="生成的 SQL 在隔离空库装载/负向校验失败，不提供下载（无部分包）",
                    details={"verify_log": vlog},
                ),
            )
            return

        _crash_if("verified")

        # ---- Durable publish: staging -> atomic rename -> SUCCEEDED ----
        # Only a fully verified package reaches this point. Files are first
        # staged (and fsynced) under a hidden directory, then atomically
        # renamed into place, and only then is the SUCCEEDED state committed.
        # A new package can never overwrite an existing one (PackageExists).
        store.stage_package(job_id, package)
        _crash_if("publish_staged")
        store.commit_published(job_id)
        _crash_if("publish_renamed")
        store.append_log(job_id, "package verified and durably published")
        store.update(job_id, status=JobStatus.SUCCEEDED)
        _crash_if("published")
    except Exception as exc:  # defensive: never leave a job dangling
        store.append_log(job_id, f"unexpected error: {exc.__class__.__name__}: {exc}")
        store.update(
            job_id,
            status=JobStatus.FAILED,
            conflict=ConflictReport(
                kind="internal",
                message=f"服务内部错误: {exc.__class__.__name__}: {exc}",
            ),
        )


def package_zip(package: dict[str, str]) -> bytes:
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in package.items():
            zf.writestr(name, content)
    return buf.getvalue()
