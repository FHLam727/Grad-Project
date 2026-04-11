"""Background jobs for weekly update actions on the main analytics database."""

from __future__ import annotations

import json
import signal
import subprocess
import sys
import threading
import uuid
import os
import shlex
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .project_analytics import ProjectAnalyticsService
from .update_bridge import build_log_path, tail_text


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat()


class ProjectJobManager:
    """Small in-memory job registry for main project weekly update tasks."""

    def __init__(self) -> None:
        self._jobs: dict[str, dict[str, Any]] = {}
        self._processes: dict[str, subprocess.Popen[str]] = {}
        self._lock = threading.Lock()

    def start_update_job(
        self,
        *,
        platform: str,
        week_start: str,
        week_end: str,
        db_path: str = "",
    ) -> dict[str, Any]:
        job_id = uuid.uuid4().hex
        payload = {
            "platform": platform,
            "week_start": week_start,
            "week_end": week_end,
            "db_path": db_path,
        }
        record = {
            "job_id": job_id,
            "job_type": "update_week",
            "status": "queued",
            "payload": payload,
            "summary": None,
            "command": [],
            "error": "",
            "created_at": _now_iso(),
            "started_at": "",
            "finished_at": "",
        }
        with self._lock:
            self._jobs[job_id] = record
            self._trim_jobs_unlocked()

        thread = threading.Thread(target=self._run_update_job, args=(job_id, payload), daemon=True)
        thread.start()
        return self.get_job(job_id)

    def get_job(self, job_id: str) -> dict[str, Any]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(job_id)
            snapshot = dict(job)
        return self._augment_job_snapshot(snapshot)

    def list_jobs(self, *, limit: int = 20) -> dict[str, Any]:
        safe_limit = max(1, min(int(limit), 100))
        with self._lock:
            jobs = sorted(self._jobs.values(), key=lambda item: item.get("created_at", ""), reverse=True)[:safe_limit]
            total = len(self._jobs)
        return {"items": [self._augment_job_snapshot(dict(job)) for job in jobs], "total": total, "limit": safe_limit}

    def latest_job(self) -> dict[str, Any] | None:
        items = self.list_jobs(limit=1)["items"]
        return items[0] if items else None

    def cancel_job(self, job_id: str) -> dict[str, Any]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(job_id)
            status = str(job.get("status") or "")
            if status not in {"queued", "running", "cancelling"}:
                raise ValueError(f"Job {job_id} is not running and cannot be cancelled.")
            self._jobs[job_id] = dict(job) | {"status": "cancelling"}
            process = self._processes.get(job_id)

        if process and process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except OSError:
                pass
        return self.get_job(job_id)

    def _run_update_job(self, job_id: str, payload: dict[str, Any]) -> None:
        try:
            service, matched, command = prepare_project_update_week_job(**payload)
        except Exception as exc:  # pragma: no cover - background worker safeguard
            self._update_job(job_id, status="failed", error=str(exc), finished_at=_now_iso())
            return

        self._update_job(job_id, status="running", started_at=_now_iso(), command=command)
        process = subprocess.Popen(
            command,
            cwd=PROJECT_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        self._set_process(job_id, process)
        stdout, stderr = process.communicate()
        self._clear_process(job_id)

        job_status = self.get_job(job_id).get("status", "")
        if job_status == "cancelling":
            self._update_job(
                job_id,
                status="cancelled",
                error="The crawl job was cancelled before import confirmation.",
                finished_at=_now_iso(),
            )
            return

        if process.returncode != 0:
            detail = (stderr or stdout or "").strip()
            self._update_job(
                job_id,
                status="failed",
                error=detail[:2000] or "Update command failed.",
                finished_at=_now_iso(),
            )
            return

        parsed_summary = parse_command_summary(stdout)
        if parsed_summary.get("status") == "staged":
            updated = service.list_analysis_windows(platform=payload["platform"], weeks=104)["items"]
            matched_window = next(
                (
                    item
                    for item in updated
                    if item["week_start"] == payload["week_start"] and item["week_end"] == payload["week_end"]
                ),
                matched,
            )
            self._update_job(
                job_id,
                status="awaiting_confirmation",
                summary=parsed_summary | {"window": matched_window},
                finished_at=_now_iso(),
            )
            return

        self._update_job(job_id, status="completed", summary=parsed_summary, finished_at=_now_iso())

    def confirm_import(
        self,
        *,
        platform: str,
        week_start: str,
        week_end: str,
        db_path: str = "",
    ) -> dict[str, Any]:
        service = ProjectAnalyticsService(db_path=Path(db_path)) if db_path else ProjectAnalyticsService()
        service.ensure_schema()
        self._update_job_for_window(platform=platform, week_start=week_start, week_end=week_end, status="importing")
        try:
            summary = service.confirm_staged_update_window(
                platform=platform,
                week_start=week_start,
                week_end=week_end,
                force=False,
            )
        except Exception as exc:
            self._update_job_for_window(
                platform=platform,
                week_start=week_start,
                week_end=week_end,
                status="failed",
                error=str(exc),
                finished_at=_now_iso(),
            )
            raise

        self._update_job_for_window(
            platform=platform,
            week_start=week_start,
            week_end=week_end,
            status="completed",
            summary=summary,
            error="",
            finished_at=_now_iso(),
        )
        return summary

    def _update_job(self, job_id: str, **updates: Any) -> None:
        with self._lock:
            if job_id not in self._jobs:
                return
            self._jobs[job_id] = dict(self._jobs[job_id]) | updates

    def _update_job_for_window(
        self,
        *,
        platform: str,
        week_start: str,
        week_end: str,
        **updates: Any,
    ) -> None:
        with self._lock:
            for job_id, record in self._jobs.items():
                payload = record.get("payload") or {}
                if (
                    payload.get("platform") == platform
                    and payload.get("week_start") == week_start
                    and payload.get("week_end") == week_end
                ):
                    self._jobs[job_id] = dict(record) | updates
                    break

    def _set_process(self, job_id: str, process: subprocess.Popen[str]) -> None:
        with self._lock:
            self._processes[job_id] = process

    def _clear_process(self, job_id: str) -> None:
        with self._lock:
            self._processes.pop(job_id, None)

    def _trim_jobs_unlocked(self, max_jobs: int = 50) -> None:
        if len(self._jobs) <= max_jobs:
            return
        ordered_ids = sorted(self._jobs, key=lambda item: self._jobs[item].get("created_at", ""))
        for job_id in ordered_ids[:-max_jobs]:
            self._jobs.pop(job_id, None)

    def _augment_job_snapshot(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        payload = snapshot.get("payload") or {}
        platform = str(payload.get("platform") or "").strip()
        week_start = str(payload.get("week_start") or "").strip()
        week_end = str(payload.get("week_end") or "").strip()
        if platform and week_start and week_end:
            log_path = build_log_path(platform=platform, week_start=week_start, week_end=week_end)
            snapshot["log_path"] = str(log_path)
            snapshot["log_tail"] = tail_text(log_path, 3200)
        else:
            snapshot["log_path"] = ""
            snapshot["log_tail"] = ""
        return snapshot


def prepare_project_update_week_job(*, platform: str, week_start: str, week_end: str, db_path: str = "") -> tuple[ProjectAnalyticsService, dict[str, Any] | None, list[str]]:
    service = ProjectAnalyticsService(db_path=Path(db_path)) if db_path else ProjectAnalyticsService()
    service.ensure_schema()
    service._validate_week_window(week_start=week_start, week_end=week_end)

    current_windows = service.list_analysis_windows(platform=platform, weeks=104)["items"]
    matched = next((item for item in current_windows if item["week_start"] == week_start and item["week_end"] == week_end), None)
    if matched and matched.get("status") == "future":
        raise ValueError(f"Week {week_start} to {week_end} is still in the future and cannot be crawled yet.")

    command = build_update_command(platform=platform, week_start=week_start, week_end=week_end, db_path=str(service.db_path))
    return service, matched, command


def build_update_command(*, platform: str, week_start: str, week_end: str, db_path: str) -> list[str]:
    next_sunday_value = (datetime.fromisoformat(week_end).date() + timedelta(days=1)).isoformat()
    env_key = "FULL_WEB_FB_UPDATE_COMMAND" if platform == "fb" else "FULL_WEB_WB_UPDATE_COMMAND" if platform == "wb" else ""
    if not env_key:
        raise ValueError(f"Unsupported platform for weekly update: {platform}")

    template = os.getenv(env_key, "").strip()
    if not template:
        raise ValueError(
            f"{env_key} is not configured. Set a platform-specific update command before using Update Database."
        )

    rendered = template.format(
        week_start=week_start,
        week_end=week_end,
        next_sunday=next_sunday_value,
        db_path=db_path,
        job_id="",
        python=sys.executable,
        project_root=str(PROJECT_ROOT),
    )
    return shlex.split(rendered)


def parse_command_summary(stdout: str) -> dict[str, Any]:
    text = (stdout or "").strip()
    if not text:
        return {"status": "completed"}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {
            "status": "completed",
            "stdout": text[-2000:],
        }


full_web_job_manager = ProjectJobManager()
