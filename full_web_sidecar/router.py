from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException

from .project_analytics import full_web_analytics_service
from .project_jobs import full_web_job_manager

try:
    from trad_simp import to_trad as _to_trad
except Exception:  # pragma: no cover - fallback keeps API available if opencc is missing
    def _to_trad(text: str) -> str:
        return text


router = APIRouter(prefix="/api/full-web-heat-analysis", tags=["full-web-heat-analysis"])


DISPLAY_TRADITIONAL_ALIAS_KEYS = {
    "cluster_key",
    "source_cluster_key",
    "target_cluster_key",
    "event_family_key",
}

RAW_STRING_KEYS = {
    "job_id",
    "job_type",
    "status",
    "platform",
    "week_start",
    "week_end",
    "month_key",
    "quarter_key",
    "window_mode",
    "db_path",
    "log_path",
    "staged_files",
    "imported_files",
    "command",
    "last_job_id",
    "created_at",
    "updated_at",
    "started_at",
    "finished_at",
    "latest_updated_at",
    "latest_updated_date",
    "coverage_start_date",
    "coverage_end_date",
    "url",
}


def _to_traditional_display(value, field_name: str | None = None):
    if isinstance(value, dict):
        payload = {}
        for key, item in value.items():
            if key in DISPLAY_TRADITIONAL_ALIAS_KEYS and isinstance(item, str):
                payload[key] = item
                payload[f"{key}_display"] = _to_trad(item)
                continue
            payload[key] = _to_traditional_display(item, field_name=key)
        return payload
    if isinstance(value, list):
        if field_name in RAW_STRING_KEYS:
            return list(value)
        return [_to_traditional_display(item, field_name=field_name) for item in value]
    if isinstance(value, str):
        if field_name in RAW_STRING_KEYS or field_name in DISPLAY_TRADITIONAL_ALIAS_KEYS:
            return value
        return _to_trad(value)
    return value


@router.get("/overview")
async def get_full_web_overview(platform: Optional[str] = "wb", auto_sync: bool = False):
    try:
        if auto_sync:
            full_web_analytics_service.sync(platform=platform)
        return _to_traditional_display(full_web_analytics_service.get_overview(platform=platform))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/analysis-windows")
async def get_full_web_analysis_windows(
    platform: Optional[str] = "wb",
    weeks: int = 24,
    window_mode: str = "monthly",
):
    try:
        return _to_traditional_display(full_web_analytics_service.list_analysis_windows(
            platform=platform or "wb",
            weeks=weeks,
            window_mode=window_mode,
        ))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/event-clusters")
async def get_full_web_event_clusters(
    platform: Optional[str] = "wb",
    q: str = "",
    dashboard_category: str = "",
    limit: int = 40,
    offset: int = 0,
    week_start: str = "",
    week_end: str = "",
    month_key: str = "",
    quarter_key: str = "",
):
    try:
        return _to_traditional_display(full_web_analytics_service.list_event_clusters(
            platform=platform,
            q=q,
            dashboard_category=dashboard_category,
            limit=limit,
            offset=offset,
            week_start=week_start,
            week_end=week_end,
            month_key=month_key,
            quarter_key=quarter_key,
        ))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/topic-clusters")
async def get_full_web_topic_clusters(
    platform: Optional[str] = "wb",
    q: str = "",
    dashboard_category: str = "",
    limit: int = 40,
    offset: int = 0,
    week_start: str = "",
    week_end: str = "",
    month_key: str = "",
    quarter_key: str = "",
):
    try:
        return _to_traditional_display(full_web_analytics_service.list_topic_clusters(
            platform=platform,
            q=q,
            dashboard_category=dashboard_category,
            limit=limit,
            offset=offset,
            week_start=week_start,
            week_end=week_end,
            month_key=month_key,
            quarter_key=quarter_key,
        ))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/event-trend")
async def get_full_web_event_trend(
    platform: Optional[str] = "wb",
    event_family_key: str = "",
    days: int = 7,
    start_date: str = "",
    end_date: str = "",
    week_start: str = "",
    week_end: str = "",
    month_key: str = "",
    quarter_key: str = "",
):
    try:
        return _to_traditional_display(full_web_analytics_service.get_event_discussion_trend(
            platform=platform,
            event_family_key=event_family_key,
            days=days,
            start_date=start_date,
            end_date=end_date,
            week_start=week_start,
            week_end=week_end,
            month_key=month_key,
            quarter_key=quarter_key,
        ))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/extract-events")
async def extract_full_web_events(
    platform: Optional[str] = "wb",
    status: str = "ready",
    replace: bool = True,
    week_start: str = "",
    week_end: str = "",
    month_key: str = "",
):
    try:
        if month_key:
            return _to_traditional_display(full_web_analytics_service.extract_events_monthly(
                platform=platform or "wb",
                month_key=month_key,
                status=status,
                replace=replace,
            ))
        if week_start or week_end:
            return _to_traditional_display(full_web_analytics_service.extract_events_weekly(
                platform=platform or "wb",
                week_start=week_start,
                week_end=week_end,
                status=status,
                replace=replace,
            ))
        return _to_traditional_display(full_web_analytics_service.extract_events(
            platform=platform,
            status=status,
            replace=replace,
        ))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/cluster-feedback")
async def submit_full_web_cluster_feedback(
    platform: Optional[str] = "wb",
    board_type: str = "event",
    action: str = "",
    source_cluster_key: str = "",
    target_cluster_key: str = "",
    week_start: str = "",
    week_end: str = "",
    month_key: str = "",
    quarter_key: str = "",
    note: str = "",
):
    try:
        return _to_traditional_display(full_web_analytics_service.submit_cluster_feedback(
            platform=platform or "wb",
            board_type=board_type,
            action=action,
            source_cluster_key=source_cluster_key,
            target_cluster_key=target_cluster_key,
            week_start=week_start,
            week_end=week_end,
            month_key=month_key,
            quarter_key=quarter_key,
            note=note,
        ))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/update-week")
async def update_full_web_week(
    platform: Optional[str] = "wb",
    week_start: str = "",
    week_end: str = "",
    db_path: str = "",
):
    try:
        return _to_traditional_display(full_web_job_manager.start_update_job(
            platform=platform or "wb",
            week_start=week_start,
            week_end=week_end,
            db_path=db_path,
        ))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/confirm-week-import")
async def confirm_full_web_week_import(
    platform: Optional[str] = "wb",
    week_start: str = "",
    week_end: str = "",
    db_path: str = "",
):
    try:
        return _to_traditional_display(full_web_job_manager.confirm_import(
            platform=platform or "wb",
            week_start=week_start,
            week_end=week_end,
            db_path=db_path,
        ))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/jobs/{job_id}/cancel")
async def cancel_full_web_job(job_id: str):
    try:
        return _to_traditional_display(full_web_job_manager.cancel_job(job_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Full-Web job not found: {job_id}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/jobs")
async def list_full_web_jobs(limit: int = 20):
    return _to_traditional_display(full_web_job_manager.list_jobs(limit=limit))


@router.get("/jobs/{job_id}")
async def get_full_web_job(job_id: str):
    try:
        return _to_traditional_display(full_web_job_manager.get_job(job_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Full-Web job not found: {job_id}") from exc
