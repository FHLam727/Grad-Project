"""Bridge Full-Web update jobs to external Weibo and Facebook crawlers.

This module lets the Grad-Project fullweb UI reuse an external MediaCrawler
checkout for the actual crawl phase while still importing the normalized output
back into this repository's `social_media_analytics.db`.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from .project_analytics import ProjectAnalyticsService


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOG_ROOT = PROJECT_ROOT / "tmp" / "full_web_update_jobs"
STAGING_ROOT = PROJECT_ROOT / ".full_web_staging"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a Full-Web weekly update by delegating to MediaCrawler / Apify and then syncing outputs."
    )
    parser.add_argument("--platform", required=True, choices=("wb", "fb"))
    parser.add_argument("--week-start", required=True, help="Inclusive week start date in YYYY-MM-DD.")
    parser.add_argument("--week-end", required=True, help="Inclusive week end date in YYYY-MM-DD.")
    parser.add_argument("--db-path", default="", help="Target social_media_analytics.db path.")
    parser.add_argument("--job-id", default="", help="Optional background job id for traceability.")
    parser.add_argument("--dry-run", action="store_true", help="Print the planned command and exit.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_local_env()

    service = ProjectAnalyticsService(db_path=Path(args.db_path)) if args.db_path else ProjectAnalyticsService()
    service.ensure_schema()
    service._validate_week_window(week_start=args.week_start, week_end=args.week_end)

    if args.platform == "wb":
        summary = run_weibo_update(
            service=service,
            week_start=args.week_start,
            week_end=args.week_end,
            job_id=args.job_id,
            dry_run=bool(args.dry_run),
        )
    else:
        summary = run_facebook_update(
            service=service,
            week_start=args.week_start,
            week_end=args.week_end,
            job_id=args.job_id,
            dry_run=bool(args.dry_run),
        )

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def load_local_env() -> None:
    load_dotenv(PROJECT_ROOT / ".env", override=False)

    env_file = os.getenv("FULL_WEB_MEDIACRAWLER_ENV_FILE", "").strip()
    if env_file:
        load_dotenv(env_file, override=False)

    media_root = os.getenv("FULL_WEB_MEDIACRAWLER_ROOT", "").strip()
    if media_root:
        candidate = Path(media_root) / ".env"
        if candidate.is_file():
            load_dotenv(candidate, override=False)


def run_weibo_update(
    *,
    service: ProjectAnalyticsService,
    week_start: str,
    week_end: str,
    job_id: str,
    dry_run: bool,
) -> dict[str, Any]:
    media_root = resolve_media_root()
    media_python = resolve_media_python(media_root)
    save_root = resolve_output_root("FULL_WEB_WB_SAVE_ROOT", PROJECT_ROOT / "data" / "full_web_sources" / "wb")
    merged_dir = save_root / "weibo" / "merged"
    snapshot = snapshot_files(merged_dir.glob("search_contents_merged_*.jsonl"))

    next_sunday = iso_next_day(week_end)
    command = [
        str(media_python),
        str(media_root / "tools" / "weibo_pipeline.py"),
        "crawl",
        "--crawl-strategy",
        os.getenv("FULL_WEB_WB_CRAWL_STRATEGY", "auto").strip() or "auto",
        "--keyword-mode",
        os.getenv("FULL_WEB_WB_KEYWORD_MODE", "regular").strip() or "regular",
        "--time-start",
        f"{week_start} 00:00:00",
        "--time-end",
        f"{next_sunday} 00:00:00",
        "--save-data-path",
        str(save_root),
        "--login-type",
        os.getenv("FULL_WEB_WB_LOGIN_TYPE", "qrcode").strip() or "qrcode",
        "--page-limit",
        str(parse_int_env("FULL_WEB_WB_PAGE_LIMIT", 50, minimum=1)),
        "--keyword-early-stop-pages",
        str(parse_int_env("FULL_WEB_WB_KEYWORD_EARLY_STOP_PAGES", 5, minimum=1)),
        "--keyword-early-stop-min-in-window",
        str(parse_int_env("FULL_WEB_WB_KEYWORD_EARLY_STOP_MIN_IN_WINDOW", 1, minimum=0)),
    ]

    keywords_csv = os.getenv("FULL_WEB_WB_KEYWORDS", "").strip()
    if keywords_csv:
        command.extend(["--keywords", keywords_csv])

    command.extend(bool_flag_args("headless", parse_bool_env("FULL_WEB_WB_HEADLESS", False)))
    command.extend(bool_flag_args("get-comments", parse_bool_env("FULL_WEB_WB_GET_COMMENTS", False)))
    command.extend(bool_flag_args("get-sub-comments", parse_bool_env("FULL_WEB_WB_GET_SUB_COMMENTS", False)))
    command.extend(bool_flag_args("force-login", parse_bool_env("FULL_WEB_WB_FORCE_LOGIN", False)))
    command.extend(bool_flag_args("keyword-early-stop", parse_bool_env("FULL_WEB_WB_KEYWORD_EARLY_STOP", True)))

    return run_update_workflow(
        platform="wb",
        service=service,
        week_start=week_start,
        week_end=week_end,
        job_id=job_id,
        media_root=media_root,
        command=command,
        before_snapshot=snapshot,
        output_glob=merged_dir.glob("search_contents_merged_*.jsonl"),
        dry_run=dry_run,
    )


def run_facebook_update(
    *,
    service: ProjectAnalyticsService,
    week_start: str,
    week_end: str,
    job_id: str,
    dry_run: bool,
) -> dict[str, Any]:
    media_root = resolve_media_root()
    media_python = resolve_media_python(media_root)
    output_root = resolve_output_root("FULL_WEB_FB_OUTPUT_ROOT", PROJECT_ROOT / "data" / "full_web_sources" / "apify")
    facebook_dir = output_root / "facebook" / "jsonl"
    snapshot = snapshot_files(facebook_dir.glob("facebook_contents_*.jsonl"))

    command = [
        str(media_python),
        str(media_root / "tools" / "facebook_weekly_sync.py"),
        "--start-date",
        week_start,
        "--end-date",
        week_end,
        "--output-root",
        str(output_root),
        "--results-limit",
        str(parse_int_env("FULL_WEB_FB_RESULTS_LIMIT", 20, minimum=1)),
    ]

    keywords_config = os.getenv("FULL_WEB_FB_KEYWORDS_CONFIG", "").strip()
    if keywords_config:
        command.extend(["--keywords-config", keywords_config])

    return run_update_workflow(
        platform="fb",
        service=service,
        week_start=week_start,
        week_end=week_end,
        job_id=job_id,
        media_root=media_root,
        command=command,
        before_snapshot=snapshot,
        output_glob=facebook_dir.glob("facebook_contents_*.jsonl"),
        dry_run=dry_run,
    )


def run_update_workflow(
    *,
    platform: str,
    service: ProjectAnalyticsService,
    week_start: str,
    week_end: str,
    job_id: str,
    media_root: Path,
    command: list[str],
    before_snapshot: dict[str, tuple[int, int]],
    output_glob,
    dry_run: bool,
) -> dict[str, Any]:
    log_path = build_log_path(platform=platform, week_start=week_start, week_end=week_end)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    summary: dict[str, Any] = {
        "platform": platform,
        "week_start": week_start,
        "week_end": week_end,
        "db_path": str(service.db_path),
        "media_root": str(media_root),
        "command": command,
        "log_path": str(log_path),
    }

    if dry_run:
        summary["status"] = "dry_run"
        return summary

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    existing_pythonpath = env.get("PYTHONPATH", "").strip()
    env["PYTHONPATH"] = f"{media_root}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else str(media_root)

    with log_path.open("w", encoding="utf-8") as log_handle:
        log_handle.write(
            f"[full_web_update] platform={platform} window={week_start}..{week_end} started_at={datetime.now().isoformat()}\n"
        )
        log_handle.write(f"[full_web_update] command={' '.join(command)}\n\n")
        completed = subprocess.run(
            command,
            cwd=media_root,
            env=env,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )

    summary["returncode"] = completed.returncode
    if completed.returncode != 0:
        summary["status"] = "crawl_failed"
        summary["error"] = tail_text(log_path, 2400)
        return summary

    changed_files = discover_changed_files(before_snapshot, output_glob)
    if not changed_files:
        summary["status"] = "no_files_detected"
        summary["error"] = (
            "Crawler finished but no new or updated normalized content files were detected. "
            "Check the job log for captcha/login interruptions or empty crawl results."
        )
        summary["log_tail"] = tail_text(log_path, 2400)
        return summary

    staged_files = copy_files_to_staging(
        platform=platform,
        week_start=week_start,
        week_end=week_end,
        changed_files=changed_files,
    )
    staged_window = service.stage_update_window(
        platform=platform,
        week_start=week_start,
        week_end=week_end,
        staged_files=staged_files,
        job_id=job_id,
        note="Crawl finished. Review and confirm import before writing these files into the analytics database.",
    )
    summary["status"] = "staged"
    summary["changed_files"] = [str(path) for path in changed_files]
    summary["staged_files"] = [str(path) for path in staged_files]
    summary["staged_window"] = staged_window
    summary["log_tail"] = tail_text(log_path, 2400)
    return summary


def resolve_media_root() -> Path:
    raw = os.getenv("FULL_WEB_MEDIACRAWLER_ROOT", "").strip()
    candidate = Path(raw) if raw else Path("/Users/sunjia/MediaCrawler")
    if not (candidate / "main.py").is_file():
        raise ValueError(
            "FULL_WEB_MEDIACRAWLER_ROOT is not configured correctly. "
            f"Expected to find main.py under {candidate}."
        )
    return candidate


def resolve_media_python(media_root: Path) -> Path:
    configured = os.getenv("FULL_WEB_MEDIACRAWLER_PYTHON", "").strip()
    candidates = [
        Path(configured) if configured else None,
        media_root / ".venv" / "bin" / "python",
        media_root / "venv" / "bin" / "python",
        Path(sys.executable),
    ]
    for candidate in candidates:
        if candidate and candidate.exists():
            return candidate
    raise ValueError("Unable to locate a Python executable for the external MediaCrawler checkout.")


def resolve_output_root(env_key: str, default: Path) -> Path:
    raw = os.getenv(env_key, "").strip()
    path = Path(raw) if raw else default
    path.mkdir(parents=True, exist_ok=True)
    return path


def build_log_path(*, platform: str, week_start: str, week_end: str) -> Path:
    safe_start = week_start.replace("-", "")
    safe_end = week_end.replace("-", "")
    return LOG_ROOT / f"{platform}_{safe_start}_{safe_end}.log"


def copy_files_to_staging(
    *,
    platform: str,
    week_start: str,
    week_end: str,
    changed_files: list[Path],
) -> list[Path]:
    week_root = STAGING_ROOT / platform / f"{week_start}__{week_end}"
    if week_root.exists():
        shutil.rmtree(week_root)
    week_root.mkdir(parents=True, exist_ok=True)

    staged_files: list[Path] = []
    for source in changed_files:
        target = week_root / source.name
        shutil.copy2(source, target)
        staged_files.append(target)
    return staged_files


def snapshot_files(paths) -> dict[str, tuple[int, int]]:
    snapshot: dict[str, tuple[int, int]] = {}
    for path in paths:
        if not path.is_file():
            continue
        stat = path.stat()
        snapshot[str(path)] = (stat.st_mtime_ns, stat.st_size)
    return snapshot


def discover_changed_files(
    before_snapshot: dict[str, tuple[int, int]],
    paths,
) -> list[Path]:
    changed: list[Path] = []
    for path in sorted(paths):
        if not path.is_file():
            continue
        stat = path.stat()
        current = (stat.st_mtime_ns, stat.st_size)
        if before_snapshot.get(str(path)) != current:
            changed.append(path)
    return changed


def tail_text(path: Path, max_chars: int) -> str:
    if not path.is_file():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    if len(text) <= max_chars:
        return text
    return text[-max_chars:]


def bool_flag_args(flag_name: str, enabled: bool) -> list[str]:
    return [f"--{flag_name}" if enabled else f"--no-{flag_name}"]


def parse_bool_env(env_key: str, default: bool) -> bool:
    raw = os.getenv(env_key, "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return default


def parse_int_env(env_key: str, default: int, *, minimum: int = 0) -> int:
    raw = os.getenv(env_key, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, value)


def iso_next_day(day_text: str) -> str:
    parsed = date.fromisoformat(day_text)
    return (parsed + timedelta(days=1)).isoformat()


if __name__ == "__main__":
    raise SystemExit(main())
