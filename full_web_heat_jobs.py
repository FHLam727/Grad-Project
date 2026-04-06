"""Compatibility wrapper for the vendored Full-Web weekly job manager."""

from full_web_backend.project_jobs import (
    build_update_command,
    project_job_manager as heat_job_manager,
    run_project_update_week_job as run_heat_update_week_job,
)
