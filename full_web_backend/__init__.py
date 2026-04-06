"""Self-contained Full-Web Heat Analysis backend for Grad-Project."""

from __future__ import annotations

from functools import lru_cache

from .project_analytics import ProjectAnalyticsService
from .project_jobs import project_job_manager


@lru_cache(maxsize=1)
def get_project_analytics_service() -> ProjectAnalyticsService:
    service = ProjectAnalyticsService()
    service.ensure_schema()
    return service


def get_project_job_manager():
    return project_job_manager
