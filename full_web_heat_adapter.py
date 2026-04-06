"""Compatibility wrapper for the vendored Full-Web Heat Analysis backend."""

from __future__ import annotations

from functools import lru_cache

from full_web_backend import get_project_analytics_service
from full_web_backend.project_analytics import ProjectAnalyticsService


def get_project_analytics_service_class():
    return ProjectAnalyticsService


@lru_cache(maxsize=1)
def get_mediacrawler_root():
    raise RuntimeError(
        "Full-Web Heat Analysis is now self-contained in Grad-Project. "
        "MediaCrawler is no longer required for the analytics service itself."
    )
