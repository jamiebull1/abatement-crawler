"""Abatement Data Crawler package."""

from .models import AbatementRecord, ScopeConfig
from .config import CrawlerConfig

__version__ = "0.1.0"
__all__ = ["AbatementRecord", "ScopeConfig", "CrawlerConfig"]
