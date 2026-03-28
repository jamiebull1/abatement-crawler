"""Abatement Data Crawler package."""

from .config import CrawlerConfig
from .models import AbatementRecord, ScopeConfig

__version__ = "0.1.0"
__all__ = ["AbatementRecord", "ScopeConfig", "CrawlerConfig"]
