from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from core.database import DatabaseManager

from ._case import CaseMetadataMixin
from ._downloads import DownloadQueryMixin
from ._evidence import EvidenceMetadataMixin, EvidenceCounts
from ._images import ImageQueryMixin
from ._indicators import IndicatorQueryMixin
from ._tags import TagQueryMixin
from ._timeline import TimelineQueryMixin
from ._urls import UrlQueryMixin


class CaseDataAccess(TagQueryMixin, TimelineQueryMixin, IndicatorQueryMixin, ImageQueryMixin, UrlQueryMixin, DownloadQueryMixin, EvidenceMetadataMixin, CaseMetadataMixin):
    """Data access layer for case databases.

    Inherits connection management and caching from BaseDataAccess (via mixins).
    Provides domain-specific queries for URLs, images, timeline, tags, etc.

    Added in-memory filter cache for improved tab switching performance.
    Thread-safe evidence connection handling using thread-local storage.
    Added close() method to properly release database connections.
    Refactored to inherit from BaseDataAccess.
    Case/evidence metadata extracted to CaseMetadataMixin.
    Evidence metadata extracted to EvidenceMetadataMixin.
    URL queries extracted to UrlQueryMixin.
    Image queries extracted to ImageQueryMixin.
    Indicator queries extracted to IndicatorQueryMixin.
    Timeline queries extracted to TimelineQueryMixin (feature-local).
    Moved find_similar_images, list_hash_matches to ImageQueryMixin.
    Download queries extracted to DownloadQueryMixin.
    get_top_domains() moved to UrlQueryMixin.
    """

    def __init__(
        self,
        case_folder: Path,
        db_path: Optional[Path] = None,
        *,
        db_manager: Optional[DatabaseManager] = None,
    ) -> None:
        """Initialize case data access.

        Args:
            case_folder: Path to the case folder
            db_path: Optional explicit path to case database
            db_manager: Optional pre-configured DatabaseManager instance
        """
        super().__init__(case_folder, db_path, db_manager=db_manager)
