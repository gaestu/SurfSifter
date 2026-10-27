"""App data layer for case access and cross-feature aggregation.

This package contains:
- _base.py: BaseDataAccess with connection management and caching infrastructure
- _stats_cache.py: CaseStatsCache, the revision-checked statistics cache
  shared by every data access of one case (issue #63)
- _case.py: CaseMetadataMixin for case metadata operations
- _evidence.py: EvidenceMetadataMixin for evidence metadata operations
- _images.py: ImageQueryMixin for image query operations
- _urls.py: UrlQueryMixin for URL query operations
- _file_list.py: FileListQueryMixin for File List query operations
- _downloads.py: DownloadQueryMixin for download query operations
- case_data.py: CaseDataAccess class for domain-specific database queries

Refactored to use BaseDataAccess base class for modular repository pattern.
Case/evidence metadata extracted to CaseMetadataMixin (_case.py).
Evidence metadata extracted to EvidenceMetadataMixin (_evidence.py).
URL queries extracted to UrlQueryMixin (_urls.py).
Image queries extracted to ImageQueryMixin (_images.py).
Download queries extracted to DownloadQueryMixin (_downloads.py).
"""

from ._base import BaseDataAccess, QueryCancelledError
from ._case import CaseMetadataMixin
from ._downloads import DownloadQueryMixin
from ._evidence import (
    EvidenceCounts,
    EvidenceMetadataMixin,
    OverviewStats,
)
from ._stats_cache import (
    ALL_STATS_CATEGORIES,
    EVIDENCE_COUNTS_CATEGORIES,
    OVERVIEW_STATS_CATEGORIES,
    TAG_STATISTICS_CATEGORIES,
    STATS_IMAGES,
    STATS_OS_INDICATORS,
    STATS_PROCESS_LOG,
    STATS_TAGS,
    STATS_URL_MATCHES,
    STATS_URLS,
    CaseStatsCache,
    StatsWriteScope,
    case_stats_cache,
    discard_case_stats_cache,
)
from ._file_list import (
    FILE_LIST_ANY_MATCH,
    FileListFilterSpec,
    FileListQueryMixin,
)
from ._images import ImageQueryMixin
from ._urls import (
    UrlCursor,
    UrlDomainChildPage,
    UrlDomainChildQuery,
    UrlDomainGroup,
    UrlDomainGroupPage,
    UrlDomainGroupQuery,
    UrlFilterSpec,
    UrlPage,
    UrlPageQuery,
    UrlQueryMixin,
    UrlSearchIndexStatus,
    ascii_fold,
)
from .case_data import CaseDataAccess

__all__ = [
    "ALL_STATS_CATEGORIES",
    "BaseDataAccess",
    "CaseDataAccess",
    "CaseStatsCache",
    "EVIDENCE_COUNTS_CATEGORIES",
    "OVERVIEW_STATS_CATEGORIES",
    "OverviewStats",
    "STATS_IMAGES",
    "STATS_OS_INDICATORS",
    "STATS_PROCESS_LOG",
    "STATS_TAGS",
    "STATS_URL_MATCHES",
    "STATS_URLS",
    "StatsWriteScope",
    "TAG_STATISTICS_CATEGORIES",
    "case_stats_cache",
    "discard_case_stats_cache",
    "CaseMetadataMixin",
    "DownloadQueryMixin",
    "EvidenceMetadataMixin",
    "EvidenceCounts",
    "FILE_LIST_ANY_MATCH",
    "FileListFilterSpec",
    "FileListQueryMixin",
    "ImageQueryMixin",
    "QueryCancelledError",
    "UrlCursor",
    "UrlDomainChildPage",
    "UrlDomainChildQuery",
    "UrlDomainGroup",
    "UrlDomainGroupPage",
    "UrlDomainGroupQuery",
    "UrlFilterSpec",
    "UrlPage",
    "UrlPageQuery",
    "UrlQueryMixin",
    "UrlSearchIndexStatus",
    "ascii_fold",
]
