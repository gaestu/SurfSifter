"""App data layer for case access and cross-feature aggregation.

This package contains:
- _base.py: BaseDataAccess with connection management and caching infrastructure
- _case.py: CaseMetadataMixin for case metadata operations
- _evidence.py: EvidenceMetadataMixin for evidence metadata operations
- _images.py: ImageQueryMixin for image query operations
- _urls.py: UrlQueryMixin for URL query operations
- _downloads.py: DownloadQueryMixin for download query operations
- case_data.py: CaseDataAccess class for domain-specific database queries

Refactored to use BaseDataAccess base class for modular repository pattern.
Case/evidence metadata extracted to CaseMetadataMixin (_case.py).
Evidence metadata extracted to EvidenceMetadataMixin (_evidence.py).
URL queries extracted to UrlQueryMixin (_urls.py).
Image queries extracted to ImageQueryMixin (_images.py).
Download queries extracted to DownloadQueryMixin (_downloads.py).
"""

from ._base import BaseDataAccess
from ._case import CaseMetadataMixin
from ._downloads import DownloadQueryMixin
from ._evidence import EvidenceMetadataMixin, EvidenceCounts
from ._images import ImageQueryMixin
from ._urls import UrlQueryMixin
from .case_data import CaseDataAccess

__all__ = [
    "BaseDataAccess",
    "CaseDataAccess",
    "CaseMetadataMixin",
    "DownloadQueryMixin",
    "EvidenceMetadataMixin",
    "EvidenceCounts",
    "ImageQueryMixin",
    "UrlQueryMixin",
]
