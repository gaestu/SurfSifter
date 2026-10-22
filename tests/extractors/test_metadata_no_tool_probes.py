"""Extractor metadata must not spawn tool version probes (issue #70).

``ExtractionTab`` reads ``metadata`` of every extractor several times per
card and per evidence tab, on the GUI thread.
"""

from __future__ import annotations

import subprocess
from unittest.mock import patch


def test_metadata_of_registered_extractors_spawns_no_subprocess(extractor_registry_all):
    assert extractor_registry_all
    with patch.object(subprocess, "run", side_effect=AssertionError("subprocess.run")), \
            patch.object(subprocess, "Popen", side_effect=AssertionError("subprocess.Popen")):
        for _ in range(2):
            for extractor in extractor_registry_all:
                meta = extractor.metadata
                assert meta.name and meta.display_name
