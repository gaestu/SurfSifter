"""Images tab: URL / Cache Key Exclude fields and the "Untagged" entry (issue #74).

Covers the Exclude fields' debounce, Reset (including the Table size reset),
the fixed "Untagged" entry (distinct from a tag named "Untagged") surviving
filter-list reloads, and tagging while "Untagged" is active.
"""
from __future__ import annotations

import gc
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest
from PySide6.QtCore import QCoreApplication, QEvent

import app.features.images.tab as tab_module
from app.data import CaseDataAccess, discard_case_stats_cache
from app.features.images.tab import ImagesTab
from core.database import DatabaseManager, init_db

pytestmark = pytest.mark.gui_offscreen

TIMEOUT_MS = 10000
EV = 1

# key -> (filename, size_bytes, discoveries [(discovered_by, cache_url, cache_key)], tag names)
IMAGES: List[Tuple[str, str, Optional[int], List[Tuple[str, Optional[str], Optional[str]]], List[str]]] = [
    ("ads", "a.jpg", 1000, [("cache_simple", "https://ads.tracker.com/a.gif", "ads-key")], []),
    ("multi", "b.jpg", 70_000,
     [("cache_firefox", "https://cdn.example.com/b.jpg", "cdn-key"),
      ("cache_simple", "https://ADS.example.com/b.jpg", "Key_100%")], ["Relevant"]),
    ("cdn", "c.png", 2000, [("cache_simple", "https://cdn.example.com/c.png", "cdn_c")], []),
    ("pct", "d.jpg", 3000, [("cache_blockfile", "https://x.example/100%_off", "key_d")], []),
    ("fs", "e.jpg", 900_000, [("filesystem_images", None, None)], []),
    ("carved", "f.gif", None, [("foremost_carver", None, None)], []),
    ("no_disc", "g.jpg", 40, [], []),
    ("named_untagged", "h.jpg", 5000, [("cache_simple", "https://h.example/h.jpg", "h-key")], ["Untagged"]),
    ("relevant", "i.jpg", 600, [("cache_simple", "https://i.example/i.jpg", "i-key")], ["Relevant"]),
]


@pytest.fixture
def images_case(tmp_path: Path):
    case_dir = tmp_path / "case"
    case_dir.mkdir()
    case_db_path = case_dir / "IMGGUI_surfsifter.sqlite"
    case_conn = init_db(case_dir, db_path=case_db_path)
    with case_conn:
        case_conn.execute(
            "INSERT INTO cases (case_id, title, created_at_utc) VALUES (?, ?, ?)",
            ("CASE_74G", "Images #74 GUI", "2025-01-01T00:00:00Z"),
        )
        case_conn.execute(
            "INSERT INTO evidences (case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
            (1, "EV-74G", "/tmp/test.E01", "2025-01-01T00:00:00Z"),
        )
    case_conn.close()
    discard_case_stats_cache(case_db_path)

    manager = DatabaseManager(case_dir, case_db_path=case_db_path, enable_split=True)
    conn = manager.get_evidence_conn(EV, label="EV-74G")
    ids: Dict[str, int] = {}
    with conn:
        for index, (key, filename, size, discoveries, tags) in enumerate(IMAGES):
            image_id = conn.execute(
                "INSERT INTO images (evidence_id, rel_path, filename, md5, sha256, first_discovered_by, ts_utc, "
                "size_bytes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (EV, f"images/{filename}", filename, f"{index:032x}", f"{index:064x}",
                 discoveries[0][0] if discoveries else "filesystem_images",
                 f"2025-01-{index + 1:02d}T00:00:00Z", size),
            ).lastrowid
            ids[key] = image_id
            for source, url, cache_key in discoveries:
                conn.execute(
                    "INSERT INTO image_discoveries (evidence_id, image_id, discovered_by, run_id, cache_url, cache_key) "
                    "VALUES (?, ?, ?, 'run-1', ?, ?)",
                    (EV, image_id, source, url, cache_key),
                )
            for name in tags:
                row = conn.execute(
                    "SELECT id FROM tags WHERE evidence_id = ? AND name_normalized = ?", (EV, name.lower())
                ).fetchone()
                tag_id = row[0] if row else conn.execute(
                    "INSERT INTO tags (evidence_id, name, name_normalized) VALUES (?, ?, ?)",
                    (EV, name, name.lower()),
                ).lastrowid
                conn.execute(
                    "INSERT INTO tag_associations (tag_id, evidence_id, artifact_type, artifact_id) "
                    "VALUES (?, ?, 'image', ?)",
                    (tag_id, EV, image_id),
                )
    manager.close_all()
    data = CaseDataAccess(case_dir, case_db_path)
    yield data, case_dir, ids
    data.close()
    discard_case_stats_cache(case_db_path)


def _wait_filters(qtbot, tab: ImagesTab) -> None:
    qtbot.waitUntil(lambda: tab.source_combo.isEnabled() and tab.hash_match_combo.isEnabled(), timeout=TIMEOUT_MS)
    assert tab._filter_worker is not None and tab._filter_worker.wait(TIMEOUT_MS)


@pytest.fixture
def make_tab(qtbot, images_case):
    created: List[ImagesTab] = []

    def _make() -> ImagesTab:
        data, case_dir, _ = images_case
        tab = ImagesTab(data, case_dir)
        created.append(tab)
        tab.set_evidence(EV, defer_load=True)
        tab._perform_deferred_load()
        _wait_filters(qtbot, tab)
        return tab

    yield _make
    # Stop the workers and delete the tabs here, in the GUI thread, before
    # the next test starts its own workers.
    for tab in created:
        tab.shutdown()
        tab.deleteLater()
    created.clear()
    QCoreApplication.sendPostedEvents(None, QEvent.DeferredDelete)
    gc.collect()


def _grid_ids(tab: ImagesTab) -> List[int]:
    return [row["id"] for row in tab.model._rows]


def _table_ids(tab: ImagesTab) -> List[int]:
    return [row["id"] for row in tab.table_model._rows]


def _expected(data: CaseDataAccess, **kwargs: Any) -> List[int]:
    return [row["id"] for row in data.iter_images(EV, limit=10_000, **kwargs)]


def _assert_both(tab: ImagesTab, expected: List[int]) -> None:
    assert _grid_ids(tab) == expected
    assert _table_ids(tab) == expected


def _select(combo, value: Any) -> None:
    index = combo.findData(value)
    assert index >= 0, value
    combo.setCurrentIndex(index)


def _timers(tab: ImagesTab):
    return (
        tab._url_filter_timer,
        tab._cache_key_filter_timer,
        tab._url_exclude_timer,
        tab._cache_key_exclude_timer,
    )


# ---------------------------------------------------------------------------
# Exclude fields
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "field_name, timer_name, filter_key, query_kwarg, text",
    [
        ("url_exclude_input", "_url_exclude_timer", "url_exclude_text", "url_excludes", "ADS"),
        ("cache_key_exclude_input", "_cache_key_exclude_timer", "cache_key_exclude_text", "cache_key_excludes", "cdn"),
    ],
)
def test_exclude_fields_apply_after_debounce(qtbot, make_tab, images_case, field_name, timer_name, filter_key,
                                             query_kwarg, text):
    data, _, _ = images_case
    tab = make_tab()
    every = _expected(data)
    _assert_both(tab, every)
    field = getattr(tab, field_name)
    timer = getattr(tab, timer_name)
    assert timer.interval() == 400 and timer.isSingleShot()

    qtbot.keyClicks(field, text)
    # Not applied while typing
    assert timer.isActive()
    assert tab.model._filters[filter_key] is None and tab.table_model._filters[filter_key] is None
    _assert_both(tab, every)

    qtbot.waitUntil(lambda: not timer.isActive(), timeout=TIMEOUT_MS)
    assert tab.model._filters[filter_key] == text and tab.table_model._filters[filter_key] == text
    excluded = _expected(data, **{query_kwarg: text})
    assert excluded and len(excluded) < len(every)
    _assert_both(tab, excluded)
    assert getattr(tab.filters, filter_key) == text

    # Whitespace around the text is stripped; clearing restores everything
    field.setText(f"  {text}  ")
    qtbot.waitUntil(lambda: not timer.isActive(), timeout=TIMEOUT_MS)
    _assert_both(tab, excluded)
    field.clear()
    assert timer.isActive()
    qtbot.waitUntil(lambda: not timer.isActive(), timeout=TIMEOUT_MS)
    assert tab.model._filters[filter_key] is None
    _assert_both(tab, every)


def test_url_include_and_exclude_combine(qtbot, make_tab, images_case):
    data, _, ids = images_case
    tab = make_tab()
    tab.url_filter_input.setText("example.com")
    tab.url_exclude_input.setText("ads")
    for timer in _timers(tab):
        qtbot.waitUntil(lambda: not timer.isActive(), timeout=TIMEOUT_MS)
    # "multi" has a cdn URL and an ADS URL: hidden by the exclude
    expected = _expected(data, url_text="example.com", url_excludes="ads")
    assert expected == [ids["cdn"]]
    _assert_both(tab, expected)
    # Same text in include and exclude: nothing
    tab.url_exclude_input.setText("example.com")
    qtbot.waitUntil(lambda: not tab._url_exclude_timer.isActive(), timeout=TIMEOUT_MS)
    _assert_both(tab, [])


def test_reset_clears_excludes_untagged_size_and_stops_timers(qtbot, make_tab, images_case):
    data, _, _ = images_case
    tab = make_tab()
    every = _expected(data)

    tab.url_exclude_input.setText("ads")
    tab.cache_key_exclude_input.setText("cdn")
    for timer in _timers(tab):
        qtbot.waitUntil(lambda: not timer.isActive(), timeout=TIMEOUT_MS)
    _select(tab.tag_combo, tab_module._UNTAGGED_TAG)
    tab.size_combo.setCurrentIndex(1)  # "< 50 KB"
    expected = _expected(data, url_excludes="ads", cache_key_excludes="cdn", untagged=True, max_size_bytes=50 * 1024)
    assert len(expected) < len(every)
    _assert_both(tab, expected)

    # Pending debounced edits are dropped by Reset
    tab.url_filter_input.setText("pending")
    tab.cache_key_filter_input.setText("pending")
    tab.url_exclude_input.setText("pending")
    tab.cache_key_exclude_input.setText("pending")
    assert all(timer.isActive() for timer in _timers(tab))

    tab.reset_filters_button.click()

    assert not any(timer.isActive() for timer in _timers(tab))
    assert tab.url_exclude_input.text() == "" and tab.cache_key_exclude_input.text() == ""
    assert tab.url_filter_input.text() == "" and tab.cache_key_filter_input.text() == ""
    assert tab.tag_combo.currentIndex() == 0 and tab.tag_combo.currentData() == ""
    assert tab.size_combo.currentIndex() == 0
    assert tab.filters.untagged is False
    assert tab.filters.url_exclude_text == "" and tab.filters.cache_key_exclude_text == ""
    for model in (tab.model, tab.table_model):
        assert model._filters["untagged"] is False
        assert model._filters["url_exclude_text"] is None and model._filters["cache_key_exclude_text"] is None
        assert model._filters["min_size_bytes"] is None and model._filters["max_size_bytes"] is None
        assert model._filters["tags"] == "%"
    _assert_both(tab, every)


def test_size_all_sizes_resets_table(make_tab, images_case):
    """Regression: "All sizes" left the old size filter in the Table (issue #74)."""
    data, _, _ = images_case
    tab = make_tab()
    tab.size_combo.setCurrentIndex(1)  # "< 50 KB"
    small = _expected(data, max_size_bytes=50 * 1024)
    _assert_both(tab, small)
    tab.size_combo.setCurrentIndex(0)  # "All sizes"
    _assert_both(tab, _expected(data))


# ---------------------------------------------------------------------------
# Untagged entry
# ---------------------------------------------------------------------------


def test_untagged_entry_follows_all_tags_and_filters_both_views(make_tab, images_case):
    data, _, ids = images_case
    tab = make_tab()
    combo = tab.tag_combo
    assert combo.itemText(0) == "All tags" and combo.itemData(0) == ""
    assert combo.itemText(1) == "Untagged" and combo.itemData(1) is tab_module._UNTAGGED_TAG
    assert all(isinstance(combo.itemData(i), str) for i in range(2, combo.count()))

    combo.setCurrentIndex(1)
    assert tab.filters.untagged is True and tab.filters.tags == ""
    untagged = _expected(data, untagged=True)
    assert set(untagged) == set(ids.values()) - {ids["multi"], ids["named_untagged"], ids["relevant"]}
    _assert_both(tab, untagged)
    for model in (tab.model, tab.table_model):
        assert model._filters["untagged"] is True and model._filters["tags"] == "%"

    # Combines with the Exclude fields
    tab.url_exclude_input.setText("tracker")
    tab._url_exclude_timer.stop()
    tab._on_filters_changed()
    _assert_both(tab, _expected(data, untagged=True, url_excludes="tracker"))

    combo.setCurrentIndex(0)
    assert tab.filters.untagged is False
    _assert_both(tab, _expected(data, url_excludes="tracker"))


def test_tag_named_untagged_is_a_normal_tag(make_tab, images_case):
    data, _, ids = images_case
    tab = make_tab()
    combo = tab.tag_combo
    index = combo.findData("Untagged")
    assert index > 1 and combo.itemData(index) == "Untagged"
    assert combo.itemText(index).startswith("Untagged (")

    combo.setCurrentIndex(index)
    assert tab.filters.untagged is False and tab.filters.tags == "Untagged"
    for model in (tab.model, tab.table_model):
        assert model._filters["tags"] == "%Untagged%" and model._filters["untagged"] is False
    _assert_both(tab, [ids["named_untagged"]])

    # Switching back to the fixed entry is a real change
    _select(combo, tab_module._UNTAGGED_TAG)
    assert ids["named_untagged"] not in _grid_ids(tab)
    _assert_both(tab, _expected(data, untagged=True))


def test_untagged_selection_survives_filter_reload_and_load_tags(qtbot, make_tab, images_case):
    data, _, _ = images_case
    tab = make_tab()
    _select(tab.tag_combo, tab_module._UNTAGGED_TAG)
    untagged = _expected(data, untagged=True)
    _assert_both(tab, untagged)

    tab._populate_filters()
    _wait_filters(qtbot, tab)
    assert tab.tag_combo.currentIndex() == 1
    assert tab.tag_combo.currentData() is tab_module._UNTAGGED_TAG
    assert tab.filters.untagged is True
    _assert_both(tab, untagged)

    tab._load_tags()
    assert tab.tag_combo.currentData() is tab_module._UNTAGGED_TAG
    assert [tab.tag_combo.itemText(i) for i in range(2)] == ["All tags", "Untagged"]
    _assert_both(tab, untagged)

    # A real tag selection survives too
    _select(tab.tag_combo, "Untagged")
    tab._populate_filters()
    _wait_filters(qtbot, tab)
    assert tab.tag_combo.currentData() == "Untagged" and tab.tag_combo.currentIndex() > 1
    tab._load_tags()
    assert tab.tag_combo.currentData() == "Untagged" and tab.tag_combo.currentIndex() > 1


# ---------------------------------------------------------------------------
# Tagging with Untagged active
# ---------------------------------------------------------------------------

tagged: List[List[int]] = []


class _FakeTagDialog:
    """Tags the given images with "Reviewed" and accepts, like the real dialog."""

    def __init__(self, case_data, evidence_id, artifact_type, artifact_ids, parent=None):
        self.case_data = case_data
        self.evidence_id = evidence_id
        self.artifact_type = artifact_type
        self.artifact_ids = list(artifact_ids)
        self._slots = []
        self.tags_changed = self
        tagged.append(self.artifact_ids)

    def connect(self, slot):
        self._slots.append(slot)

    def exec(self):
        assert self.artifact_type == "image"
        for artifact_id in self.artifact_ids:
            self.case_data.tag_artifact(self.evidence_id, "Reviewed", self.artifact_type, artifact_id)
        for slot in self._slots:
            slot()
        return 1


@pytest.mark.parametrize("flow", ["selected", "checked"])
def test_tagging_with_untagged_active_drops_images_from_both_views(make_tab, images_case, monkeypatch, flow):
    data, _, _ = images_case
    tab = make_tab()
    monkeypatch.setattr(tab_module, "TagArtifactsDialog", _FakeTagDialog)
    tagged.clear()
    _select(tab.tag_combo, tab_module._UNTAGGED_TAG)
    before = _expected(data, untagged=True)
    _assert_both(tab, before)
    targets = before[:2]

    if flow == "selected":
        tab._tag_images(targets)
    else:
        tab._checked_image_ids.update(targets)
        tab._tag_checked()

    assert sorted(tagged[0]) == sorted(targets)
    after = _expected(data, untagged=True)
    assert after == [image_id for image_id in before if image_id not in targets]
    _assert_both(tab, after)
    # The new tag is listed and the Untagged selection is kept
    assert tab.tag_combo.currentData() is tab_module._UNTAGGED_TAG
    assert tab.tag_combo.findData("Reviewed") > 1
    assert tab.filters.untagged is True
