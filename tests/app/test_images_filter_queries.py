"""Images data layer: URL / Cache Key Exclude, literal matching and Untagged (issue #74).

``iter_images`` results are checked against an independent Python oracle built
from the fixture data as it is inserted (never from the SQL). The Grid and
Table models are checked to agree with each other and with the oracle.
"""
from __future__ import annotations

import itertools
import string
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pytest

from app.data import CaseDataAccess, discard_case_stats_cache
from app.features.images.models import ImagesListModel, ImagesTableModel
from core.database import DatabaseManager, init_db
from core.enums import BROWSER_IMAGE_SOURCES

EV = 1
OTHER_EV = 2
SIZE_N = 2000
BROWSER = tuple(sorted(BROWSER_IMAGE_SOURCES))
_FOLD = str.maketrans(string.ascii_uppercase, string.ascii_lowercase)

# key -> (filename, size_bytes, discoveries [(discovered_by, cache_url, cache_key)], hash lists)
IMAGES: List[Tuple[str, str, Optional[int], List[Tuple[str, Optional[str], Optional[str]]], List[str]]] = [
    ("pct", "a.jpg", 1000,
     [("cache_simple", "https://cdn.example.com/100%_off.jpg", "Key_A%1")], ["known_bad"]),
    # Unescaped, "100%_off" / "Key_A%1" would wildcard-match these
    ("pct_decoy", "b.JPG", 2000,
     [("cache_simple", "https://cdn.example.com/100xxoff.jpg", "KeyxAz1")], []),
    ("underscore", "c.png", None,
     [("cache_firefox", "http://Site.Example/a_b/IMG", "a_b")], []),
    ("underscore_decoy", "d.gif", 50,
     [("cache_blockfile", "http://site.example/axb/img", "axb")], []),
    ("backslash", "e.jpeg", 300_000,
     [("browser_storage_indexeddb", "file:///C:\\Users\\Bob\\pic.jpg", "C:\\Cache\\K1")], []),
    ("backslash_decoy", "f.jpg", 400,
     [("safari", "file:///C:/Users/Bob/pic.jpg", "C:/Cache/K1")], ["other_list"]),
    # Several discoveries: one matching "tracker", one without URL/key, one other
    ("multi", "g.jpg", 5000,
     [("cache_simple", "https://ads.tracker.com/pixel.gif", "ads-key"),
      ("filesystem_images", None, None),
      ("cache_firefox", "https://news.example.org/photo.jpg", "news-key")], ["known_bad", "other_list"]),
    ("multi_upper", "h.webp", 7000,
     [("filesystem_images", None, None),
      ("cache_simple", "HTTPS://ADS.TRACKER.COM/BANNER", None)], []),
    ("no_discoveries", "i.jpg", 10, [], []),
    ("fs_only", "j.bmp", 600_000, [("filesystem_images", None, None)], []),
    ("carved", "k.jpg", None, [("foremost_carver", None, None)], ["known_bad"]),
    # Only ASCII letters ignore case
    ("non_ascii", "l.JPG", 900,
     [("cache_simple", "https://exämple.com/ÄRGER", "ärgerKey")], []),
    ("mixed_case", "m.jpg", 1500,
     [("cache_blockfile", "https://Images.GoogleUserContent.com/X", "GUC_Key")], []),
    ("url_no_key", "n.jpg", 2500, [("cache_simple", "https://cdn.example.com/n.jpg", None)], []),
    ("key_no_url", "o.png", 3500, [("cache_simple", None, "keyonly/o")], []),
    ("foreign_tag", "p.jpg", 100, [("cache_simple", "https://p.example/p.jpg", "p-key")], []),
    ("deleted_tag", "q.jpg", 200, [("cache_firefox", "https://q.example/q.jpg", "q-key")], []),
    ("foreign_assoc", "r.gif", 300, [("filesystem_images", None, None)], []),
    ("tag_untagged", "s.jpg", 400, [("cache_simple", "https://s.example/ads/s.jpg", "s-key")], []),
    ("tag_ads", "t.jpg", 500, [("safari", "https://t.example/t.jpg", "t-key")], []),
    ("tag_badsite", "u.png", 600, [("cache_simple", "https://u.example/u.png", "u-key")], []),
    ("url_type_tag", "v.jpg", 700, [("foremost_carver", None, None)], []),
    ("double_ext", "w.jpg.png", 800, [("cache_simple", "https://w.example/w.jpg.png", "w-key")], []),
    ("relevant", "x.JPEG", 2000,
     [("cache_simple", "https://x.example/x", "x_key"), ("cache_simple", "https://x.example/y", "y%key")], []),
]
NULL_TS = "no_discoveries"

# tag key -> (tags.evidence_id, name)
TAGS = {
    "ads": (EV, "ads"),
    "badsite": (EV, "badsite"),
    "untagged_name": (EV, "Untagged"),
    "relevant": (EV, "Relevant"),
    "relevant_other": (OTHER_EV, "Relevant"),
    "foreign": (OTHER_EV, "Foreign"),
    "gone": (EV, "Gone"),
}
DELETED_TAGS = {"gone"}
# (tag key, tag_associations.evidence_id, artifact_type, image key)
ASSOCIATIONS = [
    ("ads", EV, "image", "tag_ads"),
    ("ads", EV, "image", "multi"),
    ("ads", EV, "url", "url_type_tag"),  # a URL with this image's id is tagged
    ("badsite", EV, "image", "tag_badsite"),
    ("untagged_name", EV, "image", "tag_untagged"),
    ("relevant", EV, "image", "relevant"),
    ("relevant", EV, "image", "pct"),
    ("relevant", EV, "image", "mixed_case"),
    ("relevant", OTHER_EV, "image", "foreign_assoc"),  # association of another evidence
    ("foreign", EV, "image", "foreign_tag"),  # tag row of another evidence
    ("relevant_other", OTHER_EV, "image", "foreign_tag"),
    ("gone", EV, "image", "deleted_tag"),  # tag row deleted below
]
TAGGED_HERE = {"tag_ads", "multi", "tag_badsite", "tag_untagged", "relevant", "pct", "mixed_case"}


def _fold(value: str) -> str:
    return value.translate(_FOLD)


def _contains(value: Optional[str], term: str) -> bool:
    return value is not None and _fold(term) in _fold(value)


@dataclass
class Oracle:
    """Python reimplementation of the ``iter_images`` filters."""

    rows: List[Dict[str, Any]] = field(default_factory=list)
    discoveries: Dict[int, List[Tuple[str, Optional[str], Optional[str]]]] = field(default_factory=dict)
    hash_lists: Dict[int, Set[str]] = field(default_factory=dict)
    tag_names: Dict[int, Set[str]] = field(default_factory=dict)  # tag filter: no evidence check
    tagged: Set[int] = field(default_factory=set)  # Tags column / Untagged definition

    def select(
        self,
        *,
        tag: Optional[str] = None,
        untagged: bool = False,
        discovered_by: Optional[Sequence[str]] = None,
        extension: Optional[str] = None,
        hash_match: Optional[str] = None,
        url_text: Optional[str] = None,
        cache_key_text: Optional[str] = None,
        url_excludes: Optional[str] = None,
        cache_key_excludes: Optional[str] = None,
        min_size_bytes: Optional[int] = None,
        max_size_bytes: Optional[int] = None,
    ) -> List[int]:
        def any_discovery(image_id: int, index: int, term: str) -> bool:
            return any(_contains(d[index], term) for d in self.discoveries.get(image_id, []))

        def keep(row: Dict[str, Any]) -> bool:
            image_id = row["id"]
            if tag and not any(_fold(tag) in _fold(name) for name in self.tag_names.get(image_id, set())):
                return False
            if untagged and image_id in self.tagged:
                return False
            if discovered_by and not any(d[0] in discovered_by for d in self.discoveries.get(image_id, [])):
                return False
            if extension and not _fold(row["filename"]).endswith("." + _fold(extension)):
                return False
            lists = self.hash_lists.get(image_id, set())
            if hash_match == "__any__" and not lists:
                return False
            if hash_match and hash_match != "__any__" and hash_match not in lists:
                return False
            size = row["size_bytes"]
            if min_size_bytes is not None and size is not None and size < min_size_bytes:
                return False
            if max_size_bytes is not None and size is not None and size > max_size_bytes:
                return False
            if url_text and not any_discovery(image_id, 1, url_text):
                return False
            if cache_key_text and not any_discovery(image_id, 2, cache_key_text):
                return False
            if url_excludes and any_discovery(image_id, 1, url_excludes):
                return False
            if cache_key_excludes and any_discovery(image_id, 2, cache_key_excludes):
                return False
            return True

        selected = [row for row in self.rows if keep(row)]
        selected.sort(key=lambda row: row["ts_utc"] or "", reverse=True)
        return [row["id"] for row in selected]


@dataclass
class ImagesCase:
    data: CaseDataAccess
    ids: Dict[str, int]
    oracle: Oracle

    @property
    def all_ids(self) -> List[int]:
        return self.oracle.select()

    def query(self, *, tag: Optional[str] = None, **kwargs: Any) -> List[int]:
        """``iter_images`` as the models call it (tag wrapped as ``%tag%``)."""
        rows = self.data.iter_images(EV, tag_like=f"%{tag}%" if tag else "%", limit=10_000, **kwargs)
        return [row["id"] for row in rows]

    def keys(self, ids: Iterable[int]) -> Set[str]:
        by_id = {image_id: key for key, image_id in self.ids.items()}
        return {by_id[image_id] for image_id in ids}


@pytest.fixture
def images_case(tmp_path: Path):
    case_dir = tmp_path / "case"
    case_dir.mkdir()
    case_db_path = case_dir / "IMG74_surfsifter.sqlite"
    case_conn = init_db(case_dir, db_path=case_db_path)
    with case_conn:
        case_conn.execute(
            "INSERT INTO cases (case_id, title, created_at_utc) VALUES (?, ?, ?)",
            ("CASE_74", "Images #74", "2025-01-01T00:00:00Z"),
        )
        case_conn.execute(
            "INSERT INTO evidences (case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
            (1, "EV-74", "/tmp/test.E01", "2025-01-01T00:00:00Z"),
        )
    case_conn.close()
    discard_case_stats_cache(case_db_path)

    manager = DatabaseManager(case_dir, case_db_path=case_db_path, enable_split=True)
    conn = manager.get_evidence_conn(EV, label="EV-74")
    oracle = Oracle()
    ids: Dict[str, int] = {}
    tag_ids: Dict[str, int] = {}
    with conn:
        for index, (key, filename, size, discoveries, hash_lists) in enumerate(IMAGES):
            ts = None if key == NULL_TS else f"2025-01-{index + 1:02d}T00:00:00Z"
            image_id = conn.execute(
                "INSERT INTO images (evidence_id, rel_path, filename, md5, sha256, first_discovered_by, ts_utc, "
                "size_bytes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (EV, f"images/{filename}", filename, f"{index:032x}", f"{index:064x}",
                 discoveries[0][0] if discoveries else "filesystem_images", ts, size),
            ).lastrowid
            ids[key] = image_id
            oracle.rows.append({"id": image_id, "filename": filename, "size_bytes": size, "ts_utc": ts})
            for source, url, cache_key in discoveries:
                conn.execute(
                    "INSERT INTO image_discoveries (evidence_id, image_id, discovered_by, run_id, cache_url, cache_key) "
                    "VALUES (?, ?, ?, 'run-1', ?, ?)",
                    (EV, image_id, source, url, cache_key),
                )
                oracle.discoveries.setdefault(image_id, []).append((source, url, cache_key))
            for list_name in hash_lists:
                conn.execute(
                    "INSERT INTO hash_matches (evidence_id, image_id, db_name, db_md5, list_name) VALUES (?, ?, ?, ?, ?)",
                    (EV, image_id, f"{list_name}.txt", "0" * 32, list_name),
                )
                oracle.hash_lists.setdefault(image_id, set()).add(list_name)

        # Rows of another evidence in the same DB never count for evidence 1:
        # an image, a discovery and a hash match pointing at evidence-1 images.
        ghost = conn.execute(
            "INSERT INTO images (evidence_id, rel_path, filename, first_discovered_by, ts_utc, size_bytes) "
            "VALUES (?, 'images/ghost.jpg', 'ghost.jpg', 'cache_simple', '2030-01-01T00:00:00Z', 10)",
            (OTHER_EV,),
        ).lastrowid
        conn.execute(
            "INSERT INTO image_discoveries (evidence_id, image_id, discovered_by, run_id, cache_url, cache_key) "
            "VALUES (?, ?, 'cache_simple', 'run-2', 'https://ghost.example/g.jpg', 'ghost-key')",
            (OTHER_EV, ghost),
        )
        conn.execute(
            "INSERT INTO image_discoveries (evidence_id, image_id, discovered_by, run_id, cache_url, cache_key) "
            "VALUES (?, ?, 'cache_simple', 'run-2', 'https://ghost.example/fs.jpg', 'ghost-key')",
            (OTHER_EV, ids["fs_only"]),
        )
        conn.execute(
            "INSERT INTO hash_matches (evidence_id, image_id, db_name, db_md5, list_name) "
            "VALUES (?, ?, 'known_bad.txt', ?, 'known_bad')",
            (OTHER_EV, ids["no_discoveries"], "0" * 32),
        )

        for tag_key, (tag_ev, name) in TAGS.items():
            tag_ids[tag_key] = conn.execute(
                "INSERT INTO tags (evidence_id, name, name_normalized) VALUES (?, ?, ?)",
                (tag_ev, name, name.lower()),
            ).lastrowid
        for tag_key, ta_ev, artifact_type, image_key in ASSOCIATIONS:
            conn.execute(
                "INSERT INTO tag_associations (tag_id, evidence_id, artifact_type, artifact_id) VALUES (?, ?, ?, ?)",
                (tag_ids[tag_key], ta_ev, artifact_type, ids[image_key]),
            )
            tag_ev, name = TAGS[tag_key]
            if artifact_type != "image" or tag_key in DELETED_TAGS:
                continue
            oracle.tag_names.setdefault(ids[image_key], set()).add(name)
            if ta_ev == EV and tag_ev == EV:
                oracle.tagged.add(ids[image_key])
    # Associations to a deleted tag (PRAGMA foreign_keys is a no-op inside a
    # transaction, so it is set outside one).
    conn.execute("PRAGMA foreign_keys = OFF")
    with conn:
        for tag_key in DELETED_TAGS:
            conn.execute("DELETE FROM tags WHERE id = ?", (tag_ids[tag_key],))
    conn.execute("PRAGMA foreign_keys = ON")
    assert conn.execute(
        "SELECT COUNT(*) FROM tag_associations WHERE tag_id = ?", (tag_ids["gone"],)
    ).fetchone()[0] == 1
    manager.close_all()

    data = CaseDataAccess(case_dir, case_db_path)
    assert oracle.tagged == {ids[key] for key in TAGGED_HERE}  # the oracle's own bookkeeping
    yield ImagesCase(data, ids, oracle)
    data.close()
    discard_case_stats_cache(case_db_path)


# ---------------------------------------------------------------------------
# Literal matching and ASCII case, all four text fields
# ---------------------------------------------------------------------------

TEXT_FIELDS = ["url_text", "cache_key_text", "url_excludes", "cache_key_excludes"]
TERMS = [
    "%", "_", "\\", "100%_off", "Key_A%1", "a_b", "A_B", "\\Users\\Bob", "c:\\cache", "y%key", "x_key",
    "TRACKER", "tracker", "googleusercontent", "ärger", "ÄRGER", "ÄrgerKey", "exÄmple", "example", "ghost",
    "-key", "nothing-matches",
]


@pytest.mark.parametrize("field_name", TEXT_FIELDS)
def test_text_fields_against_oracle(images_case, field_name):
    for term in TERMS:
        assert images_case.query(**{field_name: term}) == images_case.oracle.select(**{field_name: term}), term


def test_percent_underscore_backslash_are_literal(images_case):
    c = images_case
    assert c.keys(c.query(url_text="100%_off")) == {"pct"}
    assert c.keys(c.query(cache_key_text="key_a%1")) == {"pct"}
    assert c.keys(c.query(url_text="a_b")) == {"underscore"}
    assert c.keys(c.query(cache_key_text="a_b")) == {"underscore"}
    assert c.keys(c.query(url_text="\\Users\\")) == {"backslash"}
    assert c.keys(c.query(cache_key_text="C:\\Cache")) == {"backslash"}
    assert c.keys(c.query(cache_key_text="%")) == {"pct", "relevant"}
    assert c.keys(c.query(url_text="%")) == {"pct"}
    # Exclude: the decoys an unescaped pattern would also hide stay visible
    shown = c.keys(c.query(url_excludes="100%_off"))
    assert "pct" not in shown and "pct_decoy" in shown
    shown = c.keys(c.query(cache_key_excludes="a_b"))
    assert "underscore" not in shown and "underscore_decoy" in shown
    shown = c.keys(c.query(url_excludes="\\"))
    assert "backslash" not in shown and "backslash_decoy" in shown


def test_matching_ignores_ascii_case_only(images_case):
    c = images_case
    assert c.keys(c.query(url_text="googleusercontent")) == {"mixed_case"}
    assert c.keys(c.query(cache_key_text="guc_KEY")) == {"mixed_case"}
    assert "multi_upper" in c.keys(c.query(url_text="tracker"))
    # "ÄRGER" is stored upper case: the lower-case non-ASCII text does not match it
    assert c.keys(c.query(url_text="ÄRGER")) == {"non_ascii"}
    assert c.query(url_text="ärger") == []
    assert c.keys(c.query(cache_key_text="ärgerkey")) == {"non_ascii"}
    assert c.query(cache_key_text="ÄRGERKEY") == []
    assert "non_ascii" in c.keys(c.query(url_excludes="ärger"))


# ---------------------------------------------------------------------------
# Several discoveries, missing URL / cache key, complement
# ---------------------------------------------------------------------------


def test_several_discoveries_any_match_includes_and_hides(images_case):
    c = images_case
    # "multi" has one discovery with "tracker", one without URL, one without
    assert c.keys(c.query(url_text="tracker")) == {"multi", "multi_upper"}
    assert c.keys(c.query(url_text="news.example")) == {"multi"}
    hidden = set(c.all_ids) - set(c.query(url_excludes="news.example"))
    assert c.keys(hidden) == {"multi"}
    hidden = set(c.all_ids) - set(c.query(cache_key_excludes="news-key"))
    assert c.keys(hidden) == {"multi"}
    # Each of "relevant"'s two discoveries matches one term
    assert "relevant" not in c.keys(c.query(cache_key_excludes="y%key"))
    assert "relevant" not in c.keys(c.query(cache_key_excludes="x_key"))
    assert "relevant" in c.keys(c.query(cache_key_text="x_key", cache_key_excludes="nothing"))


def test_exclude_keeps_images_without_url_or_cache_key(images_case):
    c = images_case
    no_url = {"no_discoveries", "fs_only", "carved", "foreign_assoc", "url_type_tag", "key_no_url"}
    no_key = {"no_discoveries", "fs_only", "carved", "foreign_assoc", "url_type_tag", "url_no_key"}
    # Every URL contains ":/"
    assert c.keys(c.query(url_excludes=":/")) == no_url
    assert c.keys(c.query(url_text=":/")) == set(c.ids) - no_url
    # Cache keys: "multi_upper" has a URL but no cache key at all
    assert c.keys(c.query(cache_key_excludes="k")) == no_key | {"multi_upper", "underscore", "underscore_decoy"}
    # Another evidence's discovery of fs_only does not count
    assert c.query(url_text="ghost") == [] and c.query(url_excludes="ghost") == c.all_ids


@pytest.mark.parametrize("include, exclude", [("url_text", "url_excludes"), ("cache_key_text", "cache_key_excludes")])
def test_include_and_exclude_are_exact_complements(images_case, include, exclude):
    c = images_case
    for term in ["tracker", "example", "%", "_", "\\", "key", "a_b", "ÄRGER", "ghost", "zzz", "https://"]:
        included = set(c.query(**{include: term}))
        excluded = set(c.query(**{exclude: term}))
        assert not included & excluded, term
        assert included | excluded == set(c.all_ids), term
        assert c.query(**{include: term, exclude: term}) == [], term


# ---------------------------------------------------------------------------
# Untagged and the (unchanged) tag filter
# ---------------------------------------------------------------------------


def test_untagged_edge_cases(images_case):
    c = images_case
    untagged = c.keys(c.query(untagged=True))
    assert untagged == set(c.ids) - TAGGED_HERE
    # Tagged only with another evidence's tag, via another evidence's
    # association, via a deleted tag, or a URL with the same id: all untagged.
    assert {"foreign_tag", "foreign_assoc", "deleted_tag", "url_type_tag"} <= untagged
    # A real tag named "Untagged" is a tag
    assert "tag_untagged" not in untagged
    assert c.query(untagged=True) == c.oracle.select(untagged=True)


def test_untagged_is_exactly_the_empty_tags_column(images_case):
    c = images_case
    untagged = set(c.query(untagged=True))
    for image_id in c.all_ids:
        assert (c.data.get_artifact_tags_str(EV, "image", image_id) == "") == (image_id in untagged)


def test_tag_filter_is_unchanged_substring_without_evidence_check(images_case):
    c = images_case
    assert c.keys(c.query(tag="ads")) == {"tag_ads", "multi", "tag_badsite"}  # "badsite" contains "ads"
    assert c.keys(c.query(tag="Untagged")) == {"tag_untagged"}
    assert c.keys(c.query(tag="untagged")) == {"tag_untagged"}
    # No evidence check: another evidence's tag or association still matches
    assert c.keys(c.query(tag="Relevant")) == {"relevant", "pct", "mixed_case", "foreign_assoc", "foreign_tag"}
    assert c.keys(c.query(tag="Foreign")) == {"foreign_tag"}
    assert c.query(tag="Gone") == []
    # Untagged combined with a tag: only images whose matching tag does not
    # count as theirs (documented; the tab never combines the two)
    assert c.keys(c.query(tag="Relevant", untagged=True)) == {"foreign_assoc", "foreign_tag"}
    assert c.query(tag="ads", untagged=True) == []


# ---------------------------------------------------------------------------
# Every combination
# ---------------------------------------------------------------------------

TEXT_SCENARIOS: List[Dict[str, str]] = [
    {},
    {"url_text": "example"},
    {"url_excludes": "tracker"},
    {"cache_key_text": "key"},
    {"cache_key_excludes": "_"},
    {"url_text": "https", "url_excludes": "ads"},
    {"cache_key_text": "key", "cache_key_excludes": "%"},
    {"url_text": "EXAMPLE", "url_excludes": "cdn", "cache_key_text": "-key", "cache_key_excludes": "news"},
]


def test_every_filter_combination_matches_oracle(images_case):
    c = images_case
    checked = 0
    for tag, untagged, sources, extension, (size_min, size_max), hash_match, texts in itertools.product(
        [None, "ads", "Untagged"],
        [False, True],
        [None, ("cache_simple",), BROWSER],
        [None, "jpg"],
        [(None, None), (None, SIZE_N), (SIZE_N, None)],
        [None, "__any__", "known_bad"],
        TEXT_SCENARIOS,
    ):
        kwargs = dict(
            untagged=untagged,
            discovered_by=sources,
            extension=extension,
            hash_match=hash_match,
            min_size_bytes=size_min,
            max_size_bytes=size_max,
            **texts,
        )
        expected = c.oracle.select(tag=tag, **kwargs)
        assert c.query(tag=tag, **kwargs) == expected, (tag, kwargs)
        checked += bool(expected)
    assert checked > 150  # the fixture exercises non-empty results


def test_extension_and_size_edges(images_case):
    c = images_case
    jpg = c.keys(c.query(extension="jpg"))
    assert {"pct_decoy", "non_ascii"} <= jpg  # upper-case filenames
    assert not {"double_ext", "backslash", "relevant"} & jpg  # w.jpg.png, .jpeg
    # NULL size passes both bounds
    assert {"underscore", "carved"} <= c.keys(c.query(min_size_bytes=10**9))
    assert {"underscore", "carved"} <= c.keys(c.query(max_size_bytes=0))
    # Bounds are inclusive
    assert {"pct_decoy", "relevant"} <= c.keys(c.query(min_size_bytes=SIZE_N, max_size_bytes=SIZE_N))


# ---------------------------------------------------------------------------
# Grid and Table models
# ---------------------------------------------------------------------------

_CLEARED = dict(
    tags="",
    sources=(),
    extension="",
    hash_match="",
    url_text="",
    cache_key_text="",
    url_exclude_text="",
    cache_key_exclude_text="",
    untagged=False,
    min_size_bytes=None,
    max_size_bytes=None,
)

MODEL_FILTERS: List[Dict[str, Any]] = [
    {},
    {"url_exclude_text": "tracker"},
    {"cache_key_exclude_text": "_"},
    {"url_text": "100%_off"},
    {"cache_key_text": "a_b"},
    {"untagged": True},
    {"untagged": True, "url_exclude_text": "ads", "sources": BROWSER},
    {"tags": "ads", "cache_key_exclude_text": "-key"},
    {"tags": "Untagged"},
    {"tags": "Relevant", "url_text": "example", "url_exclude_text": "cdn", "min_size_bytes": None, "max_size_bytes": SIZE_N},
    {"sources": ("cache_simple",), "extension": "jpg", "hash_match": "__any__", "untagged": True},
    {"hash_match": "known_bad", "cache_key_text": "key", "cache_key_exclude_text": "%", "min_size_bytes": SIZE_N},
]


def _oracle_for_model(c: ImagesCase, kwargs: Dict[str, Any]) -> List[int]:
    return c.oracle.select(
        tag=kwargs["tags"] or None,
        untagged=kwargs["untagged"],
        discovered_by=kwargs["sources"] or None,
        extension=kwargs["extension"] or None,
        hash_match=kwargs["hash_match"] or None,
        url_text=kwargs["url_text"] or None,
        cache_key_text=kwargs["cache_key_text"] or None,
        url_excludes=kwargs["url_exclude_text"] or None,
        cache_key_excludes=kwargs["cache_key_exclude_text"] or None,
        min_size_bytes=kwargs["min_size_bytes"],
        max_size_bytes=kwargs["max_size_bytes"],
    )


def _models(c: ImagesCase) -> Tuple[ImagesListModel, ImagesTableModel]:
    grid = ImagesListModel(c.data, page_size=10_000)
    table = ImagesTableModel(c.data, page_size=10_000)
    grid.set_evidence(EV)
    table.set_evidence(EV)
    return grid, table


def _ids(model) -> List[int]:
    return [row["id"] for row in model._rows]


def test_grid_and_table_models_agree(images_case):
    c = images_case
    grid, table = _models(c)
    assert _ids(grid) == _ids(table) == c.all_ids
    # The same models are reused, as in the tab: every call passes every value
    for filters in MODEL_FILTERS + list(reversed(MODEL_FILTERS)):
        kwargs = {**_CLEARED, **filters}
        grid.set_filters(**kwargs)
        table.set_filters(**kwargs)
        expected = _oracle_for_model(c, kwargs)
        assert _ids(grid) == _ids(table) == expected, filters


def test_table_size_filter_resets_like_grid(images_case):
    """Regression: Table kept the old size filter on (None, None) (issue #74)."""
    c = images_case
    grid, table = _models(c)
    for model in (grid, table):
        model.set_filters(**{**_CLEARED, "max_size_bytes": SIZE_N})
    small = _ids(table)
    assert small == _ids(grid) == c.oracle.select(max_size_bytes=SIZE_N)
    assert len(small) < len(c.all_ids)
    for model in (grid, table):
        model.set_filters(**{**_CLEARED, "min_size_bytes": None, "max_size_bytes": None})
    assert _ids(table) == _ids(grid) == c.all_ids
    assert table._filters["min_size_bytes"] is None and table._filters["max_size_bytes"] is None
    # Also when only the size is passed
    table.set_filters(min_size_bytes=SIZE_N)
    assert _ids(table) == c.oracle.select(min_size_bytes=SIZE_N)
    table.set_filters()
    assert _ids(table) == c.all_ids


@pytest.mark.parametrize("model_cls", [ImagesListModel, ImagesTableModel])
def test_untagged_is_passed_apart_from_tag_wrapping(images_case, model_cls):
    c = images_case
    model = model_cls(c.data, page_size=10_000)
    model.set_evidence(EV)
    untagged = c.oracle.select(untagged=True)

    model.set_filters(untagged=True, tags="")
    assert model._filters["untagged"] is True and model._filters["tags"] == "%"
    assert _ids(model) == untagged

    # None leaves untagged unchanged; with a tag nothing is selected
    model.set_filters(tags="ads")
    assert model._filters["untagged"] is True and model._filters["tags"] == "%ads%"
    assert _ids(model) == []

    model.set_filters(untagged=False, tags="ads")
    assert model._filters["untagged"] is False and model._filters["tags"] == "%ads%"
    assert _ids(model) == c.oracle.select(tag="ads")

    # A tag named "Untagged" goes through the wrapping like any tag
    model.set_filters(tags="Untagged")
    assert model._filters["tags"] == "%Untagged%" and not model._filters["untagged"]
    assert c.keys(_ids(model)) == {"tag_untagged"}

    model.set_filters(tags="", untagged=True, url_exclude_text="", cache_key_exclude_text="")
    assert _ids(model) == untagged
    model.set_filters(untagged=False)
    assert _ids(model) == c.all_ids


@pytest.mark.parametrize("model_cls", [ImagesListModel, ImagesTableModel])
def test_exclude_text_none_is_unchanged_and_empty_clears(images_case, model_cls):
    c = images_case
    model = model_cls(c.data, page_size=10_000)
    model.set_evidence(EV)
    model.set_filters(url_exclude_text="tracker", cache_key_exclude_text="_")
    expected = c.oracle.select(url_excludes="tracker", cache_key_excludes="_")
    assert _ids(model) == expected
    model.set_filters(url_text=None)
    assert model._filters["url_exclude_text"] == "tracker" and model._filters["cache_key_exclude_text"] == "_"
    assert _ids(model) == expected
    model.set_filters(url_exclude_text="")
    assert model._filters["url_exclude_text"] is None
    assert _ids(model) == c.oracle.select(cache_key_excludes="_")
    model.set_filters(cache_key_exclude_text="")
    assert _ids(model) == c.all_ids
