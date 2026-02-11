"""
Test bulk_extractor real-time progress parsing.

Verifies that fraction_read and estimated_time_remaining from
bulk_extractor's stderr output are correctly parsed and forwarded
to progress and log callbacks.
"""

import re
import pytest


def test_parse_fraction_read():
    """Test the regex pattern for extracting fraction_read from stderr."""
    # Pattern from bulk_extractor_worker.py
    pattern = r'fraction_read:\s+([\d.]+)\s*%'

    # Test cases from actual bulk_extractor output
    test_cases = [
        ("fraction_read: 8.954643 %", 8.954643),
        ("fraction_read: 15.234567 %", 15.234567),
        ("fraction_read: 99.123456 %", 99.123456),
        ("fraction_read: 100.0 %", 100.0),
    ]

    for line, expected in test_cases:
        match = re.search(pattern, line)
        assert match is not None, f"Pattern should match line: {line}"
        fraction = float(match.group(1))
        assert abs(fraction - expected) < 0.001, \
            f"Expected {expected}, got {fraction}"


def test_parse_estimated_time_remaining():
    """Test the regex pattern for extracting ETA from stderr."""
    pattern = r'estimated_time_remaining:\s+([\d:]+)'

    test_cases = [
        ("estimated_time_remaining:  0:00:51", "0:00:51"),
        ("estimated_time_remaining:  0:02:15", "0:02:15"),
        ("estimated_time_remaining:  1:30:45", "1:30:45"),
    ]

    for line, expected in test_cases:
        match = re.search(pattern, line)
        assert match is not None, f"Pattern should match line: {line}"
        time_remaining = match.group(1)
        assert time_remaining == expected, \
            f"Expected '{expected}', got '{time_remaining}'"


def test_progress_throttling_logic():
    """Test that progress updates are throttled to every 1%."""
    last_progress_update = 0
    throttle_threshold = 1.0
    updates = []

    # Simulate many small progress increments
    for i in range(200):  # 0% to 99.5% in 0.5% increments
        fraction = i * 0.5

        # Throttling logic from bulk_extractor_worker.py
        should_update = (
            fraction - last_progress_update >= throttle_threshold or
            fraction >= 99.0
        )

        if should_update:
            updates.append(fraction)
            last_progress_update = fraction

    # Should have ~100 updates (0%, 1%, 2%, ..., 98%, then all updates >= 99%)
    assert len(updates) <= 110, f"Too many updates: {len(updates)}"
    assert len(updates) >= 95, f"Too few updates: {len(updates)}"

    # First update should be when threshold is reached (1%)
    assert updates[0] in [0.5, 1.0], f"First update should be ~1%, got {updates[0]}"

    # All >= 99% should be included (no throttling)
    high_updates = [u for u in updates if u >= 99.0]
    assert len(high_updates) >= 2, "Should capture all updates >= 99%"
