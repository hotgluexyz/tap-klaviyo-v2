"""Tests for KlaviyoStream.get_paging_windows."""

import pendulum
import pytest

from tap_klaviyo.streams import ContactsStream


@pytest.fixture
def stream():
    s = object.__new__(ContactsStream)
    s._config = {
        "start_date": "2024-01-01T00:00:00Z",
        "end_date": "2024-01-02T00:00:00Z",
    }
    s.replication_key = "updated"
    s.parallelization_limit = 5
    s.min_paging_window_hours = 6
    s.get_starting_timestamp = lambda context: None
    return s


def test_get_paging_windows_returns_empty_without_replication_key(stream):
    stream.replication_key = None
    assert stream.get_paging_windows({}) == []


def test_get_paging_windows_returns_empty_when_parallelization_disabled(stream):
    stream.parallelization_limit = 1
    assert stream.get_paging_windows({}) == []


def test_get_paging_windows_returns_empty_when_no_start(stream):
    stream._config = {"end_date": "2024-01-02T00:00:00Z"}
    assert stream.get_paging_windows({}) == []


def test_get_paging_windows_returns_empty_when_start_not_before_end(stream):
    stream._config = {
        "start_date": "2024-01-02T00:00:00Z",
        "end_date": "2024-01-01T00:00:00Z",
    }
    assert stream.get_paging_windows({}) == []


def test_get_paging_windows_splits_range_into_contiguous_windows(stream):
    windows = stream.get_paging_windows({})

    assert len(windows) == 4
    assert windows[0]["window_start"] == pendulum.parse("2024-01-01T00:00:00Z")
    assert windows[-1]["window_end"] == pendulum.parse("2024-01-02T00:00:00Z")
    for i in range(len(windows) - 1):
        assert windows[i]["window_end"] == windows[i + 1]["window_start"]


def test_get_paging_windows_caps_window_count_by_parallelization_limit(stream):
    stream._config = {
        "start_date": "2024-01-01T00:00:00Z",
        "end_date": "2024-01-08T00:00:00Z",
    }
    stream.parallelization_limit = 3

    windows = stream.get_paging_windows({})

    assert len(windows) == 3
    assert windows[0]["window_start"] == pendulum.parse("2024-01-01T00:00:00Z")
    assert windows[-1]["window_end"] == pendulum.parse("2024-01-08T00:00:00Z")
