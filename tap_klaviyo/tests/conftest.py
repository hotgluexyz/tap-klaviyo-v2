"""Shared pytest fixtures for tap-klaviyo tests."""

import datetime
import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock

from tap_klaviyo.streams import ReportStream

FIXTURES_DIR = Path(__file__).parent / "fixtures"

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
}


@pytest.fixture
def sample_config():
    """Sample tap configuration."""
    return SAMPLE_CONFIG.copy()


@pytest.fixture
def mock_response():
    """Factory fixture for creating mock HTTP responses."""
    def _mock_response(json_data):
        resp = MagicMock()
        resp.json.return_value = json_data
        return resp
    return _mock_response


@pytest.fixture
def load_fixture():
    """Load JSON fixture files.
    
    Usage:
        load_fixture("streams/report/basic_response")
        load_fixture("common/empty_responses")
    """
    def _load(name: str):
        if not name.endswith(".json"):
            name = f"{name}.json"
        
        fixture_path = FIXTURES_DIR / name
        
        if not fixture_path.exists():
            raise FileNotFoundError(f"Fixture not found: {fixture_path}")
        
        with open(fixture_path) as f:
            return json.load(f)
    return _load

@pytest.fixture
def load_report_fixture(load_fixture):
    """Load fixtures from streams/report/ directory."""
    def _load(name: str):
        return load_fixture(f"streams/report/{name}")
    return _load


@pytest.fixture
def create_report_stream():
    """Factory for creating ReportStream instances with custom parameters."""
    def _create(
        dimensions: str = "Campaign Name,$message",
        aggregation_types: str = "count",
        metric_id: str = "metric_123"
    ):
        stream = ReportStream.__new__(ReportStream)
        stream.report_config = {
            "name": "test_report",
            "metric_id": metric_id,
            "dimensions": dimensions,
            "aggregation_types": aggregation_types,
            "interval": "day",
        }
        stream.name = "test_report"
        stream.metric_id = metric_id
        stream.dimensions = [d.strip() for d in dimensions.split(",")]
        stream.aggregation_types = [m.strip() for m in aggregation_types.split(",")]
        stream.interval = "day"
        return stream
    return _create


@pytest.fixture
def report_stream(create_report_stream):
    """Default ReportStream instance."""
    return create_report_stream()
