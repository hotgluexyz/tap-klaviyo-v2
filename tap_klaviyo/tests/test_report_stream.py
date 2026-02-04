"""Tests for stream classes."""

import pytest

class TestReportStreamParseResponse:
    """Tests for ReportStream.parse_response method."""

    def test_parse_response_basic(self, report_stream, mock_response, load_report_fixture):
        """Test parsing a basic response with one data point."""
        response_data = load_report_fixture("basic_response")
        results = report_stream.parse_response(mock_response(response_data))

        assert len(results) == 2

        # First record (2024-01-01)
        assert results[0]["date"] == "2024-01-01T00:00:00.000000Z"
        assert results[0]["metric_id"] == "metric_123"
        assert results[0]["Campaign Name"] == "Welcome Campaign"
        assert results[0]["$message"] == "msg_abc123"
        assert results[0]["count"] == 100

        # Second record (2024-01-02)
        assert results[1]["date"] == "2024-01-02T00:00:00.000000Z"
        assert results[1]["count"] == 150

    def test_parse_response_multiple_data_points(
        self, report_stream, mock_response, load_report_fixture
    ):
        """Test parsing a response with multiple data points."""
        response_data = load_report_fixture("multiple_data_points")
        results = report_stream.parse_response(mock_response(response_data))

        assert len(results) == 2
        assert results[0]["Campaign Name"] == "Campaign A"
        assert results[0]["count"] == 50
        assert results[1]["Campaign Name"] == "Campaign B"
        assert results[1]["count"] == 75

    def test_parse_response_multiple_aggregations(
        self, create_report_stream, mock_response, load_report_fixture
    ):
        """Test parsing a response with multiple aggregation types."""
        stream = create_report_stream(aggregation_types="count,sum,unique")
        response_data = load_report_fixture("multiple_aggregations")
        results = stream.parse_response(mock_response(response_data))

        assert len(results) == 1
        assert results[0]["count"] == 100
        assert results[0]["sum"] == 5000.50
        assert results[0]["unique"] == 80

    # It will run the test 3 times with the 3 fixtures
    @pytest.mark.parametrize("fixture_key", [
        "empty_data",
        "missing_attributes",
        "missing_data_key",
    ])
    def test_parse_response_empty_cases(
        self, report_stream, mock_response, load_report_fixture, fixture_key
    ):
        """Test parsing responses with empty or missing data."""
        all_empty_responses = load_report_fixture("empty_responses")
        response_data = all_empty_responses[fixture_key]
        
        results = report_stream.parse_response(mock_response(response_data))
        assert len(results) == 0

    def test_parse_response_missing_dimension_values(
        self, create_report_stream, mock_response, load_report_fixture
    ):
        """Test that missing dimension values are set to None."""
        stream = create_report_stream(dimensions="dim1,dim2,dim3")
        response_data = load_report_fixture("missing_dimensions")
        results = stream.parse_response(mock_response(response_data))

        assert len(results) == 1
        assert results[0]["dim1"] == "value1"
        assert results[0]["dim2"] is None
        assert results[0]["dim3"] is None

    def test_parse_response_missing_measurement_values(
        self, create_report_stream, mock_response, load_report_fixture
    ):
        """Test that missing measurement values are set to None."""
        stream = create_report_stream(aggregation_types="count,sum")
        response_data = load_report_fixture("missing_measurements")
        results = stream.parse_response(mock_response(response_data))

        assert len(results) == 1
        assert results[0]["count"] == 100
        assert results[0]["sum"] is None
