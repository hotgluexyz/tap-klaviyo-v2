"""Tests for Klaviyo API error handling."""

import pytest
from hotglue_singer_sdk.exceptions import FatalAPIError
from hotglue_singer_sdk.tap_base import InvalidCredentialsError
from requests import Response

from tap_klaviyo.streams import ContactsStream


def _response(status_code: int) -> Response:
    response = Response()
    response.status_code = status_code
    response.reason = "Forbidden" if status_code == 403 else "Not Found"
    response.url = "https://a.klaviyo.com/api/profiles"
    return response


def test_forbidden_profiles_response_is_invalid_credentials_error():
    """Klaviyo rejects invalid API keys with 403 instead of 401."""
    stream = object.__new__(ContactsStream)

    with pytest.raises(InvalidCredentialsError, match="403 Client Error: Forbidden"):
        stream.validate_response(_response(403))


def test_other_client_errors_remain_fatal_api_errors():
    """Only authentication status codes get the credential error classification."""
    stream = object.__new__(ContactsStream)

    with pytest.raises(FatalAPIError, match="404 Client Error: Not Found"):
        stream.validate_response(_response(404))
