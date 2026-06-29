"""Tests for dynamic schema inference in KlaviyoStream.

These guard the fix for HGI-10614 (be permissive for unpredictable nested
fields that can arrive as booleans) without reintroducing HGI-10622 (the SDK
record conformer silently coercing top-level string fields into booleans
whenever their schema lists "boolean").
"""

import pytest

from hotglue_singer_sdk.helpers._typing import (
    conform_record_data_types,
    is_boolean_type,
)

from tap_klaviyo.client import KlaviyoStream
from tap_klaviyo.streams import ContactsStream


@pytest.fixture
def stream() -> KlaviyoStream:
    """A bare KlaviyoStream instance for exercising pure inference helpers."""
    return object.__new__(ContactsStream)


def _prop_schema(stream: KlaviyoStream, name: str, value):
    """Return the emitted JSON-schema fragment for a single inferred property."""
    return stream._infer_property_type(name, value).to_dict()[name]


def test_top_level_all_null_field_stays_string_only(stream):
    """Top-level usually-null fields must NOT gain 'boolean' (HGI-10622)."""
    schema = _prop_schema(stream, "title", None)
    assert "boolean" not in schema["type"]
    assert set(schema["type"]) == {"string", "null"}
    # The SDK conformer must not treat it as boolean, otherwise "" -> True.
    assert is_boolean_type(schema) is False


def test_top_level_string_field_is_not_coerced(stream):
    """Real string values survive conforming instead of becoming True (HGI-10622)."""
    title_schema = _prop_schema(stream, "title", None)
    record_schema = {"properties": {"title": title_schema}}
    for value, expected in [("", ""), ("CEO", "CEO"), (None, None)]:
        out = conform_record_data_types("contacts", {"title": value}, record_schema, stream_logger())
        assert out["title"] == expected


def test_nested_boolean_field_is_tolerated(stream):
    """A nested field that is null in the sample but bool at sync time validates (HGI-10614)."""
    # Parent object present in the sample, but its nested flag is null here.
    sample_value = {"send_options": {"send_past_recipients_immediately": None}}
    schema = _prop_schema(stream, "send_config", sample_value)
    nested = schema["properties"]["send_options"]["properties"][
        "send_past_recipients_immediately"
    ]
    assert "boolean" in nested["type"]
    assert "string" in nested["type"]


def test_unknown_fallback_includes_boolean(stream):
    """The generic fallback used for nested/empty/unknown values includes boolean."""
    fallback = stream._unknown_jsonschema_type().to_dict()
    assert set(fallback["type"]) == {"string", "number", "object", "boolean"}


def stream_logger():
    import logging

    return logging.getLogger("tap-klaviyo-tests")
