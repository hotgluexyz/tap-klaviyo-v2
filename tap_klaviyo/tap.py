"""Klaviyo tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_klaviyo.streams import (
    ContactsStream,
    EventsStream,
    ListMembersStream,
    ListsStream,
    MetricsStream,
    ReviewsStream,
)

STREAM_TYPES = [
    ContactsStream,
    ListsStream,
    MetricsStream,
    EventsStream,
    ListMembersStream,
    ReviewsStream,
]


class TapKlaviyo(Tap):
    """Klaviyo tap class."""

    name = "tap-klaviyo"

    def __init__(
        self,
        config=None,
        catalog=None,
        state=None,
        parse_env_config=False,
        validate_config=True,
    ) -> None:
        self.config_file = config[0]
        super().__init__(config, catalog, state, parse_env_config, validate_config)

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
        ),
        th.Property(
            "client_secret",
            th.StringType,
        ),
        th.Property(
            "refresh_token",
            th.StringType,
        ),
        th.Property(
            "api_key",
            th.StringType,
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        discovered_streams = []
        for stream_class in STREAM_TYPES:
            try:
                stream = stream_class(tap=self)
                discovered_streams.append(stream)
            except Exception as e:
                self.logger.error(f"Error discovering stream {stream_class}: {e}")
        return discovered_streams


if __name__ == "__main__":
    TapKlaviyo.cli()
