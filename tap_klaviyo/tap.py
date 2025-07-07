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
        
        # fetch all metrics (unless they are missing from the existing catalog)
        is_sync = self.input_catalog is not None
        should_query_metrics = next((t for t in iter(self.input_catalog.keys()) if "events_" in t), None) if is_sync else True
        if should_query_metrics:
            try:
                metrics_stream = MetricsStream(tap=self)
                metrics_response = metrics_stream.request_records({})
                metrics = [record for record in metrics_response]
            except Exception as e:
                self.logger.error(f"Error fetching metrics: {e}")
                metrics = []

            # create event stream per metric
            for metric in metrics:
                metric_name = metric["attributes"]["name"]
                metric_id = metric["id"]
                stream_name = f"events_{metric_name}".lower().replace(" ", "_")
                event_stream = type(
                    metric_name,
                    (EventsStream,),
                    {
                        "name": stream_name,
                        "metric_id": metric_id,
                    },
                )(tap=self)
                discovered_streams.append(event_stream)

        return discovered_streams


if __name__ == "__main__":
    TapKlaviyo.cli()
