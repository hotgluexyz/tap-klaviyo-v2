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
    ReportStream,
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
        th.Property(
            "custom_reports",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType, required=True),
                    th.Property("metric_id", th.StringType, required=True),
                    th.Property("dimensions", th.StringType, required=False),
                    th.Property("metrics", th.StringType, required=False),
                    th.Property("interval", th.StringType, required=False),
                )
            ),
            required=False,
            description="Custom report configurations for metric aggregates"
        ),
    ).to_dict()

    def metric_name_to_id(self, metrics, metric_name):
        metric_id = next((metric["id"] for metric in metrics if metric["attributes"]["name"] == metric_name), None)
        return metric_id


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
        metrics = None
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




        if metrics:
            default_reports = self._get_default_reports(metrics)
            custom_reports = self.config.get("custom_reports", [])

            for report_config in default_reports:
                try:
                    report_stream = ReportStream(tap=self, report_config=report_config)
                    report_stream.replication_key = "date"
                    report_stream.primary_keys = ["date", "metric_id"] + report_stream.dimensions
                    discovered_streams.append(report_stream)
                except Exception as e:
                    self.logger.error(f"Error creating default report stream {report_config['name']}: {e}")

            # Add custom reports streams from config
            custom_reports = self.config.get("custom_reports", [])
            for report_config in custom_reports:
                if report_config.get("metric_name"):
                    metric_id = self.metric_name_to_id(metrics, report_config["metric_name"])
                    if not metric_id:
                        raise ValueError(f"Metric name {report_config['metric_name']} not found")
                    report_config["metric_id"] = metric_id

                report_stream = ReportStream(tap=self, report_config=report_config)
                report_stream.replication_key = "date"
                report_stream.primary_keys = ["date", "metric_id"] + report_stream.dimensions
                discovered_streams.append(report_stream)

        return discovered_streams

    def _get_default_reports(self, metrics):
            """Return default report configurations."""
            return [
                {
                    "name": "emails_opened_per_day", 
                    "metric_id": self.metric_name_to_id(metrics, "Opened Email"),
                    "dimensions": "Campaign Name,$message",
                    "aggregation_types": "count",
                    "interval": "day"
                },
                {
                    "name": "emails_clicked_per_day",
                    "metric_id": self.metric_name_to_id(metrics, "Clicked Email"),  # Clicked Email
                    "dimensions": "Campaign Name,$message",
                    "aggregation_types": "count",
                    "interval": "day"
                },
                {
                    "name": "emails_bounced_per_day",
                    "metric_id": self.metric_name_to_id(metrics, "Bounced Email"),  # Bounced Email
                    "dimensions": "Campaign Name,$message",
                    "aggregation_types": "count",
                    "interval": "day"
                },
                {
                    "name": "emails_received_per_day",
                    "metric_id": self.metric_name_to_id(metrics, "Received Email"),  # Received Email
                    "dimensions": "Campaign Name,$message",
                    "aggregation_types": "count",
                    "interval": "day"
                },
                {
                    "name": "campaign_performance_daily",
                    "metric_id": self.metric_name_to_id(metrics, "Opened Email"),  # Opened Email
                    "dimensions": "Campaign Name,$message",
                    "aggregation_types": "count",
                    "interval": "day"
                }
            ]

if __name__ == "__main__":
    TapKlaviyo.cli()
