"""Stream type classes for tap-klaviyo."""

from typing import Any, Dict, Optional, List
from tap_klaviyo.client import KlaviyoStream
from hotglue_singer_sdk import typing as th
from datetime import datetime, timedelta, timezone
from pendulum import parse

def _as_utc(dt: datetime) -> datetime:
    """Return a timezone-aware UTC datetime."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

class ContactsStream(KlaviyoStream):
    """Define custom stream."""

    def __init__(self, tap):
        """Initialize contacts stream with configuration."""
        super().__init__(tap=tap)
        self.logger.info(f"Only sync new contacts: {self.config.get('only_sync_new_contacts')}")
        self.replication_key = "created" if self.config.get("only_sync_new_contacts") == True else "updated"

    name = "contacts"
    path = "/profiles"
    primary_keys = ["id"]


class ListsStream(KlaviyoStream):
    """Define custom stream."""

    name = "lists"
    path = "/lists"
    primary_keys = ["id"]
    replication_key = "updated"

    def get_child_context(self, record, context):
        return {"id": record["id"]}


class MetricsStream(KlaviyoStream):
    """Define custom stream."""

    name = "metrics"
    path = "/metrics"
    primary_keys = ["id"]
    replication_key = None


class EventsStream(KlaviyoStream):
    """Define custom stream."""

    name = "events"
    path = "/events"
    primary_keys = ["id"]
    replication_key = "datetime"

    def get_url_params(
            self, context: Optional[dict], next_page_token: Optional[Any]
        ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = super().get_url_params(context, next_page_token)
        # add filter to get only events for a metric
        if self.name != "events":
            params["filter"] = f"equals(metric_id,'{self.metric_id}')"
        return params
    
    def get_schema(self):
        schema = super().get_schema()
        # if schema only has pk and replication key, use default schema
        if schema.get("properties") and len(schema["properties"]) == 2:
            schema = th.PropertiesList(
                th.Property("type", th.StringType),
                th.Property("id", th.StringType),
                th.Property("relationships", th.ObjectType(
                    th.Property("profile", th.ObjectType(
                        th.Property("data", th.ObjectType(
                            th.Property("type", th.StringType),
                            th.Property("id", th.StringType),
                        )),
                        th.Property("links", th.ObjectType(
                            th.Property("self", th.StringType),
                            th.Property("related", th.StringType),
                        )),
                    )),
                    th.Property("metric", th.ObjectType(
                        th.Property("data", th.ObjectType(
                            th.Property("type", th.StringType),
                            th.Property("id", th.StringType),
                        )),
                        th.Property("links", th.ObjectType(
                            th.Property("self", th.StringType),
                            th.Property("related", th.StringType),
                        )),
                    )),
                )),
                th.Property("links", th.ObjectType(
                    th.Property("self", th.StringType),
                )),
                th.Property("timestamp", th.IntegerType),
                th.Property("event_properties", th.CustomType({"type": ["object", "string"]})),
                th.Property("datetime", th.DateTimeType),
                th.Property("uuid", th.StringType),
            ).to_dict()
        return schema
    

class ListMembersStream(KlaviyoStream):
    """Define custom stream."""

    name = "list_members"
    path = "/lists/{id}/profiles"
    primary_keys = ["id"]
    replication_key = "joined_group_at"
    parent_stream_type = ListsStream


class ReviewsStream(KlaviyoStream):
    """Define custom stream."""

    name = "reviews"
    path = "/reviews"
    primary_keys = ["id"]
    replication_key = "created"

    def get_url_params(
            self, context: Optional[dict], next_page_token: Optional[Any]
        ) -> Dict[str, Any]:
            """Return a dictionary of values to be used in URL parameterization."""
            params: dict = {}
            if next_page_token:
                params["page[cursor]"] = next_page_token
            start_date = self.get_starting_time(context)
            if self.replication_key and start_date:
                start_date = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                params["filter"] = f"greater-or-equal({self.replication_key},{start_date})"
            return params

class ReportStream(KlaviyoStream):
    """Report stream for metric aggregates using Klaviyo's Query Metric Aggregates API."""
    page_size = 500

    def __init__(self, tap, report_config: Dict[str, Any]):
        """Initialize report stream with configuration."""
        self.report_config = report_config
        self.name = report_config["name"]
        self.metric_id = report_config["metric_id"]

        # Handle dimensions as comma-separated string
        dimensions = report_config.get("dimensions", "Campaign Name,$message")
        self.dimensions = [d.strip() for d in dimensions.split(",")]

        # Handle metrics as comma-separated string
        aggregation_types = report_config.get("aggregation_types", "count")
        self.aggregation_types = [m.strip() for m in aggregation_types.split(",")]

        self.interval = report_config.get("interval", "day")
        self.path = "/metric-aggregates"

        # Call parent constructor
        super().__init__(tap=tap)
        self.end_date = _as_utc(parse(self.config["end_date"])) if self.config.get("end_date") else datetime.now(timezone.utc)
        
    @property
    def rest_method(self) -> str:
        """Return the REST method for this stream."""
        return "POST"

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return empty params since we use POST body."""
        return {}

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the request payload for the metric aggregates API."""
        # Resolve end_date first (config takes precedence), then normalize to UTC
        end_date = self.end_date

        # Start date: use bookmark if present; otherwise default to a recent window (7 days)
        start_date = self.get_starting_time(context)
        if start_date is None:
            start_date = end_date - timedelta(days=7)
        start_date = _as_utc(start_date)

        # Guard: if start > end (bad bookmark), fall back to last 7 days ending at end_date
        if start_date >= end_date:
            self.logger.warning(
                f"Start date ({start_date.isoformat()}) is not before end date ({end_date.isoformat()}); "
                "falling back to last 7 days."
            )
            start_date = end_date - timedelta(days=7)

        # Klaviyo API: maximum time range is 1 year
        one_year_ago = end_date - timedelta(days=365)
        if start_date < one_year_ago:
            self.logger.warning(f"Limited date range to 1 year for report stream {self.name}")
            start_date = one_year_ago

        # Format as ISO8601 without microseconds, with 'Z'
        start_date_str = start_date.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        end_date_str = end_date.replace(microsecond=0).isoformat().replace("+00:00", "Z")

        # Build filters (Klaviyo expects filter expressions)
        filters = [
            f"greater-or-equal(datetime,{start_date_str})",
            f"less-than(datetime,{end_date_str})",
        ]

        attributes = {
                    "metric_id": self.metric_id,
                    "measurements": self.aggregation_types,
                    "interval": self.interval,
                    "timezone": "UTC",
                    "filter": filters,
                    "page_size": self.page_size,
                    "by": self.dimensions,
                }
        if next_page_token:
            attributes["page_cursor"] = next_page_token
            

        payload = {
            "data": {
                "type": "metric-aggregate",
                "attributes": attributes,
            }
        }

        return payload

    def get_schema(self) -> dict:
        """Return the schema for this report stream."""
        properties = [
            th.Property("date", th.DateTimeType, required=True),
            th.Property("metric_id", th.StringType, required=True),
        ]
        
        # Add dimension properties
        for dimension in self.dimensions:
            properties.append(th.Property(dimension, th.StringType))
        
        # Add metric properties
        for agg_type in self.aggregation_types:
            properties.append(th.Property(agg_type, th.NumberType))

        
        return th.PropertiesList(*properties).to_dict()

    def parse_response(self, response) -> List[Dict[str, Any]]:
        """Parse the response from the metric aggregates API."""
        data = response.json()
        results = []
        
        # Extract data from the response
        if "data" in data and "attributes" in data["data"]:
            attributes = data["data"]["attributes"]
            dates = attributes.get("dates", [])
            data_points = attributes.get("data", [])
            
            # Process each data point
            for item in data_points:
                # Get dimensions array
                dimensions = item.get("dimensions", [])
                measurements = item.get("measurements", {})
                
                # Create a record for each date
                for index, date_str  in enumerate(dates):
                    formatted_date = parse(date_str).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                    record = {
                        "date": formatted_date,
                        "metric_id": self.metric_id,
                    }
                    
                    # Add dimension values
                    for i, dimension in enumerate(self.dimensions):
                        if i < len(dimensions):
                            record[dimension] = dimensions[i]
                        else:
                            record[dimension] = None
                    
                    # Add metric values
                    for agg_type in self.aggregation_types:
                        metric_values = measurements.get(agg_type, [])
                        if metric_values and len(metric_values) > index:
                            record[agg_type] = metric_values[index]
                        else:
                            record[agg_type] = None
                    
                    results.append(record)
        
        return results
