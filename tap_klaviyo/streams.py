"""Stream type classes for tap-klaviyo."""

from typing import Any, Dict, Optional
from tap_klaviyo.client import KlaviyoStream
from singer_sdk import typing as th


class ContactsStream(KlaviyoStream):
    """Define custom stream."""

    name = "contacts"
    path = "/profiles"
    primary_keys = ["id"]
    replication_key = "updated"


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
        if len(schema["properties"]) == 2:
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
