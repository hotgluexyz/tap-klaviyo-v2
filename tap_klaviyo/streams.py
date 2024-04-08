"""Stream type classes for tap-klaviyo."""

from tap_klaviyo.client import KlaviyoStream


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


class ListMembersStream(KlaviyoStream):
    """Define custom stream."""

    name = "list_members"
    path = "/lists/{id}/profiles"
    primary_keys = ["id"]
    replication_key = "joined_group_at"
    parent_stream_type = ListsStream
