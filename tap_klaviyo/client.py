"""REST client handling, including KlaviyoStream base class."""

import re
from datetime import datetime
from typing import Any, Dict, Optional

import requests
from backports.cached_property import cached_property
from pendulum import parse
from singer_sdk import typing as th
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

from tap_klaviyo.auth import KlaviyoAuthenticator


class KlaviyoStream(RESTStream):
    """Klaviyo stream class."""

    url_base = "https://a.klaviyo.com/api"

    records_jsonpath = "$.data[*]"
    next_page_token_jsonpath = "$.links.next"

    @property
    def authenticator(self):
        # auth with hapikey
        if self.config.get("api_private_key"):
            api_key = f'Klaviyo-API-Key {self.config.get("api_private_key")}'
            return APIKeyAuthenticator.create_for_stream(
                self, key="Authorization", value=api_key, location="header"
            )
        # auth with acces token
        return KlaviyoAuthenticator.create_for_stream(self)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["revision"] = "2024-02-15"
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            token_link = next(iter(all_matches), None)
            if token_link:
                match = re.search(r"page%5Bcursor%5D=(.*)", token_link)
                if match:
                    return match.group(1)

    def get_starting_time(self, context):
        start_date = self.config.get("start_date")
        if start_date:
            start_date = parse(self.config.get("start_date"))
        rep_key = self.get_starting_timestamp(context)
        return rep_key or start_date

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
            params["filter"] = f"greater-than({self.replication_key},{start_date})"
        return params

    def post_process(self, row, context):
        row = super().post_process(row, context)
        rep_key = self.replication_key
        if row.get("attributes") and self.replication_key:
            row[rep_key] = row["attributes"][rep_key]
        return row

    def is_unix_timestamp(self, date):
        try:
            datetime.strptime(date, "%Y-%m-%dT%H:%M:%S%z")
            return True
        except:
            return False

    def get_jsonschema_type(self, obj):
        dtype = type(obj)

        if dtype == int:
            return th.IntegerType()
        if dtype == float:
            return th.NumberType()
        if dtype == str:
            if self.is_unix_timestamp(obj):
                return th.DateTimeType()
            return th.StringType()
        if dtype == bool:
            return th.BooleanType()
        if dtype == list:
            if len(obj) > 0:
                return th.ArrayType(self.get_jsonschema_type(obj[0]))
            else:
                return th.ArrayType(
                    th.CustomType({"type": ["number", "string", "object"]})
                )
        if dtype == dict:
            obj_props = []
            for key in obj.keys():
                obj_props.append(th.Property(key, self.get_jsonschema_type(obj[key])))
            return th.ObjectType(*obj_props)
        else:
            return th.CustomType({"type": ["number", "string", "object"]})

    def get_schema(self) -> dict:
        """Dynamically detect the json schema for the stream.
        This is evaluated prior to any records being retrieved.
        """
        self._requests_session = requests.Session()
        # Get the data
        headers = self.http_headers
        headers.update(self.authenticator.auth_headers)
        path = self.path

        request_type = self.rest_method
        url = self.url_base + path

        # discover for child streams
        if self.parent_stream_type:
            parent_url = self.url_base + self.parent_stream_type.path
            id = requests.request(
                request_type,
                parent_url,
                headers=headers,
            ).json()["data"]
            if id:
                id = id[0]["id"]
            url = url.replace("{id}", id)

        records = requests.request(
            request_type,
            url,
            headers=headers,
        )
        if records.status_code == 200:
            records = records.json().get("data", [])
        else:
            raise Exception(
                f"There was an error when fetching data for schemas {records.text}"
            )

        if len(records) > 0:
            properties = []
            property_names = set()

            # Loop through all records – some objects have different keys
            for record in records:
                # Loop through each key in the object
                for name in record.keys():
                    if name in property_names:
                        continue
                    # Add the new property to our list
                    property_names.add(name)
                    if self.is_unix_timestamp(record[name]):
                        properties.append(th.Property(name, th.DateTimeType))
                    else:
                        properties.append(
                            th.Property(name, self.get_jsonschema_type(record[name]))
                        )
                # if the rep_key is not at a header level add updated as default
                if (
                    self.replication_key is not None
                    and self.replication_key not in record.keys()
                ):
                    properties.append(
                        th.Property(self.replication_key, th.DateTimeType)
                    )
                # we need to process only first record
                break
            # Return the list as a JSON Schema dictionary object
            property_list = th.PropertiesList(*properties).to_dict()

            return property_list
        else:
            return th.PropertiesList(
                th.Property("id", th.StringType),
                th.Property(self.replication_key, th.DateTimeType),
            ).to_dict()

    @cached_property
    def schema(self) -> dict:
        return self.get_schema()
