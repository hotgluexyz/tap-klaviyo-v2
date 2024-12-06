"""REST client handling, including KlaviyoStream base class."""

import re
from datetime import datetime
from typing import Any, Dict, Optional, Callable

import requests
from backports.cached_property import cached_property
from pendulum import parse
from singer_sdk import typing as th
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

from tap_klaviyo.auth import KlaviyoAuthenticator
from urllib.parse import urlparse, parse_qs
from urllib3.exceptions import ProtocolError, InvalidChunkLength
from requests.exceptions import  ReadTimeout, ChunkedEncodingError
import backoff
import os
import json
import logging


class KlaviyoStream(RESTStream):
    """Klaviyo stream class."""

    url_base = "https://a.klaviyo.com/api"

    records_jsonpath = "$.data[*]"
    next_page_token_jsonpath = "$.links.next"

    @property
    def authenticator(self):
        api_key = self.config.get("api_private_key") or self.config.get("api_key")
        # auth with access token
        if self.config.get("refresh_token"):
            return KlaviyoAuthenticator.create_for_stream(self)
        # auth with api key
        elif api_key:
            api_key = f"Klaviyo-API-Key {api_key}"
            return APIKeyAuthenticator.create_for_stream(
                self, key="Authorization", value=api_key, location="header"
            )
        else:
            raise FatalAPIError("No valid authentication method found")    

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["revision"] = "2024-10-15"
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
                parsed_url = urlparse(token_link)
                # Extract the query parameters
                query_params = parse_qs(parsed_url.query)
                cursor = query_params.get('page[cursor]')
                if cursor:
                    if len(cursor)>0:
                        next_page_token = cursor[0]
                if next_page_token:
                    return next_page_token
        return None        

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
        for key, value in row.get("attributes", {}).items():
            row[key] = value
        row.pop("attributes", None)
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
                    th.CustomType({"type": ["string", "number", "object"]})
                )
        if dtype == dict:
            obj_props = []
            for key in obj.keys():
                obj_props.append(th.Property(key, self.get_jsonschema_type(obj[key])))
            if not obj_props:
                return th.CustomType({"type": ["string", "number", "object"]})
            return th.ObjectType(*obj_props)
        else:
            return th.CustomType({"type": ["string", "number", "object"]})

    def get_abs_path(self, path):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

    def load_schema(self, name):
        schema_path = self.get_abs_path(f'schemas/{name}.json')
        with open(schema_path, 'r') as schema_file:
            return json.load(schema_file)

    def _fill_missing_properties(self, property_list):
        try:
            default_schema = self.load_schema(self.name)
        except:
            logging.warning(f"The stream '{self.name}' does not have a default schema.")
            return property_list
        
        new_properties = default_schema.get('properties', {})

        def recursive_copy(source, target):
            for key, value in source.items():
                if isinstance(value, dict):
                    target[key] = target.get(key, {})
                    recursive_copy(value, target[key])
                else:
                    target[key] = value

        recursive_copy(property_list.get('properties', {}), new_properties)
        
        property_list['properties'] = new_properties
        return property_list

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

            record = records[0]
            # put attributes fields at header level
            attributes = record.pop("attributes", {})
            if attributes:
                record.update(attributes)

            # Loop through each key in the object
            for name in record.keys():
                if name in property_names:
                    continue
                # Add the new property to our list
                property_names.add(name)
                if name in  ["event_properties", "properties"]:
                    properties.append(
                        th.Property(name, th.CustomType({"type": ["object", "string"]}))
                    ) 
                elif self.is_unix_timestamp(record[name]):
                    properties.append(th.Property(name, th.DateTimeType))
                else:
                    properties.append(
                        th.Property(name, self.get_jsonschema_type(record[name]))
                    )
            # Return the list as a JSON Schema dictionary object
            property_list = th.PropertiesList(*properties).to_dict()
        else:
            property_list = th.PropertiesList(
                th.Property("id", th.StringType),
            ).to_dict()
        property_list = self._fill_missing_properties(property_list)
        if self.replication_key is not None:
            property_list["properties"].update(th.Property(self.replication_key,th.DateTimeType).to_dict())
        return property_list

    @cached_property
    def schema(self) -> dict:
        return self.get_schema()
    
    def request_decorator(self, func: Callable) -> Callable:
        """Instantiate a decorator for handling request failures."""
        decorator: Callable = backoff.on_exception(
            backoff.expo,
            (
                RetriableAPIError,
                ReadTimeout,
                ConnectionError,
                ConnectionResetError,
                ProtocolError,
                InvalidChunkLength,
                requests.RequestException,
                ChunkedEncodingError,
            ),
            max_tries=8,
            factor=5,
        )(func)
        return decorator
