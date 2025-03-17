"""REST client handling, including CloverStream base class."""

from __future__ import annotations

import backoff
import decimal
import json
import requests
import typing as t
from datetime import datetime
from functools import cached_property
from importlib import resources

from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.exceptions import RetriableAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator, BaseOffsetPaginator
from singer_sdk.streams import RESTStream

from tap_clover.auth import CloverAuthenticator
from tap_clover.pagination import CustomOffsetPaginator

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Auth, Context


# Reference local JSON schema files.
SCHEMAS_DIR = resources.files(__package__) / "schemas"

def flatten_nested_objects(data, parent_key='', sep='_'):
    """
        Recursively flattens a nested dictionary or list of dictionaries.
        This function traverses the input data, flattening nested dictionaries and lists
        into a single-level dictionary. Keys of nested dictionaries are combined with
        their parent keys using the specified separator (default '_'). Lists of dictionaries
        are flattened by appending the index of each item to the parent key.
        Excludes '_links', '_embedded', and 'self' keys from flattening.
        """
    items = []
    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict) and k not in ("_links", "_embedded", "self"):
            items.extend(flatten_nested_objects(v, new_key, sep=sep).items())
        elif isinstance(v, list) and k not in ("_links", "_embedded", "self"):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    items.extend(flatten_nested_objects(item, f"{new_key}{sep}{i}", sep=sep).items())
                else:
                    items.append((f"{new_key}{sep}{i}", item))
        else:
            items.append((new_key, v))
    return dict(items)

def convert_to_timestamp(value):
    # If the value is already an integer (Unix timestamp)
    if isinstance(value, int):
        return value
    # If the value is a string that can be converted to an integer (Unix timestamp in string form)
    elif isinstance(value, str):
        # Try converting the string to an integer (it might be a Unix timestamp in string form)
        try:
            return int(value)
        except ValueError:
            # If it's not an integer string, try parsing it as an ISO 8601 datetime string
            try:
                dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
                return int(dt.timestamp())
            except ValueError:
                raise ValueError("The starting value is not valid ISO 8601 or Unix timestamp")
    else:
        raise ValueError("The starting value is neither an integer nor a valid string.")

class CloverStream(RESTStream):
    """Clover stream class."""

    # Update this value if necessary or override `parse_response`.
    records_jsonpath = "$[*]"

    expandable_keys = []

    @property
    def url_base(self) -> str:
        """Return the API URL root based on environment and region settings."""
        if self.config.get("is_sandbox"):
            return "https://sandbox.dev.clover.com"

        region = self.config.get("region", "us").lower()

        base_urls = {
            "us": "https://api.clover.com",
            "canada": "https://api.clover.com",
            "europe": "https://api.eu.clover.com",
            "latin_america": "https://api.la.clover.com",
        }

        return base_urls.get(region, "https://api.clover.com")

    @cached_property
    def authenticator(self) -> Auth:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        is_sandbox = self.config.get("is_sandbox", True)
        sandbox_api_token = self.config.get("api_token", "")

        return (
            APIKeyAuthenticator.create_for_stream(
                self,
                key="authorization",
                value=f"Bearer {sandbox_api_token}",
                location="header",
            ) if is_sandbox
            else CloverAuthenticator.create_for_stream(self)
        )

    @property
    def schema(self) -> dict:
        schema_path = SCHEMAS_DIR / f"{self.name}.json"
        with schema_path.open("r", encoding="utf-8") as schema_file:
            return json.load(schema_file)

    @property
    def http_headers(self) -> dict:
        """Return any additional HTTP headers needed for the request."""
        headers: dict[str, str] = {}
        if self.config.get("user_agent"):
            headers["User-Agent"] = self.config["user_agent"]
        return headers

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance.
        """
        return CustomOffsetPaginator(start_value=0, page_size=1000)

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            params["offset"] = next_page_token
        if self.expandable_keys:
            params["expand"] = ",".join(self.expandable_keys)
        starting_timestamp = self.get_starting_replication_key_value(context)
        if self.replication_key and starting_timestamp:
            starting_timestamp = convert_to_timestamp(starting_timestamp)
            params["filter"] = f"{self.replication_key}>=[{starting_timestamp}]]"
            params["orderBy"] = f"{self.replication_key}%20DESC"
        return params

    def request_decorator(self, func: t.Callable) -> t.Callable:
        """Return a decorator that retries the function call on certain exceptions.

        The decorator retries on specific exceptions like:
        - RetriableAPIError
        - ReadTimeout
        - ConnectionError
        - RequestException
        - ConnectionRefusedError

        It uses exponential backoff with a maximum of 7 attempts and a factor of 2.
        """
        decorator: t.Callable = backoff.on_exception(
            backoff.expo,
            (
                RetriableAPIError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.RequestException,
                ConnectionRefusedError,
            ),
            max_tries=7,
            factor=2,
        )(func)
        return decorator

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the API response and yield individual records.
        """
        try:
            json_response = response.json(parse_float=decimal.Decimal)
        except requests.exceptions.JSONDecodeError as e:
            self.logger.error("Failed to decode JSON response: %s", e)
            return iter([])

        if "elements" in json_response:
            records = json_response.get("elements")
        else:
            records = extract_jsonpath(self.records_jsonpath, input=json_response)
        yield from records

    def post_process(
            self, row: dict, context: Context | None = None
    ) -> dict | None:
        """Perform post-processing on each record before output.
        """
        row = flatten_nested_objects(row)
        return row

    def validate_response(self, response: requests.Response) -> None:
        """Validate the response from the API.

        If the status code indicates success (200-299), the response is validated by
        the parent class. Otherwise, a warning is logged for unexpected status codes.
        """
        status_code = response.status_code
        if 200 <= status_code < 300:
            super().validate_response(response)
        else:
            self.logger.warning(
                "Received unexpected response status code: %s", status_code
            )
