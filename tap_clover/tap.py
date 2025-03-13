"""Clover tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# Import the custom stream types from our streams folder.
from tap_clover.streams.customers import CustomersStream
from tap_clover.streams.employees import EmployeesStream
from tap_clover.streams.merchants import MerchantsStream

class TapClover(Tap):
    """Singer tap for the Clover API."""

    name = "tap-clover"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "is_sandbox",
            th.BooleanType,
            required=True,
            title="IS Sandbox",
            description="Whether or not to use the sandbox instance.",
        ),
        th.Property(
            "merchant_id",
            th.StringType,
            required=True,
            title="Merchant ID",
            description="Clover Merchant ID.",
        ),
        th.Property(
            "app_id",
            th.StringType,
            secret=True,
            title="App ID",
            description="Clover Application ID.",
        ),
        th.Property(
            "app_secret",
            th.StringType,
            secret=True,
            title="App Secret",
            description="Clover Application Secret.",
        ),
        th.Property(
            "redirect_uri",
            th.StringType,
            title="Redirect URI",
            description="Custom Redirect URI.",
        ),
        th.Property(
            "api_token",
            th.StringType,
            secret=True,
            title="API Token",
            description="API Token (sandbox only).",
        ),
        th.Property(
            "region",
            th.StringType,
            default="us",
            title="Region",
            description="Specify the instance region.",
        ),
        th.Property(
            "user_agent",
            th.StringType,
            title="User Agent",
            description="A custom User-Agent header to send with each request.",
        ),
    ).to_dict()

    def discover_streams(self) -> list:
        """Return a list of discovered streams.

        This enables discovery mode (--discover) to output a catalog of streams,
        their schemas, replication keys, and other metadata.
        """
        return [
            MerchantsStream(self),
            CustomersStream(self),
            EmployeesStream(self),
        ]


if __name__ == "__main__":
    TapClover.cli()
