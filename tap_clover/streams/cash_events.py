from __future__ import annotations

from tap_clover.client import CloverStream
from tap_clover.streams.merchants import MerchantsStream


class CashEventsStream(CloverStream):
    """Stream for retrieving cash event records from the CloverStream API."""

    name = "cash_events"
    primary_keys = ["id"]
    replication_key = None
    expandable_keys = ["employee", "device"]
    parent_stream_type = MerchantsStream

    @property
    def path(self) -> str:
        merchant_id = self.context.get("merchant_id")
        return f"/v3/merchants/{merchant_id}/cash_events"
