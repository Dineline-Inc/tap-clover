from __future__ import annotations

from tap_clover.client import CloverStream
from tap_clover.streams.merchants import MerchantsStream


class OrdersStream(CloverStream):
    """Stream for retrieving order records from the CloverStream API."""

    name = "orders"
    primary_keys = ["id"]
    replication_key = None
    expandable_keys = [
        "employee",
        "orderType",
        "serviceCharge"
    ]
    parent_stream_type = MerchantsStream

    @property
    def path(self) -> str:
        merchant_id = self.context.get("merchant_id")
        return f"/v3/merchants/{merchant_id}/orders"

    def get_child_context(self, record: dict, context: [dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "merchant_id": self.context.get("merchant_id"),
            "order_id": record.get("id"),
        }
