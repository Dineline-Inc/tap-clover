from __future__ import annotations

from tap_clover.client import CloverStream
from tap_clover.streams.merchants import MerchantsStream


class InventoryDiscountsStream(CloverStream):
    """Stream for retrieving inventory discount records from the CloverStream API."""

    name = "inventory_discounts"
    primary_keys = ["id", "merchant_id"]
    replication_key = None
    expandable_keys = []
    parent_stream_type = MerchantsStream

    @property
    def path(self) -> str:
        merchant_id = self.context.get("merchant_id")
        return f"/v3/merchants/{merchant_id}/discounts"
