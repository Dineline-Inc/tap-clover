from __future__ import annotations

from tap_clover.client import CloverStream
from tap_clover.streams.merchants import MerchantsStream


class InventoryModifierGroupsStream(CloverStream):
    """Stream for retrieving inventory modifier group records from the CloverStream API."""

    name = "inventory_modifier_groups"
    primary_keys = ["id"]
    replication_key = None
    expandable_keys = []
    parent_stream_type = MerchantsStream

    @property
    def path(self) -> str:
        merchant_id = self.context.get("merchant_id")
        return f"/v3/merchants/{merchant_id}/modifier_groups"
