from __future__ import annotations

from tap_clover.client import CloverStream
from tap_clover.streams.merchants import MerchantsStream


class InventoryTaxRatesStream(CloverStream):
    """Stream for retrieving inventory tax rate records from the CloverStream API."""

    name = "inventory_tax_rates"
    primary_keys = ["id"]
    replication_key = None
    expandable_keys = []
    parent_stream_type = MerchantsStream

    @property
    def path(self) -> str:
        merchant_id = self.context.get("merchant_id")
        return f"/v3/merchants/{merchant_id}/tax_rates"
