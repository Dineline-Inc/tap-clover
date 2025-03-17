from __future__ import annotations

from tap_clover.client import CloverStream
from tap_clover.streams.orders import OrdersStream


class OrderDiscountsStream(CloverStream):
    """Stream for retrieving order discount records from the CloverStream API."""

    name = "order_discounts"
    primary_keys = ["id", "merchant_id"]
    replication_key = None
    expandable_keys = []
    parent_stream_type = OrdersStream

    @property
    def path(self) -> str:
        merchant_id = self.context.get("merchant_id")
        order_id = self.context.get("order_id")
        return f"/v3/merchants/{merchant_id}/orders/{order_id}/discounts"
