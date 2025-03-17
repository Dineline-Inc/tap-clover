from __future__ import annotations

from tap_clover.client import CloverStream
from tap_clover.streams.orders import OrdersStream


class OrderLineItemsStream(CloverStream):
    """Stream for retrieving order line item records from the CloverStream API."""

    name = "order_line_items"
    primary_keys = ["id"]
    replication_key = None
    expandable_keys = ["employee", "orderType"]
    parent_stream_type = OrdersStream

    @property
    def path(self) -> str:
        merchant_id = self.context.get("merchant_id")
        order_id = self.context.get("order_id")
        return f"/v3/merchants/{merchant_id}/orders/{order_id}/line_items"
