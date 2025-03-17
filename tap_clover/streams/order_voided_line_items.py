from __future__ import annotations

from tap_clover.client import CloverStream
from tap_clover.streams.orders import OrdersStream


class OrderVoidedLineItemsStream(CloverStream):
    """Stream for retrieving order voided line item records from the CloverStream API."""

    name = "order_voided_line_items"
    primary_keys = ["id"]
    replication_key = None
    expandable_keys = []
    parent_stream_type = OrdersStream

    @property
    def path(self) -> str:
        merchant_id = self.context.get("merchant_id")
        order_id = self.context.get("order_id")
        return f"/v3/merchants/{merchant_id}/orders/{order_id}/voided_line_items"
