from __future__ import annotations

from tap_clover.client import CloverStream
from tap_clover.streams.merchants import MerchantsStream


class PaymentsStream(CloverStream):
    """Stream for retrieving payment records from the CloverStream API."""

    name = "payments"
    primary_keys = ["id", "merchant_id"]
    replication_key = None
    expandable_keys = [
        "tender",
        "germanInfo",
        "cardTransaction",
        "dccInfo",
        "transactionInfo",
        "externalReferenceId",
        "oceanGatewayInfo",
        "appTracking",
        "order"
    ]
    parent_stream_type = MerchantsStream

    @property
    def path(self) -> str:
        merchant_id = self.context.get("merchant_id")
        return f"/v3/merchants/{merchant_id}/payments"
