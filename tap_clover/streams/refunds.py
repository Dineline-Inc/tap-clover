from __future__ import annotations

from tap_clover.client import CloverStream
from tap_clover.streams.merchants import MerchantsStream


class RefundsStream(CloverStream):
    """Stream for retrieving refund records from the CloverStream API."""

    name = "refunds"
    primary_keys = ["id", "merchant_id"]
    replication_key = None
    expandable_keys = [
        "payment",
        "germanInfo",
        "appTracking",
        "employee",
        "overrideMerchantTender",
        "serviceCharge",
        "lineItems",
        "transactionInfo",
        "oceanGatewayInfo"
    ]
    parent_stream_type = MerchantsStream

    @property
    def path(self) -> str:
        merchant_id = self.context.get("merchant_id")
        return f"/v3/merchants/{merchant_id}/refunds"
