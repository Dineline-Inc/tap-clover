from __future__ import annotations

from tap_clover.client import CloverStream
from tap_clover.streams.merchants import MerchantsStream


class EmployeeShiftsStream(CloverStream):
    """Stream for retrieving employee shift records from the CloverStream API."""

    name = "employee_shifts"
    primary_keys = ["id", "employee_id", "merchant_id"]
    replication_key = None
    expandable_keys = ["employee", "overrideInEmployee", "overrideOutEmployee"]
    parent_stream_type = MerchantsStream

    @property
    def path(self) -> str:
        merchant_id = self.context.get("merchant_id")
        return f"/v3/merchants/{merchant_id}/shifts"
