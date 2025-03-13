from __future__ import annotations

from tap_clover.client import CloverStream


class MerchantsStream(CloverStream):
    """Stream for retrieving merchant records from the CloverStream API."""

    name = "merchants"
    primary_keys = ["id"]
    replication_key = None
    expandable_keys = [
        "bankProcessing",
        "merchantBoarding",
        "merchantL3Prerequisite",
        "deviceBoarding",
        "hierarchy",
        "address",
        "owner",
        "gateway",
        "properties",
        "openingHours",
        "partnerApp",
        "selfBoardingApplication",
        "equipmentSummary"
    ]

    @property
    def path(self) -> str:
        merchant_id = self.config.get("merchant_id")
        return f"/v3/merchants/{merchant_id}"

    def get_child_context(self, record: dict, context: [dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "merchant_id": record.get("id"),
        }
