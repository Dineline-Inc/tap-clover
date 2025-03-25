"""Clover tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_clover.streams.cash_events import CashEventsStream
# Import the custom stream types from our streams folder.
from tap_clover.streams.customers import CustomersStream
from tap_clover.streams.employee_shifts import EmployeeShiftsStream
from tap_clover.streams.employees import EmployeesStream
from tap_clover.streams.inventory_attributes import InventoryAttributesStream
from tap_clover.streams.inventory_categories import InventoryCategoriesStream
from tap_clover.streams.inventory_discounts import InventoryDiscountsStream
from tap_clover.streams.inventory_item_groups import InventoryItemGroupsStream
from tap_clover.streams.inventory_item_stocks import InventoryItemStocksStream
from tap_clover.streams.inventory_items import InventoryItemsStream
from tap_clover.streams.inventory_modifier_groups import InventoryModifierGroupsStream
from tap_clover.streams.inventory_modifiers import InventoryModifiersStream
from tap_clover.streams.inventory_tags import InventoryTagsStream
from tap_clover.streams.inventory_tax_rates import InventoryTaxRatesStream
from tap_clover.streams.merchant_addresses import MerchantAddressesStream
from tap_clover.streams.merchant_default_service_charges import MerchantDefaultServiceChargesStream
from tap_clover.streams.merchant_devices import MerchantDevicesStream
from tap_clover.streams.merchant_gateways import MerchantGatewaysStream
from tap_clover.streams.merchant_opening_hours import MerchantOpeningHoursStream
from tap_clover.streams.merchant_order_types import MerchantOrderTypesStream
from tap_clover.streams.merchant_properties import MerchantPropertiesStream
from tap_clover.streams.merchant_roles import MerchantRolesStream
from tap_clover.streams.merchant_tenders import MerchantTendersStream
from tap_clover.streams.merchant_tip_suggestions import MerchantTipSuggestionsStream
from tap_clover.streams.merchants import MerchantsStream
from tap_clover.streams.order_discounts import OrderDiscountsStream
from tap_clover.streams.order_line_items import OrderLineItemsStream
from tap_clover.streams.order_voided_line_items import OrderVoidedLineItemsStream
from tap_clover.streams.orders import OrdersStream
from tap_clover.streams.payments import PaymentsStream
from tap_clover.streams.refunds import RefundsStream


class TapClover(Tap):
    """Singer tap for the Clover API."""

    name = "tap-clover"

    def __init__(
            self,
            *,
            config: dict | PurePath | str | list[PurePath | str] | None = None,
            catalog: PurePath | str | dict | Catalog | None = None,
            state: PurePath | str | dict | None = None,
            parse_env_config: bool = False,
            validate_config: bool = True,
    ) -> None:
        super().__init__(
            config=config,
            catalog=catalog,
            state=state,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )
        self.config_file = config[0]

    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
            required=False
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=True
        ),
        th.Property(
            "is_sandbox",
            th.BooleanType,
            required=True,
            title="IS Sandbox",
            description="Whether or not to use the sandbox instance.",
        ),
        th.Property(
            "merchant_id",
            th.StringType,
            required=True,
            title="Merchant ID",
            description="Clover Merchant ID.",
        ),
        th.Property(
            "client_id",
            th.StringType,
            secret=True,
            title="App ID",
            description="Clover Application ID.",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            secret=True,
            title="App Secret",
            description="Clover Application Secret.",
        ),
        th.Property(
            "api_token",
            th.StringType,
            secret=True,
            title="API Token",
            description="API Token (sandbox only).",
        ),
        th.Property(
            "region",
            th.StringType,
            default="us",
            title="Region",
            description="Specify the instance region.",
        ),
        th.Property(
            "user_agent",
            th.StringType,
            title="User Agent",
            description="A custom User-Agent header to send with each request.",
        ),
    ).to_dict()

    def discover_streams(self) -> list:
        """Return a list of discovered streams.

        This enables discovery mode (--discover) to output a catalog of streams,
        their schemas, replication keys, and other metadata.
        """
        return [
            MerchantsStream(self),
            MerchantAddressesStream(self),
            MerchantDefaultServiceChargesStream(self),
            MerchantDevicesStream(self),
            MerchantGatewaysStream(self),
            MerchantOpeningHoursStream(self),
            MerchantOrderTypesStream(self),
            MerchantPropertiesStream(self),
            MerchantRolesStream(self),
            MerchantTendersStream(self),
            MerchantTipSuggestionsStream(self),
            CashEventsStream(self),
            CustomersStream(self),
            EmployeesStream(self),
            EmployeeShiftsStream(self),
            InventoryAttributesStream(self),
            InventoryCategoriesStream(self),
            InventoryDiscountsStream(self),
            InventoryItemsStream(self),
            InventoryItemGroupsStream(self),
            InventoryItemStocksStream(self),
            InventoryModifierGroupsStream(self),
            InventoryModifiersStream(self),
            InventoryTagsStream(self),
            InventoryTaxRatesStream(self),
            OrdersStream(self),
            OrderDiscountsStream(self),
            OrderLineItemsStream(self),
            OrderVoidedLineItemsStream(self),
            PaymentsStream(self),
            RefundsStream(self),
        ]


if __name__ == "__main__":
    TapClover.cli()
