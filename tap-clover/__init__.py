""" Singer Tap for Clover API's """
# !/usr/bin/env python3
import json
import singer
import requests
from singer import utils
import pandas as pd
from schemas import get_schemas, STREAMS
import custom_functions as cf


REQUIRED_CONFIG_KEYS = ["host", "access_token", "merchant_id"]
LOGGER = singer.get_logger()


def sync_direct_api_get(config, api):

    """ get the direct api data get method"""

    host = config['host']
    api = api.format(mId=config['merchant_id'])
    access_token = config['access_token']
    header = {"Content-type": "application/json"}
    url = "{host}{api}?access_token={access_token}&return_null_fields=true"\
        .format(host=host, api=api, access_token=access_token)
    schemas = {}

    responses = requests.get(url, headers=header)
    responses.raise_for_status()
    response = responses.json()

    if bool(response):
        is_sync = True
        final_rows, schemas = cf.clean_api_data(response)
    else:
        is_sync = False
        final_rows = []

    return final_rows, schemas, is_sync


def sync_merchants_api_get(config, api):

    """ get the direct api data get method"""

    final_rows = []
    schemas = {}
    host = config['host']
    api = api.format(mId=config['merchant_id'])
    access_token = config['access_token']
    header = {"Content-type": "application/json"}
    url = "{host}{api}?access_token={access_token}&return_null_fields=true"\
        .format(host=host, api=api, access_token=access_token)

    responses = requests.get(url, headers=header)
    responses.raise_for_status()
    response = responses.json()

    if bool(response):
        final_rows, schemas = cf.clean_api_data({"response": [response]})
        is_sync = True
    else:
        is_sync = False

    return final_rows, schemas, is_sync


def get_order_ids(host, access_token, mid):

    """ Calls order API's and return list of orderIds """

    url = "{host}/v3/merchants/{mid}/orders?access_token={access_token}&return_null_fields=true"\
        .format(host=host, mid=mid, access_token=access_token)
    responses = requests.get(url)
    responses.raise_for_status()
    response = responses.json()

    key = [*response][0]
    rows = response[key]
    order_ids = []

    for row in rows:
        order_ids.append(row['id'])

    return order_ids


def sync_orders_line_items(config, api):

    """ get the direct api data get method"""

    host = config['host']
    order_ids = get_order_ids(host, config['access_token'], config['merchant_id'])
    final_rows = []

    for order_id in order_ids:
        api_url = api
        api_url = api_url.format(mId=config['merchant_id'], orderId=order_id)
        url = "{host}{api}?access_token={access_token}&return_null_fields=true"\
              .format(host=host, api=api_url, access_token=config['access_token'])

        responses = requests.get(url)
        responses.raise_for_status()
        response = responses.json()

        if bool(response):
            key = [*response][0]
            data_frame = pd.json_normalize(response[key])
            rows = json.loads(data_frame.to_json(orient='records'))
            final_rows = final_rows + rows

    if len(final_rows) > 0:
        is_sync = True
        rows, schemas = cf.clean_api_data({"response": final_rows})
    else:
        schemas = {}
        rows = []
        is_sync = False

    return rows, schemas, is_sync


SYNC_FUNCTIONS = {
    'Orders': sync_direct_api_get,
    'Merchant_opening_hours': sync_direct_api_get,
    'Customers': sync_direct_api_get,
    'Employees': sync_direct_api_get,
    'Inventory_items': sync_direct_api_get,
    'Inventory_item_stocks': sync_direct_api_get,
    'Inventory_tags': sync_direct_api_get,
    'Inventory_tax_rates': sync_direct_api_get,
    'Inventory_item_groups': sync_direct_api_get,
    'Inventory_categories': sync_direct_api_get,
    'Inventory_modifier_groups': sync_direct_api_get,
    'Inventory_modifiers': sync_direct_api_get,
    'Inventory_attributes': sync_direct_api_get,
    'Refunds': sync_direct_api_get,
    'Payments': sync_direct_api_get,
    'Employees_shifts': sync_direct_api_get,
    'Inventory_discounts': sync_direct_api_get,
    'Merchant_address': sync_merchants_api_get,
    'Merchant_gateway': sync_merchants_api_get,
    'Merchant_devices': sync_direct_api_get,
    'Merchant_properties': sync_merchants_api_get,
    'Merchant_default_service_charge': sync_merchants_api_get,
    'Merchant_tip_suggestions': sync_direct_api_get,
    'Merchant_order_types': sync_direct_api_get,
    'Merchant_roles': sync_direct_api_get,
    'Merchant_tenders': sync_direct_api_get,
    'Orders_line_items': sync_orders_line_items,
    'Merchants': sync_merchants_api_get
}


SYNC_API = {
    'Orders': '/v3/merchants/{mId}/orders',
    'Merchant_address': '/v3/merchants/{mId}/address',
    'Merchant_opening_hours': '/v3/merchants/{mId}/opening_hours',
    'Customers': '/v3/merchants/{mId}/customers',
    'Employees': '/v3/merchants/{mId}/employees',
    'Inventory_items': '/v3/merchants/{mId}/items',
    'Inventory_item_stocks': '/v3/merchants/{mId}/item_stocks',
    'Inventory_tags': '/v3/merchants/{mId}/tags',
    'Inventory_tax_rates': '/v3/merchants/{mId}/tax_rates',
    'Inventory_item_groups': '/v3/merchants/{mId}/item_groups',
    'Inventory_categories': '/v3/merchants/{mId}/categories',
    'Inventory_modifier_groups': '/v3/merchants/{mId}/modifier_groups',
    'Inventory_modifiers': '/v3/merchants/{mId}/modifiers',
    'Inventory_attributes': '/v3/merchants/{mId}/attributes',
    'Refunds': '/v3/merchants/{mId}/refunds',
    'Payments': '/v3/merchants/{mId}/payments',
    'Merchant_gateway': '/v3/merchants/{mId}/gateway',
    'Employees_shifts': '/v3/merchants/{mId}/shifts',
    'Inventory_discounts': '/v3/merchants/{mId}/discounts',
    'Merchant_devices': '/v3/merchants/{mId}/devices',
    'Merchant_properties': '/v3/merchants/{mId}/properties',
    'Merchant_default_service_charge': '/v3/merchants/{mId}/default_service_charge',
    'Merchant_tip_suggestions': '/v3/merchants/{mId}/tip_suggestions',
    'Merchant_order_types': '/v3/merchants/{mId}/order_types',
    'Merchant_roles': '/v3/merchants/{mId}/roles',
    'Merchant_tenders': '/v3/merchants/{mId}/tenders',
    'Orders_line_items': '/v3/merchants/{mId}/orders/{orderId}/line_items',
    'Merchants': '/v3/merchants/{mId}'
}


def sync(config, _state, catalog):

    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog['streams']:

        if stream['schema'].get('selected', False):
            stream_id = stream['tap_stream_id']
            LOGGER.info("Syncing stream: %s", stream_id)
            sync_func = SYNC_FUNCTIONS[stream_id]
            api = SYNC_API[stream_id]
            final_rows, schemas, is_sync = sync_func(config, api)
            if is_sync:
                cf.singer_write(stream_id, final_rows, schemas)


def discover():

    """Run discovery mode"""

    schemas, schemas_metadata = get_schemas()
    streams = []

    for schema_name, schema in schemas.items():
        schema_meta = schemas_metadata[schema_name]

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata': schema_meta
        }
        streams.append(catalog_entry)

    return {'streams': streams}


@utils.handle_top_exception(LOGGER)
def main():

    """ Main function """

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))

    # Otherwise run in sync mode
    else:

        if args.properties:
            catalog = args.properties
        # 'catalog' is the current name
        elif args.catalog:
            catalog = args.catalog.to_dict()
        else:
            catalog = discover()

        state = args.state or {
            'bookmarks': {}
        }

        sync(args.config, state, catalog)


if __name__ == "__main__":
    main()
