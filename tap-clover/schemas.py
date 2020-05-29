
""" Handles the schema for the APIs """

import os
import json
from singer import metadata

STREAMS = {
    'Orders': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Merchant_address': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Merchant_opening_hours': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Customers': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Employees': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Inventory_items': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Inventory_item_stocks': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Inventory_tags': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Inventory_tax_rates': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Inventory_item_groups': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Inventory_categories': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Inventory_modifier_groups': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Inventory_modifiers': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Inventory_attributes': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Refunds': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Payments': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Merchant_gateway': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Employees_shifts': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Inventory_discounts': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Merchant_devices': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Merchant_properties': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Merchant_default_service_charge': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Merchant_tip_suggestions': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Merchant_order_types': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Merchant_roles': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Merchant_tenders': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Orders_line_items': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    },
    'Merchants': {
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['id']
    }
}


def get_abs_path(path):
    """ returns the file path"""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_schemas():

    """ Reads the schemas for all the api's and returns stream sets """

    schemas = {}
    schemas_metadata = {}

    for stream_name, stream_metadata in STREAMS.items():

        schema_path = get_abs_path('schemas/{}.json'.format(stream_name))
        with open(schema_path) as file:
            schema = json.load(file)
        meta = metadata.get_standard_metadata(
            schema=schema,
            valid_replication_keys=stream_metadata.get('replication_keys', None),
            replication_method=stream_metadata.get('replication_method', None)
        )
        schemas[stream_name] = schema
        schemas_metadata[stream_name] = meta

    return schemas, schemas_metadata
