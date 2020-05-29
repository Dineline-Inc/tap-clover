""" User define functions for Clover Tap """
# !/usr/bin/env python3
import json
import collections
import singer
from genson import SchemaBuilder


def flatten(data, parent_key='', sep='__'):

    """ Return flatted json from nested json """

    items = []
    collections_abc = collections.abc
    for key, val in data.items():
        new_key = parent_key + sep + key if parent_key else key
        if isinstance(val, collections_abc.MutableMapping):
            items.extend(flatten(val, new_key, sep=sep).items())
        else:
            items.append((new_key, str(val) if isinstance(val, list) else val))
    return dict(items)


def clean_api_data(response):

    """ To maintain same schema in json, flatting and merging the schema for all rows """

    key = [*response][0]
    rows = response[key]
    flatted_rows = []

    for row in rows:
        flatted_rows.append(flatten(row))

    columns = get_columns(flatted_rows)
    final_rows = merge_schema(flatted_rows, columns)
    schemas = get_json_schemas(final_rows)
    return final_rows, schemas


def get_columns(rows):

    """get common columns for all the rows"""

    columns = []
    for row in rows:
        columns = columns + list(row.keys() - columns)
    return columns


def get_json_schemas(json_data):

    """Return the standard json schema for given json"""

    builder = SchemaBuilder()
    builder.add_schema({"type": "object", "properties": {}})
    builder.add_object(json_data)
    api_schema = builder.to_schema()
    return api_schema


def merge_schema(rows, columns):

    """ add the empty values in missing columns for each row """

    new_rows = []
    for row in rows:
        for col in columns:
            if col not in row.keys():
                row[col] = ''
            if isinstance(row[col], bool):
                row[col] = str(row[col]).lower()
        json_data = json.dumps(row, indent=2, sort_keys=True)
        new_rows.append(json.loads(json_data))
    return new_rows


def singer_write(stream_name, json_data, schema):

    """ Write schema, records and state to singer """

    singer.write_schema(stream_name=stream_name, schema=schema, key_properties=[])
    singer.write_records(stream_name, json_data)
    singer.write_state({stream_name: 'Done'})
