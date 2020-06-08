""" User define functions for Clover Tap """
# !/usr/bin/env python3
import json
import singer
from genson import SchemaBuilder
import pandas as pd


def clean_api_data(response):

    """ To maintain same schema in json, flatting and merging the schema for all rows """

    key = [*response][0]
    data_frame = pd.json_normalize(response[key])
    df_final = data_frame.fillna('None')
    final_rows = json.loads(df_final.to_json(orient='records'))
    schemas = get_json_schemas(final_rows)

    return final_rows, schemas


def get_json_schemas(json_data):

    """Return the standard json schema for given json"""

    builder = SchemaBuilder()
    builder.add_schema({"type": "object", "properties": {}})
    builder.add_object(json_data)
    api_schema = builder.to_schema()
    return api_schema


def singer_write(stream_name, json_data, schema):

    """ Write schema, records and state to singer """

    singer.write_schema(stream_name=stream_name, schema=schema, key_properties=[])
    singer.write_records(stream_name, json_data)
    singer.write_state({stream_name: 'Done'})
