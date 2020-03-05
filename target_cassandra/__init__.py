#!/usr/bin/env python3

import argparse
import io
import os
import sys
import json
import threading
import http.client
import urllib
from datetime import datetime
import collections
from typing import Sequence, Union

from jsonschema.validators import Draft4Validator
import singer
import dateutil.parser

from cassandra.auth import PlainTextAuthProvider
from cassandra.cqlengine import columns
from cassandra.cqlengine import connection
from datetime import datetime
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model

logger = singer.get_logger()


JSONSCHEMA_TO_CASSANDRA = {
    'string': columns.Text,
    'array': None,
    'boolean': columns.Boolean,
    'integer': columns.Integer,
    'number': columns.Float,
    'null': None,
    'object': None,
}

# "typecast" values from JSON records to values suitable for a mapped model
# by default, use the identity function
JSONVALUE_TO_CASSANDRA = collections.defaultdict(lambda: (lambda v: v))
JSONVALUE_TO_CASSANDRA[columns.DateTime] = lambda v: None if v is None else dateutil.parser.parse(
    v)


def jsonschema_to_cassandra(jsonschema_definition: dict) -> columns.Column:
    # if type 'anyOf', we need to choose one
    # TODO how to make this generic enough?
    # For now, we take the first def
    if 'anyOf' in jsonschema_definition:
        return jsonschema_to_cassandra(jsonschema_definition['anyOf'][0])

    jsonschema_type = jsonschema_definition['type']
    jsonschema_additional_props = {
        k: v for k, v in jsonschema_definition.items() if k != 'type'}

    if type(jsonschema_type) == str:
        assert jsonschema_type in JSONSCHEMA_TO_CASSANDRA
        format_def = jsonschema_additional_props.get('format')
        if jsonschema_type == 'string' and format_def == 'date-time':
            return columns.DateTime
        else:
            return JSONSCHEMA_TO_CASSANDRA[jsonschema_type]
    elif type(jsonschema_type) == list:
        # remove 'null' from type definition. All columns are nullable by default in Cassandra
        property_type = [pt for pt in jsonschema_type if pt != 'null']
        assert(len(property_type) == 1)
        return jsonschema_to_cassandra({'type': property_type[0], **jsonschema_additional_props})


def process_schema(table_name: str, schema: dict, key_properties: Sequence[str]) -> Model:
    model_attrs = {}
    for property_key, property_definition in schema['properties'].items():
        property_type = property_definition.get('type')
        model_column_class = jsonschema_to_cassandra(property_definition)
        if model_column_class is None:
            continue
        model_attrs[property_key] = model_column_class(
            primary_key=property_key in key_properties)

    # yay metaprogramming
    return type("%sModel" % table_name, (Model,), model_attrs)


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def flatten(d, parent_key='', sep='__'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)


def persist_lines(config, lines):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}
    mapped_models = {}
    defined_column_names = {}

    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    # Loop over lines from stdin
    for line in lines:
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if 'type' not in o:
            raise Exception(
                "Line is missing required key 'type': {}".format(line))
        t = o['type']

        if t == 'RECORD':
            if 'stream' not in o:
                raise Exception(
                    "Line is missing required key 'stream': {}".format(line))
            if o['stream'] not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(o['stream']))

            # Get schema for this record's stream
            schema = schemas[o['stream']]

            # Validate record
            validators[o['stream']].validate(o['record'])

            # If the record needs to be flattened, uncomment this line
            # flattened_record = flatten(o['record'])

            # Process Record message here..
            mapped_model = mapped_models[o['stream']]
            # build record dict
            record = {}
            for column_name in defined_column_names[o['stream']]:
                typecaster = JSONVALUE_TO_CASSANDRA[type(
                    mapped_model._get_column(column_name))]
                record[column_name] = typecaster(o['record'][column_name])

            model_instance = mapped_model(**record)
            model_instance.save()

            state = None
        elif t == 'STATE':
            logger.debug('Setting state to {}'.format(o['value']))
            state = o['value']
        elif t == 'SCHEMA':
            if 'stream' not in o:
                raise Exception(
                    "Line is missing required key 'stream': {}".format(line))
            stream = o['stream']
            schemas[stream] = o['schema']
            validators[stream] = Draft4Validator(o['schema'])
            if 'key_properties' not in o:
                raise Exception("key_properties field is required")
            key_properties[stream] = o['key_properties']
            mapped_models[stream] = process_schema(stream, schemas[stream],
                                                   o.get('key_properties'))
            # sync model to Cassandra
            sync_table(mapped_models[stream])

            # list of defined columns
            defined_column_names[stream] = set(
                mapped_models[stream]._columns.keys())

        elif t == 'ACTIVATE_VERSION':
            logger.debug("TODO: implement support for ACTIVATE_VERSION?")
        else:
            raise Exception("Unknown message type {} in message {}"
                            .format(o['type'], o))

    return state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input:
            config = json.load(input)
            # TODO: validate config
    else:
        config = {}

    connection.setup(config['contact_points'], config['keyspace'], protocol_version=3, auth_provider=PlainTextAuthProvider(
        username=config['username'], password=config['password']))

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_lines(config, input)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
