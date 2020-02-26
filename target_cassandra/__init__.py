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
from typing import Sequence

import pkg_resources
from jsonschema.validators import Draft4Validator
import singer

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
}


def create_model_from_schema(schema: object) -> Model:
    """ Create an class derived from cassandra.cqlengine.models.Models, based on the provided schema """
    pass


def process_schema(table_name: str, schema: dict, key_properties: Sequence[str]) -> Model:
    model_attrs = {}
    for property_key, property_definition in schema['properties'].items():
        property_type = property_definition.get('type')

        if type(property_type) == list:
           # remove 'null' from type definition. All columns are nullable by default in Cassandra
            property_type = [pt for pt in property_type if pt != 'null']
            assert len(property_type) == 1

            model_column_class = JSONSCHEMA_TO_CASSANDRA.get(property_type[0])

            # TODO handle `object` class
            if model_column_class is None:
                continue

            assert model_column_class is not None

            model_attrs[property_key] = model_column_class(
                primary_key=property_key in key_properties)

        elif type(property_type) == str:
            model_column_class = JSONSCHEMA_TO_CASSANDRA.get(property_type)
            assert model_column_class is not None

            model_attrs[property_key] = model_column_class(
                primary_key=property_key in key_properties)

        elif property_type is None and 'anyOf' in property_definition:
            property_type = property_definition['anyOf']

            pass

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
            mapped_model.create(**{k: o['record'][k]
                                   for k in defined_column_names[o['stream']]})

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

    connection.setup(config['contact_points'], 'mykeyspace', protocol_version=3, auth_provider=PlainTextAuthProvider(
        username=config['username'], password=config['password']))

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_lines(config, input)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
