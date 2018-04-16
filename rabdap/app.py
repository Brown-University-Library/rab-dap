import sys
import os
import csv
import argparse
import logging
from datetime import datetime

import pymongo
from flask import Flask, jsonify, current_app

from utils import LdapClient
from config.settings import config

#Globals
app = Flask(__name__)
mongo_cli = pymongo.MongoClient(config['MONGO_ADDR'])

logging.basicConfig(
    filename=os.path.join(config['LOG_DIR'],'dev.log'),
    format='%(asctime)-15s %(message)s',
    level=logging.DEBUG)

# begin Data Transformations

def unpack_ldap_data(ldapData):
    attrs = {
        'brownBruID' : 'bruid',
        'brownShortID' : 'shortid',
        'brownUUID' : 'uuid',
        'displayName' : 'name',
        'mail' :'email'
    }
    return { attrs[k]: v[0]
        for k,v in ldapData['attributes'].items() }

def cast_entry_data(ldapData):
    entry_data = unpack_ldap_data(ldapData)
    entry_data['created'] = datetime.now()
    entry_data['updated'] = datetime.now()
    entry_data['historical'] = {}
    entry_data['rabid'] = 'http://vivo.brown.edu/individual/{}'.format(
        entry_data['shortid'])
    return entry_data

# end Data Transformations

def get_ldap_client():
    ldap_client = getattr(current_app, 'ldap_client', None)
    if ldap_client is None:
        ldap_client = LdapClient(config)
        current_app.ldap_client = ldap_client
    if ldap_client.opened:
        ldap_client.reset()
    else:
        ldap_client.open()
    return ldap_client


# begin Database Queries

def get_rabdap_entry(id_type, id_val):
    rab_iddb = mongo_cli.get_database(config['RABDAP'])
    resp = rab_iddb['rabids'].find_one(
        { id_type : id_val },
        {'_id': False, 'bruid': True,
        'rabid': True, 'shortid': True } )
    return resp

def create_rabdap_entry(id_type, id_val):
    ldap_client = get_ldap_client()
    resp = ldap_client.search(id_val, id_type)
    entry = cast_entry_data(resp[0])
    rab_iddb = mongo_cli.get_database(config['RABDAP'])
    inserted = rab_iddb['rabids'].insert_one(entry)
    return inserted.inserted_id

# end Database Queries


@app.route('/get/<id_type>/<id_val>', methods=['GET'])
def get(id_type, id_val):
    entry = get_rabdap_entry(id_type, id_val)
    return jsonify(entry)

@app.route('/gorc/<id_type>/<id_val>', methods=['GET'])
def get_or_create(id_type, id_val):
    entry = get_rabdap_entry(id_type, id_val)
    if entry is None:
        logging.debug("Creating ID data for {0}:{1}".format(
            id_type, id_val) )
        rabdap_id = create_rabdap_entry(id_type, id_val)
        entry = get_rabdap_entry('_id', rabdap_id)
    return jsonify(entry)

@app.route('/regenerate', methods=['POST'])
def regenerate(id_type, id_val):
    pass


if __name__ == '__main__':
    app.run(host='0.0.0.0')