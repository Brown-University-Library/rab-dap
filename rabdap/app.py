import sys
import csv
import argparse
import logging
from datetime import datetime

import pymongo
from flask import Flask, jsonify

from utils import LdapClient
from config.settings import config

#Globals
app = Flask(__name__)
mongo_cli = pymongo.MongoClient(config['MONGO_ADDR'])
ldap_cli = LdapClient(config)

logging.basicConfig(
    # filename=os.path.join(config['LOG_DIR'],'example.log'),
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

# begin Database Queries

def get_rabdap_entry(id_type, id_val):
    rab_iddb = mongo_cli.get_database(config['RABDAP'])
    resp = rab_iddb['rabids'].find_one(
        { id_type : id_val },
        {'_id': False, 'bruid': True,
        'rabid': True, 'shortid': True } )
    return resp

def create_rabdap_entry(id_type, id_val):
    if ldap_cli.opened:
        logging.debug("Open LDAP connection, resetting")
        ldap_cli.reset()
    else:
        logging.debug("No LDAP connection, opening")        
        ldap_cli.open()
    resp = ldap_cli.search(id_val, id_type)
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
    logging.debug("Getting ID data")
    entry = get_rabdap_entry(id_type, id_val)
    if entry is None:
        logging.debug("Local ID doesn't exist, creating")
        rabdap_id = create_rabdap_entry(id_type, id_val)
        entry = get_rabdap_entry('_id', rabdap_id)
    return jsonify(entry)

@app.route('/regenerate', methods=['POST'])
def regenerate(id_type, id_val):
    pass


if __name__ == '__main__':
    app.run(host='0.0.0.0')