import sys
import csv
import argparse
from datetime import datetime

import pymongo
from flask import Flask, jsonify

from utils import LdapClient
from config.settings import config

#Globals
app = Flask(__name__)
mongo_cli = pymongo.MongoClient(config['MONGO_ADDR'])
ldap_cli = LdapClient(config)

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

def cast_entry(ldapData):
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

def create_rabdap_entry(bruid):
    ldap_cli.open()
    resp = ldap_cli.search_bruids([bruid])
    ldap_cli.close()
    entry = cast_entry(resp[0])
    rab_iddb = mongo_cli.get_database(config['RABDAP'])
    inserted = rab_iddb['rabids'].insert_one(entry)
    return inserted.inserted_id

# end Database Queries


@app.route('/get/<id_type>/<id_val>', methods=['GET'])
def get(id_type, id_val):
    entry = get_rabdap_entry(id_type, id_val)
    return jsonify(entry)

@app.route('/getorcreate/bruid/<bruid>', methods=['GET'])
def get_or_create(bruid):
    entry = get_rabdap_entry('bruid', bruid)
    if entry is None:
        rabdap_id = create_rabdap_entry(bruid)
        entry = get_rabdap_entry('_id', rabdap_id)
    return jsonify(entry)

@app.route('/regenerate', methods=['POST'])
def regenerate(id_type, id_val):
    pass


if __name__ == '__main__':
    app.run(host='0.0.0.0')