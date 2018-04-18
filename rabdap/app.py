import sys
import os
import csv
import argparse
import logging
import urllib
from datetime import datetime

import pymongo
from flask import Flask, jsonify, current_app, request

from rabdap.utils import LdapClient
from config.settings import config

#Globals
app = Flask(__name__)
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

def check_rabdap_filters(filterData):
    allowed_filters = [ 'id_filter', 'date_filter' ]
    allowed_id_types = [ 'bruid', 'shortid', 'uuid', 'name', 'email' ]
    allowed_date_types = [ 'updated', 'created' ]
    recognized = { f: filterData[f] for f in filterData
                        if f in allowed_filters }
    mongo_filters = []
    id_filter = recognized.get('id_filter')
    date_filter = recognized.get('date_filter')
    if id_filter and id_filter['id_type'] in allowed_id_types:
        mongo_filters.append({ id_filter['id_type']: { 
            '$in': [ v for v in id_filter['id_vals'] ]
            } } )
    if date_filter and date_filter['date_type'] in allowed_date_types:
        date = datetime( date_filter['year'],
            date_filter['month'], date_filter['day'])
        mongo_filters.append(
            { date_filter['date_type']: { '$lt': date } })
    return mongo_filters[0]

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

def get_mongo_client():
    mongo_client = getattr(current_app, 'mongo_client', None)
    if mongo_client is None:
        mongo_client = pymongo.MongoClient(
            'mongodb://{0}:{1}@{2}/{3}'.format(config['MONGO_USER'],
                urllib.parse.quote_plus( config['MONGO_PASSW'] ),
                config['MONGO_ADDR'], config['MONGO_DB']) )
        current_app.mongo_client = mongo_client
    client_db = mongo_client.get_database(config['MONGO_DB'])
    return client_db

# begin Database Queries

def get_rabdap_entry(mongoClient, idType, idVal):
    entry = mongoClient['rabids'].find_one(
        { idType : idVal },
        {'_id': False, 'bruid': True,
        'rabid': True, 'shortid': True } )
    return entry

def create_rabdap_entry(ldapClient, mongoClient, idType, idVal):
    resp = ldapClient.search(idVal, idType)
    entry = cast_entry_data(resp[0])
    inserted = mongoClient['rabids'].insert_one(entry)
    created = get_rabdap_entry(
        mongoClient, '_id', inserted.inserted_id)
    return created

def get_many_rabdap_entries(mongoClient, filterData):
    good_filters = check_rabdap_filters(filterData)
    if good_filters:
        cursor = mongoClient['rabids'].find(
            good_filters, { '_id': False })
        return jsonify( [ e for e in cursor ] )
    return {}

# end Database Queries

@app.route('/get/<idType>/<idVal>', methods=['GET'])
def get(idType, idVal):
    mongo_client = get_mongo_client()
    entry = get_rabdap_entry(mongo_client, idType, idVal)
    return jsonify(entry)

@app.route('/gorc/<idType>/<idVal>', methods=['GET'])
def get_or_create(idType, idVal):
    mongo_client = get_mongo_client()
    entry = get_rabdap_entry(mongo_client, idType, idVal)
    if entry is None:
        logging.debug("Creating ID data for {0}:{1}".format(
            idType, idVal) )
        ldap_client = get_ldap_client()
        entry = create_rabdap_entry(
            ldap_client, mongo_client, idType, idVal)
    return jsonify(entry)

@app.route('/regenerate', methods=['POST'])
def regenerate():
    data_filters = request.get_json()
    mongo_client = get_mongo_client()
    entries = get_many_rabdap_entries(mongo_client, data_filters)
    return entries


if __name__ == '__main__':
    app.run(host='0.0.0.0')
