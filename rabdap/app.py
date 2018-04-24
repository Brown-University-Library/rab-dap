import sys
import os
import csv
import argparse
import logging
import urllib
import datetime
import collections

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
    entry_data['created'] = datetime.datetime.now()
    entry_data['updated'] = entry_data['created']
    entry_data['historical'] = {}
    entry_data['rabid'] = 'http://vivo.brown.edu/individual/{}'.format(
        entry_data['shortid'])
    return entry_data

def merge_entries(oldEntry, futureEntry):
    merged = { k: v for k,v in futureEntry.items() }
    merged['created'] = oldEntry['created']
    historical = collections.defaultdict(list)
    for k,v in oldEntry['historical'].items():
        historical[k] = v
    del oldEntry['historical']
    for k,v in oldEntry.items():
        if oldEntry[k] != merged[k]:
            historical[k].insert(0, v)
    merged['historical'] = { k: v[:10] for k,v in historical.items() }
    return merged
    
# def check_rabdap_filters(filterData):
#     allowed_filters = [ 'id_filter', 'date_filter' ]
#     allowed_id_types = [ 'bruid', 'shortid', 'uuid', 'name', 'email' ]
#     allowed_date_types = [ 'updated', 'created' ]
#     recognized = { f: filterData[f] for f in filterData
#                         if f in allowed_filters }
#     mongo_filters = []
#     id_filter = recognized.get('id_filter')
#     date_filter = recognized.get('date_filter')
#     if id_filter and id_filter['id_type'] in allowed_id_types:
#         mongo_filters.append({ id_filter['id_type']: { 
#             '$in': [ v for v in id_filter['id_vals'] ]
#             } } )
#     if date_filter and date_filter['date_type'] in allowed_date_types:
#         date = datetime( date_filter['year'],
#             date_filter['month'], date_filter['day'])
#         mongo_filters.append(
#             { date_filter['date_type']: { '$lt': date } })
#     return mongo_filters[0]

def create_date_filter(month=0,year=0,day=0):
    default = datetime.datetime.now()
    date = datetime.datetime( year or default.year,
        month or default.month, day or default.day )
    return { 'updated': { '$lt': date } }

def create_rabdap_filter(filterType, kwargs):
    day = kwargs.get('day',0)
    month = kwargs.get('month',0)
    year = kwargs.get('year',0)
    date_filter = create_date_filter( int(month), int(year), int(day) )
    return date_filter    

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
    client_col = client_db[config['MONGO_COLL']]
    return client_col

# begin Database Queries

def get_rabdap_entry(mongoClient, idType, idVal):
    entry = mongoClient.find_one( { idType : idVal },
        {'_id': False, 'bruid': True,
        'rabid': True, 'shortid': True } )
    return entry

def create_rabdap_entry(ldapClient, mongoClient, idType, idVal):
    resp = ldapClient.search(idVal, idType)
    entry = cast_entry_data(resp[0])
    inserted = mongoClient.insert_one(entry)
    created = get_rabdap_entry(
        mongoClient, '_id', inserted.inserted_id)
    return created

def get_many_rabdap_entries(mongoClient, filterData):
    cursor = mongoClient.find(
        filterData, { '_id': False })
    return [ entry for entry in cursor ]

def get_ldap_entries(ldapClient, searchTerms, searchField):
    ldap_data = ldapClient.search(searchTerms, searchField )
    cast_data = [ cast_entry_data(l) for l in ldap_data ]
    return cast_data

def merge_entry_data(currentEntries, futureEntries, key):    
    future_index = { f[key]: f for f in futureEntries }
    matched_entries = [ ( c, future_index.get(c[key], None) )
        for c in currentEntries ]
    merged_entries =  [ merge_entries(m[0], m[1])
        for m in matched_entries ]
    return merged_entries

def update_rabdap_entries(ldapClient, mongoClient, currentEntries, key):
    future_entries = get_ldap_entries(ldapClient,
        [ e[ key ] for e in currentEntries ], key)
    updated_entries = merge_entry_data(currentEntries,
        future_entries, key)
    results = overwrite_rabdap_entries(
        mongoClient, updated_entries, key)
    return results

def overwrite_rabdap_entries(mongoClient, updatedEntries, key):
    results = []
    for e in updatedEntries:
        try:
            mongo_resp = mongoClient.replace_one({ key: e[key] }, e)
            results.append({e[key]: mongo_resp.modified_count})
        except:
            results.append({e[key]: 0})
    return results

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

@app.route('/regenerate/<filterType>', methods=['POST'])
def regenerate(filterType):
    rabdap_filter = create_rabdap_filter(filterType, request.args)
    mongo_client = get_mongo_client()
    existing_entries = get_many_rabdap_entries(mongo_client, rabdap_filter)
    ldap_client = get_ldap_client()
    update_results = update_rabdap_entries(
        ldap_client, mongo_client, existing_entries, 'bruid')
    return jsonify(update_results)


if __name__ == '__main__':
    app.run(host='0.0.0.0')
