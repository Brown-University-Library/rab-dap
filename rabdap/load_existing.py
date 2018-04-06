import sys
import json
import pymongo
from datetime import datetime

from config.settings import config

mongo = pymongo.MongoClient(config['MONGO_ADDR'])
id_db = mongo.get_database(config['RABDAP'])
id_coll = id_db['rabids']

def main(inFile):
    with open(inFile, 'r') as f:
        jdata = json.load(f)

    unique = { (d['brown_id'], d['short_id'], d['email']) 
        for bruid, d in jdata.items() }
    data = []
    for u in unique:
        id_map = {}
        id_map['bruid'] = u[0]
        id_map['shortid'] = u[1]
        id_map['email'] = u[2]
        id_map['rabid'] = 'http://vivo.brown.edu/individual/{}'.format(
            u[1])
        id_map['created'] = datetime.now()
        id_map['updated'] = datetime.now()
        id_map['historical'] = {}
        data.append(id_map)

    id_coll.insert_many(data)

if __name__ == '__main__':
    in_file = sys.argv[1]
    main(in_file)