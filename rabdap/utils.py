import json
import time
import ldap3

class LdapClient:
    def __init__(self, cfg):
        self.throttle = cfg['LDAP_THROTTLE']
        self.ldap_attrs = [ 'brownBruID','brownShortID',
            'brownUUID','displayName','mail']
        self.server = ldap3.Server(cfg['LDAP_SERVER'])
        self.conn = ldap3.Connection(
            self.server,
            'cn={0},ou={1},dc=brown,dc=edu'.format(
                cfg['LDAP_USER'], cfg['LDAP_USERGROUP']),
            cfg['LDAP_PASSWORD'])
        self.opened = False
        self.closed = True

    def open(self):
        self.conn.bind() 
        self.opened = self.conn.bound
        self.closed = self.conn.closed

    def close(self):
        self.conn.unbind()
        self.opened = self.conn.bound
        self.closed = self.conn.closed

    def search(self, searchTerms, field='bruid'):
        field_map = {
            'bruid' : 'brownbruid',
            'shortid' : 'brownshortid',
            'uuid' : 'brwonuuid',
            'name' : 'displayname',
            'email' : 'mail'
        }
        try:
            search_field = field_map[field]
        except:
            raise("Unrecognized LDAP field")
        if isinstance(searchTerms, list):
            formatted = [ '({0}={1})'.format(search_field, s)
                for s in searchTerms ]
            chunked = chunk_list(formatted, 100)
            query = [ '(|{0})'.format(''.join(c)) for c in chunked ]
        else:
            query = [ '({0}={1})'.format(search_field, searchTerms) ]
        ldap_data = []
        for q in query:
            time.sleep(self.throttle)
            resp = self.conn.search('ou=people,dc=brown,dc=edu',
                        q, attributes=self.ldap_attrs)
            if resp == True:
                entries = [ json.loads( e.entry_to_json() )
                            for e in self.conn.entries ]
                ldap_data.extend(entries)
            else:
                continue
        return ldap_data

def chunk_list(lst, size):
    chunked = []
    for i in range(0, len(lst), size):
        chunked.append( lst[i:i + size] )
    return chunked