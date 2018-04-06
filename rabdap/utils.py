import json
import time
import ldap3

class LdapClient:
    def __init__(self, cfg):
        self.throttle = cfg['LDAP_THROTTLE']
        self.server_addr = cfg['LDAP_SERVER']
        self.user = cfg['LDAP_USER']
        self.passw = cfg['LDAP_PASSWORD']
        self.user_grp = cfg['LDAP_USERGROUP']
        self.ldap_attrs = [ 'brownBruID','brownShortID',
            'brownUUID','displayName','mail']
        self.server = ldap3.Server(self.server_addr)
        self.conn = None
        self.is_open = False
        self.is_closed = True

    def open(self):
        self.conn = ldap3.Connection(
            self.server,
            'cn={0},ou={1},dc=brown,dc=edu'.format(
                self.user, self.user_grp),
            self.passw, auto_bind=True)
        self.is_open = True
        self.is_closed = False

    def close(self):
        self.conn.unbind()
        self.is_open = False
        self.is_closed = True

    def search_bruids(self, bruids):
        formatted = [ '(brownbruid={0})'.format(b) for b in bruids ]
        chunked = chunk_list(formatted, 100)
        ldap_data = []
        for chunk in chunked:
            time.sleep(self.throttle)
            print('Querying')
            or_str = '(|{0})'.format(''.join(chunk))
            resp = self.conn.search('ou=people,dc=brown,dc=edu',
                        or_str,
                        attributes=self.ldap_attrs)
            if resp == True:
                entries = [ json.loads( e.entry_to_json() )
                            for e in self.conn.entries ]
                ldap_data.extend(entries)
            else:
                continue
        return ldap_data

# begin Data transformations
def chunk_list(lst, size):
    chunked = []
    for i in range(0, len(lst), size):
        chunked.append( lst[i:i + size] )
    return chunked