import os

MOD_ROOT = os.path.abspath(__file__ + "/../../")
APP_ROOT = os.path.join(MOD_ROOT, 'rabdap')

config = {
    'LOG_DIR': os.path.join(APP_ROOT, 'logs'),
    'MONGO_USER' : '',
    'MONGO_PASSW': '',
    'MONGO_ADDR': '',
    'MONGO_DB' : '',
    'LDAP_SERVER': '',
    'LDAP_USER' : '',
    'LDAP_USERGROUP' : '',
    'LDAP_PASSWORD' : '',
}
