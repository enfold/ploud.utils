""" ploud utils config """
import psycopg2.pool
from pyramid.config import Configurator

import ptah
from ptah import config

CONFIGFILE = '/opt/ploud/ploud.conf'

PLOUD_POOL = None
CLIENTS_POOL = None

PYRAMID_CONFIG = None

ptah.register_settings(
    'ploud',

    ptah.form.TextField(
        'domain',
        title = 'Ploud Domain',
        description = 'Ploud domain name for hosting.',
        default = ''),

    ptah.form.TextField(
        'dsn',
        title = 'Ploud DSN',
        description = 'Main ploud database dsn.',
        default = ''),

    ptah.form.TextField(
        'clientsdsn',
        title = 'Ploud Client DSN',
        description = 'Ploud clients zodb database dsn.',
        default = ''),

    ptah.form.BoolField(
        'registration',
        title = 'Registration',
        description = 'Enable ploud registrations.',
        default = True),

    ptah.form.TextField(
        'loginservice',
        title = 'Login service url',
        default = 'http://localhost:8085'),

    ptah.form.BoolField(
        'maintenance',
        title = 'Maintenance',
        description = 'Enable maintenance worker.',
        default = False),

    title = 'Ploud settings',
)

ptah.register_settings(
    'apache',

    ptah.form.IntegerField(
        'processes',
        title = 'Ploud processes',
        description = 'Number of available ploud processes.',
        default = 4),

    ptah.form.TextField(
        'lbfile',
        title = 'LB file',
        description = 'Path to load balancer file for apache.',
        default = '/opt/ploud/lb.db'),

    ptah.form.TextField(
        'host',
        title = 'Apache host',
        description = 'Apache host name for direct access.',
        default = 'localhost:82'),

    ptah.form.TextField(
        'accesspos',
        title = 'Apache log access pos',
        default = '/opt/ploud/access.pos'),

    ptah.form.TextField(
        'ploudlog',
        default = '/var/log/apache2/ploud-access.log'),

    ptah.form.TextField(
        'varnishlog',
        default = '/var/log/varnish/varnishncsa.log'),

    title = 'Ploud apache settings',
)


def initializeConfig(settings, filepath=None, nodb=False):
    if filepath is None:
        filepath = CONFIGFILE
    settings['include'] = filepath

    print settings

    cfg = Configurator(settings=settings)
    cfg.include('ptah')
    cfg.include('ploud.utils')
    cfg.commit()
    cfg.begin()

    cfg.ptah_init_settings()
    cfg.commit()

    global PYRAMID_CONFIG
    PYRAMID_CONFIG = cfg


@config.subscriber(ptah.events.SettingsInitialized)
def initialized(ev):
    global PLOUD_DSN, CLIENTS_DSN
    global PLOUD_POOL, CLIENTS_POOL
    def parse_dsn(s):
        params = {}
        s = s.split('//', 1)[-1]

        head, tail = s.split('@', 1)
        user, passwd = head.split(':', 1)
        host, dbname = tail.split('/', 1)

        params['user'] = user
        params['password'] = passwd
        params['host'] = host
        params['database'] = dbname
        return params

    PLOUD = ptah.get_settings('ploud', ev.registry)
    PLOUD_DSN = parse_dsn(PLOUD['dsn'])
    CLIENTS_DSN = parse_dsn(PLOUD['clientsdsn'])

    # Init ploud connections pool
    PLOUD_POOL = psycopg2.pool.ThreadedConnectionPool(0, 8, **PLOUD_DSN)
    CLIENTS_POOL = psycopg2.pool.ThreadedConnectionPool(0, 10, **CLIENTS_DSN)
