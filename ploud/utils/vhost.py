import os, sys
import bsddb
import psycopg2
import ptah
import ploud_config

file = '/opt/ploud/virtual-hosts.db'


def addVirtualHosts(hosts, env):
    db = bsddb.hashopen(file, 'c')
    for host in hosts:
        db[host] = env
    db.close()


def removeVirtualHosts(hosts):
    db = bsddb.hashopen(file, 'c')
    for host in hosts:
        if db.has_key(host):
            del db[host]
    db.close()


def rebuildVirtualHosts():
    ploud_config.initializeConfig()
    ploud = ploud_config.PLOUD_POOL.getconn()
    c1 = ploud.cursor()

    c1.execute("SELECT host FROM vhost")

    hosts = [row[0] for row in c1.fetchall()]

    try:
        os.unlink(file)
    except:
        pass
    addVirtualHosts(hosts, 'plone41')

    print '%d added.'%len(hosts)

    c1.close()
    ploud.close()


def initVirtualHosts():
    ploud_config.initializeConfig()
    ploud = ploud_config.PLOUD_POOL.getconn()
    c1 = ploud.cursor()

    PLOUD = ptah.get_settings('ploud')

    #c1.execute("DELETE FROM vhost")
    c1.execute("SELECT id, site_name FROM sites")
    for row in c1.fetchall():
        host = row[1].endswith('.%s'%PLOUD['domain']) \
            and row[1] or '%s.%s'%(row[1], PLOUD['domain'])
        stmt = "INSERT INTO vhost(id, host) VALUES(%d, '%s')"%(row[0], host)
        c1.execute(stmt)

    c1.close()
    ploud.commit()
    ploud.close()
