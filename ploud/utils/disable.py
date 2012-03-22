import os, sys
import bsddb
import psycopg2
import ploud_config

file = '/opt/ploud/disabled-hosts.db'


def enableVhosts(hosts):
    db = bsddb.hashopen(file, 'c')
    for host in hosts:
        if db.has_key(host):
            del db[host]
    db.close()


def disableVhosts(hosts):
    db = bsddb.hashopen(file, 'c')
    for host in hosts:
        db[host] = '1'
    db.close()


def disableVhost():
    try:
        hostname = sys.argv[1]
    except:
        print "Hostname is required."
        return

    ploud_config.initializeConfig()
    ploud = ploud_config.PLOUD_POOL.getconn()
    c1 = ploud.cursor()

    c1.execute("SELECT id FROM vhost WHERE host = %s", (hostname,))
    try:
        id = c1.fetchone()[0]
    except:
        print "Can't find host name: %s"%hostname
        return

    c1.execute("UPDATE sites SET disabled = TRUE WHERE id = %s", (id,))
    c1.execute("SELECT host FROM vhost WHERE id = %s", (id,))
    hosts = [row[0] for row in c1.fetchall()]
    disableVhosts(hosts)

    c1.close()
    ploud.commit()
    ploud.close()
    print "Host is disabled."


def enableVhost():
    try:
        hostname = sys.argv[1]
    except:
        print "Hostname is required."
        return

    ploud = psycopg2.connect("dbname=ploud user=ploud password=12345 host=ploud-app1")
    c1 = ploud.cursor()

    c1.execute("SELECT id FROM vhost WHERE host = %s", (hostname,))
    try:
        id = c1.fetchone()[0]
    except:
        print "Can't find host name: %s"%hostname
        return

    c1.execute("UPDATE sites SET disabled = FALSE WHERE id = %s", (id,))
    c1.execute("SELECT host FROM vhost WHERE id = %s", (id,))
    hosts = [row[0] for row in c1.fetchall()]
    enableVhosts(hosts)

    c1.close()
    ploud.commit()
    ploud.close()
    print "Host is enabled."


def rebuildVhosts():
    ploud_config.initializeConfig()
    ploud = ploud_config.PLOUD_POOL.getconn()
    c1 = ploud.cursor()

    c1.execute("SELECT host FROM sites, vhost WHERE disabled = TRUE and sites.id = vhost.id")

    hosts = [row[0] for row in c1.fetchall()]

    try:
        os.unlink(file)
    except:
        pass
        
    disableVhosts(hosts)

    for host in hosts:
        print 'Disabled: %s'%host
    print '%d disabled.'%len(hosts)

    c1.close()
    ploud.close()
