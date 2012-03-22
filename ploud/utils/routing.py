import os, sys
import bsddb, random
import ptah
import ploud_config


def addHosts(hosts, name=''):
    APACHE = ptah.get_settings('apache')
    file = APACHE['lbfile']
    processes = APACHE['processes']

    i = random.randint(1, processes)

    db = bsddb.hashopen(file, 'w')

    if name:
        db[str(name)] = str(i)

    for host in hosts:
        db[str(host)] = str(i)

    db.close()


def rebalance():
    ploud_config.initializeConfig()
    APACHE = ptah.get_settings('apache')
    file = APACHE['lbfile']
    processes = APACHE['processes']

    conn = ploud_config.PLOUD_POOL.getconn()
    c1 = conn.cursor()

    c1.execute("SELECT vhost.host,sites.bwin,sites.bwout,sites.site_name FROM vhost, sites "
               "WHERE vhost.id = sites.id and sites.disabled = %s ORDER by sites.id",(False,))

    db = bsddb.hashopen(file, 'w')

    data = [(bwin+bwout, host, name) for host, bwin, bwout, name in c1.fetchall()]
    data.sort()

    i = 1
    for size, host, name in data:
        db[host] = str(i)
        db[name] = str(i)
        i = i + 1
        if i > processes:
            i = 1

    print 'Rebalancing is done.'

    db.close()

    c1.close()
    conn.close()
