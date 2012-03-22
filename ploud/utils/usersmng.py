import sys, os, os.path
from datetime import datetime

from ploud.utils import ploud_config
from ploud.utils.policy import POLICIES


def listUsers():
    ploud_config.initializeConfig()

    try:
        srch = sys.argv[1]
    except:
        srch = ''

    conn = ploud_config.PLOUD_POOL.getconn()
    cursor = conn.cursor()

    if not srch:
        cursor.execute("SELECT id, email FROM users ORDER BY id")
    else:
        cursor.execute("SELECT id, email FROM users WHERE email LIKE %s ORDER BY id",(srch,))

    rows = cursor.fetchall()
    for row in rows:
        print '%s: %s'%row

    print "Total found: %s"%len(rows)
    cursor.close()
    ploud_config.PLOUD_POOL.putconn(conn)


def waitingList():
    ploud_config.initializeConfig()

    try:
        srch = sys.argv[1]
    except:
        srch = ''

    conn = ploud_config.PLOUD_POOL.getconn()
    cursor = conn.cursor()

    if not srch:
        cursor.execute("SELECT email, requested FROM waitinglist WHERE completed = FALSE ORDER BY id")
    else:
        cursor.execute("SELECT email, requested FROM waitinglist "
                       "WHERE email LIKE %s and completed = FALSE ORDER by id ",(srch,))

    rows = cursor.fetchall()
    for row in rows:
        print '%s: %s'%row

    print "Total found: %s"%len(rows)
    cursor.close()
    ploud_config.PLOUD_POOL.putconn(conn)


def enableUser():
    email = sys.argv[1]
    ploud_config.initializeConfig()

    conn = ploud_config.PLOUD_POOL.getconn()
    cursor = conn.cursor()

    cursor.execute("SELECT email FROM waitinglist "
                   "WHERE completed = %s and email=%s",(False,email))
    
    row = cursor.fetchone()
    if row is None:
        print "Can't find email: %s"%email
        return

    print row

    cursor.close()
    conn.commit()


def userInfo():
    ploud_config.initializeConfig()

    id = sys.argv[1]

    conn = ploud_config.PLOUD_POOL.getconn()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM users WHERE id = %s", (id,))
    rec = cursor.fetchone()
    if rec is None:
        print "User id is not found."
        return

    print 'User:', id
    print 'Email:', rec[1]
    print 'Joined:', rec[3]
    print 'Policy:', rec[-1]

    policy = POLICIES[rec[-1]]

    cursor.execute("SELECT id,site_name,typeof,size,bwin+bwout,disabled,last_accessed FROM sites "
                   "WHERE user_id = %s and removed = FALSE order by site_name", (id,))
    data = cursor.fetchall()

    print 'Number of sites:', len(data)
    
    size = 0 
    bandwidth = 0
    for site in data:
        size += site[3]
        bandwidth += site[4]

    print 'Database size: %0.2fMb (%0.1f%%)'%(size/1048576.0, size / (policy.dbsize/100.0))
    print 'Bandwidth: %0.2fMb (%0.1f%%)'%(bandwidth/1048576.0, bandwidth / (policy.bandwidth/100.0))
    print '--------------------------------------------------'

    for site in data:
        print 'Site id/type: %s/%s'%(site[0], site[2])
        print 'Site name:', site[1]
        print 'Last accessed:', site[6]
        print 'Disabled:', site[5]
        print 'Database size: %0.2fMb (%0.1f%%)'%(site[3]/1048576.0, site[3] / (size/100.0))
        print 'Bandwidth: %0.2fMb (%0.1f%%)'%(site[4]/1048576.0, site[4] / (bandwidth/100.0))
        
        print '================================================='

    cursor.close()
    ploud_config.PLOUD_POOL.putconn(conn)
