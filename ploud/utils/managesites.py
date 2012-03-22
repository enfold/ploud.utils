import sys, os, os.path
from datetime import datetime

from ploud.utils import ploud_config
from ploud.utils.policy import POLICIES


def listSites():
    ploud_config.initializeConfig()

    try:
        srch = sys.argv[1]
    except:
        srch = ''

    conn = ploud_config.PLOUD_POOL.getconn()
    cursor = conn.cursor()

    cursor.execute("SELECT id, email FROM users")
    users = dict([(id, email) for id, email in cursor.fetchall()])

    if not srch:
        cursor.execute("SELECT id, site_name, user_id, bwin+bwout, size, last_accessed "
                       "FROM sites ORDER BY id")
    else:
        cursor.execute("SELECT id, site_name, user_id, bwin+bwout, size, last_accessed "
                       "FROM sites WHERE site_name LIKE %s ORDER BY id",(srch,))

    bwsize = 0
    dbsize = 0
    total = 0

    for id, site_name, user_id, bw, size, last_accessed in cursor.fetchall():
        total += 1
        bwsize += bw
        dbsize += size

        cursor.execute("SELECT host FROM vhost WHERE id = %s",(id,))
        vhosts = [row[0] for row in cursor.fetchall()]

        print '%0.5d: '%id,
        print 'db: %3.2f Mb | '%(size/1048576.0),
        print 'bw: %0.2f Mb | '%(bw/1048576.0),
        try:
            print ' %s | '%last_accessed.strftime("%d/%m/%y %H:%M"),
        except:
            print ' date is not set | ',
        print '%s | %s | '%(users[user_id], site_name),
        print ', '.join(vhosts)

    print "Total: %s"%total
    print 'Bandwidth:', '%0.2f Mb'%(bwsize/1048576.0)
    print 'Database size:', '%0.2f Mb'%(dbsize/1048576.0)
    cursor.close()
    ploud_config.PLOUD_POOL.putconn(conn)


def cleanupRemoved():
    ploud_config.initializeConfig()

    conn = ploud_config.PLOUD_POOL.getconn()
    cursor = conn.cursor()
    

    cursor.execute('SELECT id, site_name FROM sites WHERE removed = TRUE ORDER by id')
    for sid, site_name in cursor.fetchall():
        print 'Removeing %s(%s)...'%(sid, site_name),
        
        clients = ploud_config.CLIENTS_POOL.getconn()
        c = clients.cursor()
        for tb in ('blob_chunk', 'commit_lock', 'object_state'):
            try:
                c.execute('DROP TABLE ploud%s_%s CASCADE'%(sid, tb))
            except:
                pass
        
        try:
            c.execute('DROP SEQUENCE ploud%s_zoid_seq'%sid)
        except:
            pass
        c.close()
        clients.commit()
        ploud_config.CLIENTS_POOL.putconn(clients)
        
        cursor.execute("DELETE FROM sites WHERE id = %s",(sid,))
        print ' removed'

    cursor.close()
    conn.commit()
    ploud_config.PLOUD_POOL.putconn(conn)
