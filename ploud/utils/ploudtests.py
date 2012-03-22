import sys, shutil
from datetime import datetime, timedelta
from ploud.utils import ploud_config, vhost


def createSites():
    email = sys.argv[1]
    prefix = sys.argv[2]
    count = sys.argv[3]
    if ':' in count:
        start, count = count.split(':')
        start, count = int(start), int(count)
    else:
        start = 0
        count = int(count)
    try:
        template = 'ploud%s'%int(sys.argv[4])
    except:
        template = 'template_plone41'

    config.initializeConfig()

    conn = config.PLOUD_POOL.getconn()
    cursor = conn.cursor()

    clients_conn = config.CLIENTS_POOL.getconn()
    clients_cursor = clients_conn.cursor()

    cursor.execute("SELECT id FROM users WHERE email=%s",((email,)))
    uid = cursor.fetchone()[0]

    total = timedelta()

    for id in range(start, start+count):
        name = '%s%0.5d'%(prefix, id)

        t = datetime.now()

        cursor.execute("INSERT INTO sites (id, site_name, typeof, user_id, size) "
                       "VALUES(NEXTVAL('sites_seq'), %s, 'plone41', %s, 0)", (name, uid))
        cursor.execute("SELECT id FROM sites WHERE site_name=%s",(name,))
        sid = cursor.fetchone()[0]

        hostname = '%s.ploud.com'%name
        vhost.addVirtualHosts((str(hostname),), 'plone41')
        cursor.execute("INSERT INTO vhost VALUES(%s, %s)",(sid, hostname))
        cursor.execute("COMMIT")

        # copy zodb
        clients_cursor.execute(
            """
        CREATE TABLE ploud%(id)s_blob_chunk
              (LIKE %(tp1)s_blob_chunk INCLUDING ALL);
        INSERT INTO ploud%(id)s_blob_chunk
              SELECT * FROM %(tp)s_blob_chunk;

        CREATE TABLE ploud%(id)s_commit_lock
              (LIKE %(tp1)s_commit_lock INCLUDING ALL);
        INSERT INTO ploud%(id)s_commit_lock
              SELECT * FROM %(tp)s_commit_lock;

        CREATE TABLE ploud%(id)s_object_state
              (LIKE %(tp1)s_object_state INCLUDING ALL);
        INSERT INTO ploud%(id)s_object_state
              SELECT * FROM %(tp)s_object_state;
        """ % {'id': sid, 'tp': template, 'tp1': 'template_plone41'})

        clients_cursor.execute("SELECT last_value FROM %s_zoid_seq"%template)
        clients_cursor.execute("CREATE SEQUENCE ploud%s_zoid_seq START WITH %s"%(
                sid, clients_cursor.fetchone()[0]))
        clients_cursor.execute("COMMIT")

        t1 = datetime.now() - t
        print 'Created "%s": '%name, t1
        total = total + t1


    print "Total time:", total
    clients_cursor.close()
    clients_conn.close()

    cursor.close()
    conn.close()
