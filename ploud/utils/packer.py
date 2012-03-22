import sys, shutil
from datetime import datetime
from ploud.utils import ploud_config, dbsize


tables = """
        CREATE TABLE object_ref (
            zoid        BIGINT NOT NULL,
            to_zoid     BIGINT NOT NULL,
            tid         BIGINT NOT NULL,
            PRIMARY KEY (zoid, to_zoid)
        );
        CREATE TABLE object_refs_added (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            tid         BIGINT NOT NULL
        );
        CREATE TABLE pack_object (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            keep        BOOLEAN NOT NULL,
            keep_tid    BIGINT NOT NULL,
            visited     BOOLEAN NOT NULL DEFAULT FALSE
        );
        CREATE INDEX pack_object_keep_false ON pack_object (zoid)
            WHERE keep = false;
        CREATE INDEX pack_object_keep_true ON pack_object (visited)
            WHERE keep = true;
"""

drop_tables = """
        DROP TABLE object_ref;
        DROP TABLE object_refs_added;
        DROP TABLE pack_object;"""

def pack():
    import ZODB
    from relstorage.options import Options
    from relstorage.storage import RelStorage
    from ploud.relstorage import local, PostgreSQLAdapter

    ploud_config.initializeConfig()

    conn = ploud_config.PLOUD_POOL.getconn()
    cursor = conn.cursor()

    clients_conn = ploud_config.CLIENTS_POOL.getconn()
    clients_cursor = clients_conn.cursor()

    options = Options(
        keep_history = False,
        blob_dir = '/tmp/ploud_pack_blobs',
        shared_blob_dir = False)
    dsn = "dbname=%(database)s user=%(user)s password=%(password)s "\
                "host=%(host)s "%ploud_config.CLIENTS_DSN

    try:
        shutil.rmtree('/tmp/ploud_pack_blobs')
    except:
        pass

    ids = []
    if (sys.argv) > 1:
        for id in sys.argv[1:]:
            try:
                ids.append(int(id))
            except:
                pass
    if ids:
        force = 1
        cursor.execute("SELECT id,site_name,packed,packed_size,size FROM sites WHERE id in (%s) ORDER BY id"%(str(ids)[1:-1]))
    else:
        force = 0
        cursor.execute("SELECT id,site_name,packed,packed_size,size FROM sites ORDER BY id")

    for row in cursor.fetchall():
        uid, name, packed, packed_size, size = row

        local.prefix = 'ploud%s_'%uid

        # pack if db size is more than 115% of packed db size
        if not force and packed_size and (size/(packed_size/100.0) < 115):
            print "Skiping '%s' %s"%(name, uid)
            continue

        clients_cursor.execute("DELETE FROM object_ref")
        clients_cursor.execute("DELETE FROM object_refs_added")
        clients_cursor.execute("DELETE FROM pack_object")
        clients_cursor.execute("COMMIT")

        print "Packing '%s' %s:"%(name, uid),
        t1 = datetime.now()
        pgadapter = PostgreSQLAdapter(
            ploud_config.CLIENTS_POOL, ploud_config.CLIENTS_POOL, dsn, options=options)
        storage = RelStorage(pgadapter, options=options)
        db = ZODB.DB(storage, database_name='main',
                     cache_size=15000, cache_byte_size=10485760)
        db.pack()
        db.close()
        storage.release()
        del storage
        del db
        del pgadapter
        psize = dbsize.dbsize(clients_cursor, uid)
        print "size: %0.2fmb was %0.2fmb %s"%(
            psize/(1024*1024.0), size/(1024*1024.0), str(datetime.now()-t1)[:-5])
        cursor.execute("UPDATE sites SET packed=%s, packed_size=%s, size=%s WHERE id=%s",
                       (datetime.now(), psize, psize, uid))
        cursor.execute('commit')
        try:
            shutil.rmtree('/tmp/ploud_pack_blobs')
        except:
            pass

    clients_cursor.close()
    ploud_config.CLIENTS_POOL.putconn(clients_conn)

    cursor.close()
    ploud_config.PLOUD_POOL.putconn(conn)
