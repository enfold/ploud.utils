import sys, os, os.path, shutil
from datetime import datetime
from ploud.utils import ploud_config


def dumpdb():
    import ZODB.FileStorage
    from relstorage.options import Options
    from relstorage.storage import RelStorage
    from ploud.relstorage import local, PostgreSQLAdapter

    ploud_config.initializeConfig()

    name = sys.argv[1]
    conn = ploud_config.PLOUD_POOL.getconn()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM sites WHERE site_name=%s",(name,))
    sid = cursor.fetchone()[0]
    cursor.close()
    ploud_config.PLOUD_POOL.putconn(conn)

    local.prefix = 'ploud%s_'%sid
    dest = sys.argv[2]

    print "Dump ploud site: %s"%sys.argv[1]

    fdir = os.path.join(dest, 'filestorage')
    bdir = os.path.join(dest, 'blobstorage')
    try:
        os.makedirs(fdir)
    except:
        pass
    try:
        os.makedirs(bdir)
    except:
        pass
    try:
        shutil.rmtree('/tmp/ploud_dump_blobs')
    except:
        pass

    t = datetime.now()

    options = Options(
        keep_history = False,
        blob_dir = '/tmp/ploud_dump_blobs',
        shared_blob_dir = False)
    dsn = "dbname=%(database)s user=%(user)s password=%(password)s "\
                "host=%(host)s "%ploud_config.CLIENTS_DSN
    pgadapter = PostgreSQLAdapter(
        ploud_config.CLIENTS_POOL, ploud_config.CLIENTS_POOL, dsn, options=options)
    source = RelStorage(pgadapter, options=options)

    destination = ZODB.FileStorage.FileStorage(
        os.path.join(fdir, 'Data.fs'), blob_dir = bdir)

    destination.copyTransactionsFrom(source)
    source.close()
    destination.close()
    try:
        shutil.rmtree('/tmp/ploud_dump_blobs')
    except:
        pass
    print "Done in %s"%(datetime.now()-t)


def loaddb():
    import ZODB.FileStorage
    from relstorage.options import Options
    from relstorage.storage import RelStorage
    from ploud.relstorage import local, PostgreSQLAdapter

    ploud_config.initializeConfig()

    sid = sys.argv[1]
    fdir = sys.argv[2]
    bdir = sys.argv[3]

    conn = ploud_config.PLOUD_POOL.getconn()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM sites WHERE id=%s",(sid,))
    rec = cursor.fetchone()
    cursor.close()
    ploud_config.PLOUD_POOL.putconn(conn)

    if rec is None:
        print "Can't find site id:", sid
        return 

    local.prefix = 'ploud%s_'%sid
    dest = sys.argv[2]

    try:
        shutil.rmtree('/tmp/ploud_dump_blobs')
    except:
        pass

    t = datetime.now()

    options = Options(
        keep_history = False,
        blob_dir = '/tmp/ploud_dump_blobs',
        shared_blob_dir = False)
    dsn = "dbname=%(database)s user=%(user)s host=%(host)s "%ploud_config.CLIENTS_DSN
    pgadapter = PostgreSQLAdapter(
        ploud_config.CLIENTS_POOL, ploud_config.CLIENTS_POOL, dsn, options=options)
    pgadapter.schema.drop_all()
    destination = RelStorage(pgadapter, options=options)

    source = ZODB.FileStorage.FileStorage(os.path.join(fdir), blob_dir = bdir)

    destination.copyTransactionsFrom(source)
    source.close()
    destination.close()
    try:
        shutil.rmtree('/tmp/ploud_dump_blobs')
    except:
        pass
    print "Done in %s"%(datetime.now()-t)
