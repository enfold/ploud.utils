import sys, urllib
import psycopg2
from datetime import datetime

import ptah
import ploud_config, routing


def main():
    ploud_config.initializeConfig()
    APACHE = ptah.get_settings('apache')
    accesspos = APACHE['accesspos']
    pos = 0
    vpos = 0
    try:
        line = open(accesspos, 'rb').read()
        line = line.split(' ')
        pos = int(line[0])
        vpos = int(line[1])
    except:
        pass

    # process ploud-access file
    ploudlog = APACHE['ploudlog']
    logfile = open(ploudlog, 'rb')
    logfile.seek(0, 2)
    size = logfile.tell()

    if pos > size:
        pos = 0
        rotated = True
        logfile = open('%s.1'%ploudlog, 'rb')
    else:
        logfile.seek(pos)
        pos = size
        rotated = False

    data = {}
    accessed = {}
    for line in logfile.xreadlines():
        host = line.split(':', 1)[0]
        ac, bwin, bwout = line.split(':::')[-1].split(' ')

        rec = data.get(host)
        if rec is None:
            rec = [0, 0]
            data[host] = rec

        rec[0] = rec[0] + int(bwin)
        rec[1] = rec[1] + int(bwout)

        if ac == 'False':
            accessed[host] = 1

    logfile.close()

    # process varnish log
    varnishlog = APACHE['varnishlog']
    logfile = open(varnishlog, 'rb')
    logfile.seek(0, 2)
    size = logfile.tell()

    if vpos > size:
        vpos = 0
        logfile = open('%s.1'%varnishlog, 'rb')
    else:
        logfile.seek(vpos)
        vpos = size

    for line in logfile.xreadlines():
        try:
            info, other = line.split('HTTP/1.1"', 1)
        except:
            try:
                info, other = line.split('HTTP/1.0"', 1)
            except:
                pass

        try:
            code, size = other.strip().split(' ', 2)[:2]
            size = int(size)
        except:
            continue

        url = info.split(']', 1)[-1].strip().split(' ', 1)[-1]
        host = urllib.splithost(urllib.splittype(url)[1])[0]

        rec = data.get(host)
        if rec is None:
            rec = [0, 0]
            data[host] = rec

        rec[1] = rec[1] + size

    # save pos
    posfile = open(accesspos, 'wb')
    posfile.write('%s %s'%(pos, vpos))
    posfile.close()

    # save data to db
    ploud = ploud_config.PLOUD_POOL.getconn()
    cursor = ploud.cursor()

    for host, rec in data.items():
        cursor.execute("SELECT id FROM vhost WHERE host = %s", (host,))
        try:
            host_id = cursor.fetchone()[0]
        except:
            continue

        cursor.execute("SELECT bwin, bwout FROM sites WHERE id = %s", (host_id,))
        bwin, bwout = cursor.fetchone()

        cursor.execute("UPDATE sites SET bwin=%d, bwout=%d WHERE id=%d"%(
                bwin+rec[0], bwout+rec[1], host_id))
        print host, 'in: %0.2fK, out: %0.2fK'%(float(bwin+rec[0])/1024, float(bwout+rec[1])/1024)

    now = datetime.now()
    for host in accessed.keys():
        cursor.execute("UPDATE sites SET last_accessed=%s WHERE id in ("
                       "SELECT id FROM vhost WHERE host = %s)", (now, host))

    cursor.close()
    ploud.commit()
    ploud.close()


def clearBandwidth():
    ploud_config.initializeConfig()

    conn = ploud_config.PLOUD_POOL.getconn()
    cursor = conn.cursor()

    cursor.execute("UPDATE sites SET bwin=0, bwout=0")
    cursor.close()
    conn.commit()
    ploud_config.PLOUD_POOL.putconn(conn)
