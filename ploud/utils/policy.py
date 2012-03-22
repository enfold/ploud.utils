"""

$Id:  2007-12-12 12:27:02Z fafhrd $
"""
import sys
import bsddb
import ptah
import ploud_config, vhost, routing

POLICIES = {}
file = '/opt/ploud/policy.db'


class Policy(object):

    def __init__(self, id, sites, clones,
                 bandwidth, dbsize, bots, vhosts, filesize,
                 removes, transfers, title='', description=''):
        self.id = id
        self.sites = sites
        self.clones = clones
        self.dbsize = dbsize
        self.bandwidth = bandwidth
        self.bots = bots
        self.vhosts = vhosts
        self.filesize = filesize
        self.removes = removes
        self.transfers = transfers
        self.title = title
        self.description = description

    def apply(self, uid, cursor):
        cursor.execute("SELECT id, site_name FROM sites WHERE user_id = %s", (uid,))
        for row in cursor.fetchall():
            self.removeSite(row[0], cursor)
            self.addSite(row[0], row[1], 'plone41', cursor)

    def unapply(self, uid, cursor):
        cursor.execute("SELECT id FROM sites WHERE user_di = %s", (uid,))
        for row in cursor.fetchall():
            self.removeSite(row[0], cursor)

    def changeHostsPolicy(self, hosts, name):
        changePolicy(hosts, self.id)
        routing.addHosts(hosts, name)

    def addSite(self, sid, name, env, cursor):
        # add virtual host
        PLOUD = ptah.get_settings('ploud')
        cursor.execute("SELECT host FROM vhost WHERE id = %s", (sid,))
        if self.vhosts:
            hosts = [row[0] for row in cursor.fetchall()]
        else:
            hosts = [row[0] for row in cursor.fetchall()
                     if row[0].endswith('.%s'%PLOUD['domain'])]
        vhost.addVirtualHosts(hosts, env)

        self.changeHostsPolicy(hosts, name)

    def removeSite(self, sid, cursor):
        # remove virtual hosts
        cursor.execute("SELECT host FROM vhost WHERE id = %s", (sid,))
        hosts = [row[0] for row in cursor.fetchall()]
        vhost.removeVirtualHosts(hosts)

        # remove host policy
        removePolicy(hosts)


# sites: 2
# clones: 2
# bandwidth: 1Gb
# db size: 256Mb
# bots: no
# vhosts: no
# file size: 1Mb
POLICIES[0] = Policy(
    0, 2, 2, 1073741824, 268435456, False, False, 1048576, 2, 0)
POLICIES[0].title = 'Member'
POLICIES[0].description = 'Your account has been verified. You will be limited to perform limited number of site operations such as cloning, deleting and creating.'


# sites: 2
# clones: 10
# bandwidth: 20Gb
# db size: 5Gb
# bots: yes
# vhosts: yes
# file size: 20Mb
POLICIES[1] = Policy(
    1, 2, 10, 21474836480, 5368709120, True, True, 20971520, 10, 10)
POLICIES[1].title = 'Basic Member'
POLICIES[1].description = 'Your paid membership provides unlimited functionality with a limited number of site operations such as cloning, deleting and creating. The total number of active websites is limited to 3.'


# sites: 7
# clones: 999
# bandwidth: 100Gb
# db size: 30Gb
# bots: yes
# vhosts: yes
# file size: 100Mb
POLICIES[2] = Policy(
    2, 8, 999, 107374182400, 32212254720, True, True, 104857600, 30, 30)
POLICIES[2].title = 'Full Member'
POLICIES[2].description = 'Your paid membership provides unlimited functionality with unlimited site operations. The total number of active websites is limited to 8.'

# unvalidated users
# sites: 1
# clones: 1
# bandwidth: 30Mb
# db size: 5Mb
# bots: no
# vhosts: no
# file size: 1Mb
POLICIES[98] = Policy(
    98, 1, 1, 31457280, 5242880, False, False, 1048576, 0, 0)
POLICIES[98].title = 'Pending Member'
POLICIES[98].description = 'Your account has not been verified. Without completing the verification process your account will be deleted in 24 hours.'

# internal use
POLICIES[99] = Policy(
    99, 999, 999, 16106127360L, 16106127360L, True, True, 16106127360L, 999, 999)
POLICIES[99].title = 'Administrator'
POLICIES[99].description = 'You are administrator. You can do everything.'


def changePolicy(hosts, policy):
    val = str(policy)
    db = bsddb.hashopen(file, 'c')
    for host in hosts:
        db[str(host)] = val
    db.close()


def removePolicy(hosts):
    db = bsddb.hashopen(file, 'c')
    for host in hosts:
        if db.has_key(host):
            del db[host]
    db.close()


def rebuildPolicy():
    ploud_config.initializeConfig()

    conn = ploud_config.PLOUD_POOL.getconn()
    c1 = conn.cursor()

    c1.execute("SELECT vhost.host,users.type FROM sites,vhost,users "
               "WHERE users.id = sites.user_id and sites.id = vhost.id")

    hosts = {}
    for host, polid in c1.fetchall():
        data = hosts.setdefault(polid, set())
        data.add(host)

    for polid, data in hosts.items():
        changePolicy(data, polid)
        print 'Policy %s: %d added.'%(polid, len(data))

    c1.close()
    conn.close()


def main():
    user = sys.argv[1]
    polid = int(sys.argv[2])

    ploud_config.initializeConfig()

    conn = ploud_config.PLOUD_POOL.getconn()
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM users WHERE email=%s", (user,))
    uid = cursor.fetchone()[0]

    cursor.execute("UPDATE users SET type=%s WHERE id=%s",(polid, uid))

    POLICIES[polid].apply(uid, cursor)

    cursor.close()
    conn.commit()
    ploud_config.PLOUD_POOL.putconn(conn)
