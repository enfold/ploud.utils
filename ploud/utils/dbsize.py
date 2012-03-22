import psycopg2

def dbsize(cursor, siteid):
    stmt = """SELECT pg_total_relation_size('ploud%(id)s_blob_chunk'),
                        pg_relation_size('ploud%(id)s_object_state')"""%{'id': siteid}
    cursor.execute(stmt)
    return sum(cursor.fetchone())


def main():
    ploud = psycopg2.connect("dbname=ploud user=ploud host=ploud-app1")
    clients = psycopg2.connect("dbname=clients user=ploud host=ploud-app1")

    c1 = ploud.cursor()
    c2 = clients.cursor()

    c1.execute("SELECT id, site_name FROM sites")
    for row in c1.fetchall():
        size = dbsize(c2, row[0])
        c1.execute("UPDATE sites SET size=%d WHERE id=%d"%(size, row[0]))
    c1.close()
    c2.close()
    ploud.commit()
    ploud.close()
    clients.close()
