import psycopg2
import configparser


def db_select(conn,operate):
    cur=conn.cursor()
    cur.execute(operate)
    rows=cur.fetchall()
    conn.commit()
    cur.close()
    return rows

def db_exe(conn,operate):
    cur=conn.cursor()
    cur.execute(operate)
    conn.commit()
    cur.close()

def db_close(conn):
    conn.close()
    print("exit  success")

def db_conn(db):
    cf=configparser.ConfigParser()
    cf.read("pgconfig.txt")
    host = cf.get(db, "host")
    dbname=cf.get(db,"dbname")
    user=cf.get(db,"user")
    password=cf.get(db,"password")
    port=cf.get(db,"port")
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    print("connect -----%s-----  success!!!"%dbname)
    return conn
















