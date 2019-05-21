import psycopg2
import configparser
import os

def db_exe(conn,operate):
    cur=conn.cursor()
    cur.execute(operate)
    rows=cur.fetchall()
    conn.commit()
    cur.close()
    return rows

def db_close(conn):
    conn.close()
    print("exit  success")

def db_conn(db):                                                                            #注意配置文件需要不加引号
    #root_dir = os.path.dirname(os.path.abspath('.'))                                        #获取上一层
    cf=configparser.ConfigParser()
    #cf.read(root_dir+"/db.conf")
    cf.read("db.conf")
    host = cf.get(db, "host")
    dbname=cf.get(db,"dbname")
    user=cf.get(db,"user")
    password=cf.get(db,"password")
    port=cf.get(db,"port")
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    print("connect -----%s-----  success!!!"%dbname)
    return conn
















