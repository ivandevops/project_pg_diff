from connect_pg.db_link import *
from connect_pg.entity import *


def select(conn, sql):
    rows = db_exe(conn , sql )
    return rows

####注意修改查找custom_type
def select_object( conn, is_private = None, object_type = None, custom_type = None):
    sql = "select object_name from fd_database_info where"
    if is_private.strip() != "":
        sql = sql + " and is_private = " + is_private
    if object_type.strip() != "":
        sql = sql + " and object_type = " + object_type
    if custom_type.strip() != "":
        sql = sql + " and custom_type = " + custom_type
        sql = (sql + " ordey by object_name ").replace("where add","where").replace("where ordey","ordey")
    result_table = db_exe(conn, sql)
    tablename_list=[]
    for tablename in result_table:
        tablename_list.append(tablename[0])
    return tablename_list


def select_tablename( conn, oskey):
    if oskey == "public":
        sql = "WITH table_pgtable AS (SELECT tablename,split_part( tablename, '_', LENGTH ( REPLACE ( tablename, '_', '__' )) - LENGTH ( tablename ) + 1 ) AS suffix FROM pg_tables WHERE tablename LIKE 'fd%') SELECT table_pgtable.tablename FROM table_pgtable WHERE table_pgtable.tablename LIKE 'fd%' AND table_pgtable.suffix != 'back' AND table_pgtable.suffix != 'bck' AND table_pgtable.suffix != 'bak' AND table_pgtable.suffix != 'copy' AND table_pgtable.suffix !~ '.*[0-9].*|.*[A-Z]+.*'  AND table_pgtable.suffix NOT IN (SELECT os_key FROM fd_content_dataos)  ORDER BY tablename"
    elif oskey.strip() != "":
        sql = "select tablename from pg_tables like 'fd%" + oskey + "' ORDER BY tablename"
    else:
        return
    result_table = db_exe(conn, sql)
    tablename_list=[]
    for tablename in result_table:
        tablename_list.append(tablename[0])
    return tablename_list


def select_oskey( conn,os_type,os_status,os_create_time = None):
    sql = "select os_key from fd_content_dataos where"
    if os_type.strip() != "":
        sql = sql + " and os_type = " + os_type
    if os_status.strip() != "":
        sql = sql + " and os_status = " + os_status
    if os_create_time.strip() != "":
        sql = sql + " and os_create_time > " + os_create_time
        sql = (sql + " ordey by os_key ").replace("where add","where").replace("where ordey","ordey")
    result_table = db_exe(conn, sql)
    tablename_list=[]
    for tablename in result_table:
        tablename_list.append(tablename[0])
    return tablename_list


def select_table_columns( conn, table_name):
    sql = "SELECT column_name AS c_name,udt_name AS c_type,CASE WHEN character_maximum_length > 0 THEN '(' || character_maximum_length || ')' END AS c_varchar_len, CASE WHEN numeric_precision > 0 AND numeric_scale > 0 THEN '(' || numeric_precision || ', ' || numeric_scale || ')' END AS c_double_len,CASE WHEN POSITION('text' in udt_name)=1 OR POSITION('varchar' in udt_name)=1  THEN ' COLLATE \"pg_catalog\".\"default\"' END AS c_collate, CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' END AS c_is_nullable, CASE WHEN column_default IS NOT NULL THEN ' DEFAULT' END || ' ' || column_default AS c_default FROM information_schema.columns WHERE table_name = " + table_name + " AND table_schema='public' ORDER BY ordinal_position"
    result_table = db_exe( conn , sql )
    dist_table={}
    for row in result_table:
        column = TableColumn(row[0],row[1],row[2],row[3],row[4],row[5],row[6])
        dist_table.update(column.name,column)
    return dist_table


def select_table_constraint( conn, table_name):
    sql = "SELECT conname , CASE WHEN contype='c' THEN ' CHECK(\"'|| consrc ||'\")' END  AS ck , CASE WHEN contype='u' THEN  ' UNIQUE(\"'|| ( SELECT REPLACE(findattname('public','" + table_name + "','u'),',','\",\"') ) ||'\")' END AS uk , CASE WHEN contype='p' THEN ' PRIMARY KEY (\"'||  ( SELECT REPLACE(findattname('public','" + table_name + "','p'),',','\",\"') ) ||'\")' END  AS pk, CASE WHEN contype='f' THEN ' FOREIGN KEY(\"'|| ( SELECT findattname('public','" + table_name + "','u') ) ||'\") REFERENCES '|| (SELECT p.relname FROM pg_class p WHERE p.oid=c.confrelid )  || '('|| ( SELECT findattname('public','" + table_name + "','u') ) ||')' END AS fk FROM pg_constraint c  WHERE contype in('u','c','f','p') AND conrelid=( SELECT oid  FROM pg_class  WHERE relname='" + table_name + "' AND relnamespace =( SELECT oid FROM pg_namespace WHERE nspname='public' ) )	ORDER BY conname"
    result_table =  db_exe(conn, sql)
    dist_table = {}
    for row in result_table:
        column = TableConstraint(row[0], row[1], row[2], row[3], row[4])
        dist_table.update(column.name, column)
    return dist_table


def select_table_remarks( conn, table_name):
    sql = "SELECT a.attname , d.description FROM pg_class c JOIN pg_description d ON c.oid=d.objoid JOIN pg_attribute a ON c.oid = a.attrelid  WHERE c.relname='" + table_name + "' AND a.attnum = d.objsubid ORDER BY a.attname"
    result_table =  db_exe(conn, sql)
    dist_table = {}
    for row in result_table:
        column = TableRemarks(row[0], row[1])
        dist_table.update(column.name, column)
    return dist_table


def select_oskey_exist( conn, oskey):
    sql = "SELECT os_key FROM fd_content_dataos where os_key = " + oskey
    result = db_exe(conn, sql)
    if len(result[0][1]) > 0:
        return True
    else:
        return False
