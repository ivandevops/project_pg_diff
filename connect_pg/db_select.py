from connect_pg.db_link import *
from connect_pg.entity import *


def select(conn, sql):                      #执行创建语句
    rows = db_exe(conn , sql )
    return rows


# 查询database_info表中不同状态的用户，每个类型的用户返回一个列表
def select_object(conn, is_private = None, object_type = None, custom_type = None):
    sql = "SELECT object_name FROM fd_database_info WHERE"
    if is_private != None :
        sql = sql + " AND is_private = " + str(is_private)
    if object_type != None:
        sql = sql + " AND object_type = " + str(object_type)
    if custom_type != None:
        sql = sql + " AND custom_type @> '" + str(custom_type) + "'::jsonb"
    sql = (sql + " ORDER BY object_name ").replace("WHERE AND","WHERE").replace("WHERE ORDER","ORDER")
    result_table = db_select(conn, sql)
    tablename_list=[]
    for tablename in result_table:
        tablename_list.append(tablename[0])
    return tablename_list


# 查询系统中同一oskey的表
def select_tablename(conn, oskey):
    if oskey == "public":
        sql = "WITH table_pgtable AS (SELECT tablename,split_part( tablename, '_', LENGTH ( REPLACE ( tablename, '_', '__' )) - LENGTH ( tablename ) + 1 ) AS suffix FROM pg_tables WHERE tablename LIKE 'fd%') SELECT table_pgtable.tablename FROM table_pgtable WHERE table_pgtable.tablename LIKE 'fd%' AND table_pgtable.suffix != 'back' AND table_pgtable.suffix != 'bck' AND table_pgtable.suffix != 'bak' AND table_pgtable.suffix != 'copy' AND table_pgtable.suffix != 'old' AND table_pgtable.suffix != 'new' AND table_pgtable.suffix !~ '.*[0-9].*|.*[A-Z]+.*'  AND table_pgtable.suffix NOT IN (SELECT os_key FROM fd_content_dataos)  ORDER BY tablename"
    elif oskey.strip() != "":
        sql = "SELECT tablename FROM pg_tables WHERE tablename LIKE  'fd%" + oskey + "' ORDER BY tablename"
    else:
        return
    result_table = db_select(conn, sql)
    tablename_list=[]
    for tablename in result_table:
        tablename_list.append(tablename[0])
    return tablename_list


# 查询库中dataos表里不同状态的用户
def select_dataos_oskey(conn, os_type = None, os_status = None, os_create_time = None):
    sql = "SELECT os_key FROM fd_content_dataos WHERE"
    if os_type != None :
        sql = sql + " AND os_type = " + str(os_type)
    if os_status != None :
        sql = sql + " AND os_status = " + str(os_status)
    if os_create_time != None :
        sql = sql + " ADN os_create_time > " + str(os_create_time)
    sql = (sql + " ORDER BY os_key ").replace("WHERE AND","WHERE").replace("WHERE ORDER","ORDER")
    result_table = db_select(conn, sql)
    tablename_list=[]
    for tablename in result_table:
        tablename_list.append(tablename[0])
    return tablename_list


# 查询库中pgtable表中的oskey
def select_pgtables_oskey(conn):
    sql = "SELECT A.suffix FROM(SELECT DISTINCT(split_part( tablename, '_', LENGTH ( REPLACE ( tablename, '_', '__' )) - LENGTH ( tablename ) + 1 )) AS suffix FROM pg_tables WHERE tablename LIKE'fd_%') AS A WHERE A.suffix = UPPER ( A.suffix ) AND ( LENGTH ( A.suffix ) = 10 OR LENGTH ( A.suffix ) = 11 )"
    result_table = db_select(conn, sql)
    tablename_list=[]
    for tablename in result_table:
        tablename_list.append(tablename[0])
    return tablename_list


# 判断oskey是否在dataos表中存在
def select_oskey_exist(conn, oskey):
    sql = "SELECT os_key FROM fd_content_dataos where os_key = " + oskey
    result = db_select(conn, sql)
    if len(result[0][0]) > 0:
        return True
    else:
        return False


# 拼接表创建语句
def splicing_create_table_sql(tablename, column_entity_dict, constraint_entity_dict, remark_entity_dict):
    sql = " CREATE TABLE \"" + tablename + "\"("
    for column_name in column_entity_dict.keys():
        column_entity = column_entity_dict.get(column_name)
        sql = sql + "\"" + judge_str(column_entity.name) + "\" " + judge_str(column_entity.type) + " " + judge_str(column_entity.varchar_len) + " " + judge_str(column_entity.double_len) + " " + judge_str(column_entity.collate) + " " + judge_str(column_entity.is_nullable) + " " + judge_str(column_entity.default) + ","
    for constraint_name in constraint_entity_dict.keys():
        constraint_entity = constraint_entity_dict.get(constraint_name)
        sql = sql + " CONSTRAINT \"" + judge_str(constraint_entity.name) + "\" " + judge_str(constraint_entity.ck) + " " + judge_str(constraint_entity.uk) + " " + judge_str(constraint_entity.pk) + " " + judge_str(constraint_entity.fk) + ","
    sql = sql + ");"
    while sql.find(",);") > 0:
        sql = sql.replace(",);", ");")
    while sql.find("  ") > 0:
        sql = sql.replace("  ", " ")
    for remark_name in remark_entity_dict:
        remark_entity = constraint_entity_dict.get(remark_name)
        sql = sql + "COMMENT ON COLUMN " + tablename + "." + judge_str(remark_entity.name) + "IS '"+ judge_str(remark_entity.dict) + "';"
    return sql


# 查询创建语句： 传入连接和表名 返回
def select_create_table_sql(conn, tablename):
    column_entity_dict = select_create_table_column(conn, tablename)
    constraint_entity_dict = select_create_table_constraint(conn, tablename)
    remark_entity_dict = select_create_table_remark(conn, tablename)
    return splicing_create_table_sql(tablename, column_entity_dict, constraint_entity_dict, remark_entity_dict)


# 传入连接和表名  返回字典   字段、约束、注释
def select_create_table_dict(conn, tablename):
    table_dict = dict()
    table_dict.update({"column":select_create_table_column(conn, tablename)})
    table_dict.update({"constraint":select_create_table_constraint(conn, tablename)})
    table_dict.update({"remark":select_create_table_remark(conn, tablename)})
    return table_dict


# 传入连接和表名，返回字典  字段名：字段实体
def select_create_table_column(conn, table_name):
    sql = "SELECT column_name AS c_name,udt_name AS c_type,CASE WHEN character_maximum_length > 0 THEN '(' || character_maximum_length || ')' END AS c_varchar_len, CASE WHEN numeric_precision > 0 AND numeric_scale > 0 THEN '(' || numeric_precision || ', ' || numeric_scale || ')' END AS c_double_len,CASE WHEN POSITION('text' in udt_name)=1 OR POSITION('varchar' in udt_name)=1  THEN ' COLLATE \"pg_catalog\".\"default\"' END AS c_collate, CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' END AS c_is_nullable, CASE WHEN column_default IS NOT NULL THEN ' DEFAULT' END || ' ' || column_default AS c_default FROM information_schema.columns WHERE table_name = '" + table_name + "' AND table_schema='public' ORDER BY ordinal_position"
    result_table = db_select( conn , sql )
    dict_table= dict()
    for row in result_table:
        column = TableColumn(row[0],row[1],row[2],row[3],row[4],row[5],row[6])
        dict_table.update({column.name:column})
    return dict_table

# 传入连接和表名，返回字典  约束名：约束值
def select_create_table_constraint(conn, table_name):
    sql = "SELECT conname , CASE WHEN contype='c' THEN ' CHECK(\"'|| consrc ||'\")' END  AS ck , CASE WHEN contype='u' THEN  ' UNIQUE(\"'|| ( SELECT REPLACE(findattname('public','" + table_name + "','u'),',','\",\"') ) ||'\")' END AS uk , CASE WHEN contype='p' THEN ' PRIMARY KEY (\"'||  ( SELECT REPLACE(findattname('public','" + table_name + "','p'),',','\",\"') ) ||'\")' END  AS pk, CASE WHEN contype='f' THEN ' FOREIGN KEY(\"'|| ( SELECT findattname('public','" + table_name + "','u') ) ||'\") REFERENCES '|| (SELECT p.relname FROM pg_class p WHERE p.oid=c.confrelid )  || '('|| ( SELECT findattname('public','" + table_name + "','u') ) ||')' END AS fk FROM pg_constraint c  WHERE contype in('u','c','f','p') AND conrelid=( SELECT oid  FROM pg_class  WHERE relname='" + table_name + "' AND relnamespace =( SELECT oid FROM pg_namespace WHERE nspname='public' ) )	ORDER BY conname"
    result_table =  db_select(conn, sql)
    dict_table = dict()
    for row in result_table:
        constraint = TableConstraint(row[0], row[1], row[2], row[3], row[4])
        dict_table.update({constraint.name: constraint})
    return dict_table


# 传入连接和表名，返回字典  注释名：注释内容
def select_create_table_remark(conn, table_name):
    sql = "SELECT a.attname , d.description FROM pg_class c JOIN pg_description d ON c.oid=d.objoid JOIN pg_attribute a ON c.oid = a.attrelid  WHERE c.relname='" + table_name + "' AND a.attnum = d.objsubid ORDER BY a.attname"
    result_table =  db_select(conn, sql)
    dict_table = dict()
    for row in result_table:
        remark = TableRemark(row[0], row[1])
        dict_table.update({remark.name: remark})
    return dict_table


# 表处理：传入连接和创建表的语句，进行创建表
def create_table_sql(conn, create_table_sql):
    db_exe(conn, create_table_sql)


#
def create_table_dict(conn, tablename, create_table_dict):
    return create_table_sql(conn, splicing_create_table_sql(tablename, create_table_dict.get("column"), create_table_dict.get("constraint"), create_table_dict.get("remark")))


def create_table_dict(conn, tablename, column_entity_dict, constraint_entity_dict, remark_entity_dict):
    return create_table_sql(conn, splicing_create_table_sql(tablename, column_entity_dict, constraint_entity_dict, remark_entity_dict))


def drop_table(conn, tablename):
    sql = "DROP TABLE " + tablename
    db_exe(conn, sql)


# 表字段处理
def alter_table_add_column(conn, tablename, column_entity):
    sql = "ALTER TABLE " + tablename + " ADD COLUMN " + column_entity.name + " "+ judge_str(column_entity.type)  + judge_str(column_entity.varchar_len) + judge_str(column_entity.double_len) + judge_str(column_entity.collate) + judge_str(column_entity.is_nullable) + judge_str(column_entity.default)
    db_exe(conn, sql)


def alter_table_alter_column(conn, tablename, column_entity):
    sql = "ALTER TABLE " + tablename + " ALTER COLUMN " + column_entity.name  + " TYPE " + judge_str(column_entity.type)  + judge_str(column_entity.varchar_len) + judge_str(column_entity.double_len) + judge_str(column_entity.collate) + judge_str(column_entity.default)
    db_exe(conn, sql)


def alter_table_alter_column_nullable(conn, tablename, column_entity):
    sql = "ALTER TABLE " + tablename + " ALTER COLUMN " + column_entity.name  + " SET " + judge_str(column_entity.is_nullable)
    db_exe(conn, sql)


def alter_table_drop_column(conn, tablename, column_entity):
    sql = "ALTER TABLE " + tablename + " DROP COLUMN " + column_entity.name
    db_exe(conn, sql)


# 表字段约束处理
def alter_table_add_constraint(conn, tablename, constraint_entity):
    sql = "ALTER TABLE \"" + tablename + "\" ADD CONSTRAINT \"" + judge_str(constraint_entity.name) + "\"" + judge_str(constraint_entity.ck) + judge_str(constraint_entity.uk) + judge_str(constraint_entity.pk) + judge_str(constraint_entity.fk)
    db_exe(conn, sql)


# 注意返回值
def alter_table_alter_constraint(conn, tablename, constraint_entity):
    alter_table_drop_constraint(conn, tablename, constraint_entity)
    alter_table_add_constraint(conn, tablename, constraint_entity)


def alter_table_drop_constraint(conn, tablename, constraint_entity):
    sql = "ALTER TABLE " + tablename + " drop CONSTRAINT " + constraint_entity.name
    db_exe(conn, sql)


# 表字段备注处理
def alter_table_add_remarks(conn, tablename, remark_entity):
    sql = "COMMENT ON COLUMN " + tablename + "." + remark_entity.name + "IS '"+ judge_str(remark_entity.dict) + "'"
    db_exe(conn, sql)


def alter_table_alter_remarks(conn, tablename, remark_entity):
    return alter_table_add_remarks(conn, tablename, remark_entity)


def alter_table_drop_remarks(conn, tablename, remark_entity):
    sql = ""
    db_exe(conn, sql)


def judge_str(in_str = None):
    return "" if in_str is None else in_str


def select_create_sequence(conn,sequence_name):
    sql = "SELECT '" + sequence_name + "',max_value,min_value,cache_value,increment_by,start_value FROM \"" + sequence_name + "\" "
    result_table = db_select(conn,sql)
    dict_seq = dict()
    for row in result_table:
        sequence = Sequence(row[0],row[1],row[2],row[3],row[4],row[5])
        dict_seq.update({sequence_name:sequence})
    return dict_seq


def create_sequence_sql(conn, create_sequence_sql):
    db_exe(conn, create_sequence_sql)


def create_sequence_entity(conn, sequence_entity):
    create_sequence_sql(conn, splicing_create_sequence_sql(sequence_entity))


def splicing_create_sequence_sql(sequence_entity):
    sql = "CREATE SEQUENCE \"" + sequence_entity.name + "\" INCREMENT " + sequence_entity.increment + " MINVALUE " + sequence_entity.min + " MAXVLUE " + sequence_entity.max + " START " + sequence_entity.start + " CACHE " + sequence_entity.now
    return sql








