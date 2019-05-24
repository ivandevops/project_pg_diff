import sys
from connect_pg.db_select import *
from tools.update import *
from tools.show_temp_list__dict import *
sys.path.append("D:\\project_pg_diff\\connect_pg")


def create(conn_tmp, conn_oli, tmp_tablename, oskey):
    column_entity_dict = select_table_column_dict(conn_tmp, conn_oli, tmp_tablename, oskey)         #判断索引   序列
    constraint_entity_dict = select_table_constraint(conn_tmp,tmp_tablename,oskey)
    #约束
    #~~~备注
    #~~~~索引

    create_table_dict(conn_oli, oli_tablename, column_entity_dict,约束dict，备注dict)
    create索引

# 传入模板库表名，模板库连接，oskey，更新表字段中序列的os_key
def select_table_column_dict(conn_tmp,conn_oli,tmp_tablename,oli_oskey):
    tmp_column_dict=select_create_table_column(conn_tmp,tmp_tablename)
    for column_name in tmp_column_dict.keys():
        tmp_column = tmp_column_dict.get(column_name)
        if tmp_column.default.find("nextval") > 0:
            tmp_sequence_name = tmp_column.default[tmp_column.default.find("nextval('") + len("nextval('"):tmp_column.default.find("'::regclass)")]
            tmp_sequence_entity = select_create_sequence(conn_tmp, tmp_sequence_name)
            if oli_oskey != "public":
                tmp_sequence_entity.name = update_oskey_by_tmp_tablename(tmp_sequence_name, tmp_tablename, oli_oskey)
                tmp_column.default = update_oskey_by_tmp_tablename(tmp_column.default,tmp_tablename,oli_oskey)
                tmp_column_dict.update({column_name: tmp_column})
            create_sequence_entity(conn_oli, tmp_sequence_entity)
    return tmp_column_dict


# 传入模板库表名，模板库连接，oskey，
def select_table_constraint(conn_tmp,tmp_tablename,oli_oskey):
    tmp_constraint_dict=select_create_table_constraint(conn_tmp,tmp_tablename)
    for constraint_name in tmp_constraint_dict.keys():
        tmp_constraint = tmp_constraint_dict.get(constraint_name)
        if oli_oskey != "public":
            tmp_constraint_dict.popitem(constraint_name)
            constraint_name=update_oskey_by_tmp_tablename(tmp_constraint.name,tmp_tablename,oli_oskey)
            tmp_constraint_dict.update({constraint_name:tmp_constraint})

    return tmp_constraint_dict








