import sys
from connect_pg.db_select import *
from tools.update import *
from tools.show_temp_list__dict import *
sys.path.append("D:\\project_pg_diff\\connect_pg")


def create(conn_tmp, conn_oli, tmp_tablename, oskey):
    oskey_tmp = tmp_tablename.split['_'][-1]
    column_entity_dict = select_table_column_dict(conn_tmp, conn_oli, tmp_tablename, oskey)
    constraint_entity_dict = select_table_constraint_dict(conn_tmp,tmp_tablename,oskey)
    tmp_remark_dict = select_create_table_remark(conn_tmp, tmp_tablename)
    if create_table_dict(conn_oli, tmp_tablename, column_entity_dict, constraint_entity_dict, tmp_remark_dict):
        if create_index(conn_tmp,  conn_oli, oskey_tmp, oskey):
            if create_trigger(conn_tmp, conn_oli, oskey_tmp, oskey):
                print("create success!!!")






def select_table_column_dict(conn_tmp, conn_oli, tmp_tablename, oli_oskey):
    tmp_column_dict=select_create_table_column(conn_tmp, tmp_tablename)
    for column_name in tmp_column_dict.keys():
        tmp_column = tmp_column_dict.get(column_name)
        if tmp_column.default.find("nextval") > 0:
            tmp_sequence_name = tmp_column.default[tmp_column.default.find("nextval('") + len("nextval('"):tmp_column.default.find("'::regclass)")]
            tmp_sequence_entity = select_create_sequence(conn_tmp, tmp_sequence_name)
            if oli_oskey != "public":
                tmp_sequence_entity.name = update_oskey_by_tmp_tablename(tmp_sequence_name, tmp_tablename, oli_oskey)
                tmp_column.default = update_oskey_by_tmp_tablename(tmp_column.default,tmp_tablename,oli_oskey)
                tmp_column_dict.update({column_name: tmp_column})
            create_sequence_entity(conn_oli, tmp_sequence_entity)                   # 创建序列的操作
    return tmp_column_dict                                                          # 返回字段的实体



def select_table_constraint_dict(conn_tmp, tmp_tablename, oli_oskey):
    tmp_constraint_dict = select_create_table_constraint(conn_tmp, tmp_tablename)
    for constraint_name in tmp_constraint_dict.keys():
        tmp_constraint = tmp_constraint_dict.get(constraint_name)
        if oli_oskey != "public":
            tmp_constraint.name = update_oskey_by_tmp_tablename(tmp_constraint.name,  tmp_tablename, oli_oskey)
            constraint_name=update_oskey_by_tmp_tablename(constraint_name, tmp_tablename, oli_oskey)
            tmp_constraint_dict.update({constraint_name:tmp_constraint})

    return tmp_constraint_dict






def create_index(conn_tmp, conn_oli, oskey_tmp, oli_oskey):
    dict_index = show_create_index(conn_tmp)
    if oli_oskey == 'public':
        index_list = select_object(conn_tmp, object_type=4, is_private=0)
        for index in index_list:
            db_exe(conn_oli,dict_index[index])
    else:
        index_list = select_object(conn_tmp, object_type=4, is_private=1)
        for index in index_list:
            dict_index.update({index:index_list[index].replace(oskey_tmp,oli_oskey)})
            db_exe(conn_oli,dict_index[index])
    return True




def create_trigger(conn_tmp, conn_oli, oskey_tmp, oli_oskey):
    dict_trigger = show_create_trigger(conn_tmp)
    if oli_oskey == 'public':
        trigger_list = select_object(conn_tmp, object_type=5, is_private=0)
        for trigger in trigger_list:
            db_exe(conn_oli,dict_trigger[trigger])
    else:
        trigger_list = select_object(conn_tmp, object_type=5, is_private=1)
        for trigger in trigger_list:
            dict_trigger.update({trigger:trigger_list[trigger].replace(oskey_tmp,oli_oskey)})
            db_exe(conn_oli, dict_trigger[trigger])
    return True







