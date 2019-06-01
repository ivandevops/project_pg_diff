from tools.update import *
from tools.show_temp_list__dict import *
sys.path.append("D:\\project_pg_diff\\connect_pg")

# 创建，单表，单表的索引、序列、约束
def create(conn_tmp, conn_oli, tmp_tablename, oskey):
    if oskey == 'public':
        column_entity_dict = select_table_column_dict(conn_tmp, conn_oli, tmp_tablename, oskey)
        constraint_entity_dict = select_table_constraint_dict(conn_tmp, tmp_tablename, oskey)
        tmp_remark_dict = select_create_table_remark(conn_tmp, tmp_tablename)
        if create_table_dict(conn_oli, tmp_tablename, column_entity_dict, constraint_entity_dict, tmp_remark_dict):
            create_index(conn_tmp, conn_oli, oskey_tmp=None, oli_oskey=oskey)           #------------------------------------------------------------
            print(tmp_tablename + ' create success(include sequence and constraint and remark!)')

    if oskey != 'public':
        oskey_tmp = tmp_tablename.split('_')[-1]
        print(oskey_tmp)
        column_entity_dict = select_table_column_dict(conn_tmp, conn_oli, tmp_tablename, oskey)
        constraint_entity_dict = select_table_constraint_dict(conn_tmp,tmp_tablename,oskey)
        tmp_remark_dict = select_create_table_remark(conn_tmp, tmp_tablename)
        if create_table_dict(conn_oli, tmp_tablename, column_entity_dict, constraint_entity_dict, tmp_remark_dict):
            #create_index(conn_tmp, conn_oli, oskey_tmp, oskey)
            print(tmp_tablename+' create success(include sequence and constraint and remark!)')



# 创建用户，根据os_key创建用户
def create_user(conn_tmp, conn_oli, oskey, is_private=None, custom_type=None):
    tmp_table_list = select_object(conn_tmp, is_private, 3, custom_type)
    if create_func(conn_tmp,conn_oli):
        for table_name in tmp_table_list:
            create(conn_tmp,conn_oli,table_name,oskey)
    if oskey != 'public':
        create_view(conn_tmp, conn_oli)


def select_table_column_dict(conn_tmp, conn_oli, tmp_tablename, oli_oskey):
    tmp_column_dict=select_create_table_column(conn_tmp, tmp_tablename)
    print(oli_oskey)
    for column_name in tmp_column_dict.keys():
        tmp_column = tmp_column_dict.get(column_name)
        if tmp_column.default != None:
            if tmp_column.default.find("nextval") > 0:
                seq_list =[]
                seq_list_rows = db_select(conn_oli, "select relname from pg_class where  relkind='S'")
                for row in seq_list_rows:
                    seq_list.append(row[0])
                if tmp_column.default[tmp_column.default.find("nextval('") + len("nextval('"):tmp_column.default.find("'::regclass)")] not in seq_list:
                    tmp_sequence_name = tmp_column.default[tmp_column.default.find("nextval('") + len("nextval('"):tmp_column.default.find("'::regclass)")]
                    tmp_sequence_entity = select_create_sequence(conn_tmp, tmp_sequence_name)
                    if oli_oskey != "public":
                        print(tmp_sequence_entity[tmp_sequence_name].name)
                        update_oskey_by_tmp_tablename(tmp_sequence_name, tmp_tablename, oli_oskey)
                        tmp_column.default = update_oskey_by_tmp_tablename(tmp_column.default,tmp_tablename,oli_oskey)
                        tmp_column_dict.update({column_name: tmp_column})
                    create_sequence_entity(conn_oli, tmp_sequence_entity[tmp_sequence_name])
    return tmp_column_dict


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


def create_view(conn_tmp, conn_oli):
    dict_view = show_create_view(conn_tmp)
    view_list = select_object(conn_tmp, object_type=6)
    for view in view_list:
        db_exe(conn_oli,dict_view[view])
        print('视图 ' + view + 'create success！')
    return True



def get_func_oid(conn_tmp):
    func_list = select_object(conn_tmp, object_type=1)
    oid_list = []
    for func_name in func_list:
        rows = db_select(conn_tmp, "select oid from pg_proc where proname ='" + str(func_name) + "'")
        if len(rows) == 1:
            oid_list.append(rows[0][0])
        if len(rows) > 1:
            for i in range(len(rows)):
                oid_list.append(rows[i][0])
    return oid_list


def create_func(conn_tmp, conn_online):
    oid_list = get_func_oid(conn_tmp)
    for oid in oid_list:
        rows = db_select(conn_tmp, "select pg_get_functiondef(" + str(oid) + ")")
        db_exe(conn_online, rows[0][0])
    print(str(len(oid_list)) + "func")
    return True

