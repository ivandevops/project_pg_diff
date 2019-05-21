from tools.show_temp_list__dict import *
def new(os_key,conn_db,conn_dev):
    new_func(S_fuc,conn_db,conn_dev)
    new_seq(S_public_seq,conn_db,conn_dev)
    new_table(S_public_table,conn_db,conn_dev)
    new_index(S_public_index,conn_db,conn_dev)
    new_trigger(S_public_trigger,conn_db,conn_dev)
    add(os_key,conn_db,conn_dev)
    new_view(S_view,conn_db,conn_dev)
    exit()
def add(os_key,conn_db,conn_dev):
    new_seq(S_private_seq,conn_db,conn_dev)
    new_table(S_private_table,conn_db,conn_dev)
    new_index(S_private_index,conn_db,conn_dev)
    new_trigger(S_private_trigger,conn_db,conn_dev)
    exit()


def new_table(table_list,conn_db,conn_dev):
    dict_table = show_create_table(conn_db)
    for table in table_list:
        db_exe(conn=conn_dev,operate=dict_table[table])
def new_func(func_list,conn_db,conn_dev):
    dict_func = show_create_func(conn_db)
    for func in func_list:
        db_exe(conn=conn_dev,operate=dict_func[func])
def new_index(index_list,conn_db,conn_dev):
    dict_index = show_create_index(conn_db)
    for index in index_list:
        db_exe(conn=conn_dev,operate=dict_index[index])
def new_trigger(trigger_list,conn_db,conn_dev):
    dict_trigger = show_create_trigger(conn_db)
    for trigger in trigger_list:
        db_exe(conn=conn_dev,operate=dict_trigger[trigger])
def new_seq(seq_list,conn_db,conn_dev):
    dict_seq = show_create_sequence(conn_db)
    for seq in seq_list:
        db_exe(conn=conn_dev,operate=dict_seq[seq])
def new_view(view_list,conn_db,conn_dev):
    dict_view = show_create_view(conn_db)
    for view in view_list:
        db_exe(conn=conn_dev,operate=dict_view[view])




