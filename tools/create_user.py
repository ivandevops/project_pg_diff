from tools.show_temp_list__dict import *
def new(os_key,conn_1,conn_2):
    if new_func(func_list=S_fuc,conn_db=conn_1,conn_dev=conn_2):
            if  new_seq(seq_list=S_public_seq,conn_db=conn_1,conn_dev=conn_2):
                if new_table(os_key,table_list=S_public_table,conn_db=conn_1,conn_dev=conn_2):
                    if new_index(index_list=S_public_index,conn_db=conn_1,conn_dev=conn_2):
                        if new_trigger(trigger_list=S_public_trigger,conn_db=conn_1,conn_dev=conn_2):
                            if  add(os_key=os_key,conn_db=conn_1,conn_dev=conn_2):
                                if  new_view(view_list=S_view,conn_db=conn_1,conn_dev=conn_2):
                                    exit()
def add(os_key,conn_db,conn_dev):
    if new_seq(seq_list=S_private_seq,conn_db=conn_db,conn_dev=conn_dev):
        if new_table(os_key,table_list=S_private_table,conn_dev=conn_dev,conn_db=conn_db):
            if new_index(index_list=S_private_index,conn_db=conn_db,conn_dev=conn_dev):
                if new_trigger(trigger_list=S_private_trigger,conn_dev=conn_dev,conn_db=conn_db):
                    exit()


def new_table(os_key,table_list,conn_db,conn_dev):
    dict_table = show_create_table(conn_db)
    if table_list == S_private_table:
        for table in table_list:
            db_exe(conn=conn_dev,operate=dict_table[table])
            str=table.split('_')
            str.pop()
            str_1='_'.join(str)
            db_exe(conn=conn_dev,operate='ALTER TABLE '+'"'+table+'"'+' rename to '+'"'+str_1+'_'+os_key+'"')
        return True
    if table_list == S_public_table:
        for table in table_list:
            db_exe(conn=conn_dev, operate=dict_table[table])
        return True

def new_func(func_list,conn_db,conn_dev):
    dict_func = show_create_func(conn_db)
    for func in func_list:
        db_exe(conn=conn_dev,operate=dict_func[func])
    return True
def new_index(index_list,conn_db,conn_dev):
    dict_index = show_create_index(conn_db)
    for index in index_list:
        db_exe(conn=conn_dev,operate=dict_index[index])
    return True
def new_trigger(trigger_list,conn_db,conn_dev):
    dict_trigger = show_create_trigger(conn_db)
    for trigger in trigger_list:
        db_exe(conn=conn_dev,operate=dict_trigger[trigger])
    return True
def new_seq(seq_list,conn_db,conn_dev):
    dict_seq = show_create_sequence(conn_db)
    for seq in seq_list:
        db_exe(conn=conn_dev,operate=dict_seq[seq])
    return True
def new_view(view_list,conn_db,conn_dev):
    dict_view = show_create_view(conn_db)
    for view in view_list:
        db_exe(conn=conn_dev,operate=dict_view[view])
    return True





