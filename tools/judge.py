from tools.show_create import *
S_fuc = []
S_public_seq = []
S_public_table = []
S_public_index = []
S_public_trigger = []
S_private_seq = []
S_private_table = []
S_private_index = []
S_private_trigger = []
S_view = []

def judge():                                                                #对不同的类型的对象做标记-------这个函数仅仅用于扫描模板库
    conn=db_conn("db")
    rows=db_exe(conn=conn,operate="SELECT is_private,object_type,object_name FROM fd_database_info;")
    for row in rows:
        if row[0] == 0:         #公有对象
             if row[1] == 1:        #函数
                 S_fuc.append(row[2])
             if row[1] == 2:        #序列
                S_public_seq.append(row[2])
             if row[1] == 3:        #表
                S_public_table.append(row[2])
             if row[1] == 4:        #索引
                S_public_index.append(row[2])
             if row[1] == 5:        #触发器
                S_public_trigger.append(row[2])
        if row[0] == 1:
             if row[1] == 2:        #序列
                S_private_seq.append(row[2])
             if row[1] == 3:        #表
                S_private_seq.append(row[2])
             if row[1] == 4:        #索引
                S_private_index.append(row[2])
             if row[1] == 5:        #触发器
                S_private_trigger.append(row[2])
        if row[1] == 6:             #视图
            S_view.append(row[2])
    db_close(conn)


def new_table(table_list):
    dict_table = show_create_table()
    for table in table_list:
        table_name = table
        db_exe(dict_table[table_name])


def new_func(func_list):
    dict_func = show_create_func()
    for func in func_list:
        func_name = func
        db_exe(dict_func[func_name])


def new_index(index_list):
    dict_index = show_create_index()
    for index in index_list:
        index_name = index
        db_exe(dict_index[index_name])


def new_trigger(trigger_list):
    dict_trigger = show_create_trigger()
    for trigger in trigger_list:
        trigger_name = trigger
        db_exe(dict_trigger[trigger_name])


def new_seq(seq_list):
    dict_seq = show_create_sequence()
    for seq in seq_list:
        seq_name = seq
        db_exe(dict_seq[seq_name])


def new_view(view_list):
    dict_view = show_create_view()
    for view in view_list:
        view_name = view
        db_exe(dict_view[view_name])

