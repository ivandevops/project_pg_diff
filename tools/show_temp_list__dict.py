from connect_pg.db_link import *
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


def judge(conn_db):                                                                #对不同的类型的对象做标记-------这个函数仅仅用于扫描模板库
    rows=db_exe(conn=conn_db,operate="SELECT is_private,object_type,object_name FROM fd_database_info;")
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



def show_create_func(conn):
    rows=db_exe(conn=conn,operate="SELECT object_name FROM fd_database_info where object_type=1;")
    dict_func=dict()
    for row in rows:
        dict_func.setdefault(row[0],0)          #存储函数名
    for funcname in dict_func.keys():
        str_select = "select show_create_funcation( )".split()[0] + ' ' + "select show_create_funcation( )".split()[
            1] + "'" + funcname + "'" + "select show_create_funcation( )".split()[2]
        rows=db_exe(conn=conn,operate=str_select)
        dict_func.update({funcname: rows[0][0]})
    return dict_func


def show_create_table(conn):
    rows=db_exe(conn=conn,operate="SELECT object_name FROM fd_database_info where object_type=3;")
    dict_table=dict()
    for row in rows:
        dict_table.setdefault(row[0],0)         #字典存储表名
    for tablename in dict_table.keys():
        str_select = "select show_create_table( )".split()[0] + ' ' + "select show_create_table( )".split()[
            1] + " 'public' " + "," + "'" + tablename + "'" + "select show_create_table( )".split()[2]
        rows=db_exe(conn=conn,operate=str_select)
        dict_table.update({tablename: rows[0][0]})          #key存在更新，key不存在添加新的,添加创建语句形成字典
    return dict_table




def show_create_sequence(conn):
    rows = db_exe(conn=conn,operate="SELECT object_name FROM fd_database_info where object_type=2;")
    dict_seq = dict()
    for row in rows:
        dict_seq.setdefault(row[0], 0)          #存储函数名
    for seqname in dict_seq.keys():
        str_select = "select show_create_sequence( )".split()[0] + ' ' + "select show_create_sequence( )".split()[
            1] + " 'public' " + "," + "'" + seqname + "'" + "select show_create_sequence( )".split()[2]
        #str_select = "select show_create_sequence( 'public'." + seqname + ")"
        rows = db_exe(conn=conn,operate=str_select)
        dict_seq.update({seqname: rows[0][0]})
    return dict_seq





def show_create_index(conn):
    rows = db_exe(conn=conn,operate="SELECT object_name FROM fd_database_info where object_type=4;")
    dict_index = dict()
    for row in rows:
        dict_index.setdefault(row[0], 0)
    for index_name in dict_index.keys():
        str_select = "select show_create_index( )".split()[0] + ' ' + "select show_create_index( )".split()[
            1] + "'" + index_name + "'" + "select show_create_index( )".split()[2]
        rows = db_exe(conn=conn,operate=str_select)
        dict_index.update({index_name: rows[0][0]})
    return dict_index





def show_create_trigger(conn):
    rows = db_exe(conn=conn,operate="SELECT object_name FROM fd_database_info where object_type=5;")
    dict_trigger = dict()
    for row in rows:
        dict_trigger.setdefault(row[0], 0)
    for trigger_name in dict_trigger.keys():
        str_select = "select show_create_trigger( )".split()[0] + ' ' + "select show_create_trigger( )".split()[
            1] + "'" + trigger_name + "'" + "select show_create_trigger( )".split()[2]
        rows = db_exe(conn=conn,operate=str_select)
        dict_trigger.update({trigger_name: rows[0][0]})
    return dict_trigger



def show_create_view(conn):
    rows = db_exe(conn=conn,operate="SELECT object_name FROM fd_database_info where object_type=6;")
    dict_view = dict()
    for row in rows:
        dict_view.setdefault(row[0], 0)
    for view_name in dict_view.keys():
        str_select = "select show_create_view( )".split()[0] + ' ' + "select show_create_view( )".split()[
            1] + "'" + view_name + "'" + "select show_create_view( )".split()[2]
        rows = db_exe(conn=conn,operate=str_select)
        dict_view.update({view_name: rows[0][0]})

    return dict_view


