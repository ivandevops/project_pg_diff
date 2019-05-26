from connect_pg.db_link import *





def show_create_func(conn):
    rows=db_select(conn=conn,operate="SELECT object_name FROM fd_database_info where object_type=1;")
    dict_func=dict()
    for row in rows:
        dict_func.setdefault(row[0],0)          #存储函数名
    for funcname in dict_func.keys():
        str_select = "select show_create_funcation( )".split()[0] + ' ' + "select show_create_funcation( )".split()[
            1] + "'" + funcname + "'" + "select show_create_funcation( )".split()[2]
        rows=db_select(conn=conn,operate=str_select)
        dict_func.update({funcname: rows[0][0]})
    return dict_func






def show_create_sequence(conn):
    rows = db_select(conn=conn,operate="SELECT object_name FROM fd_database_info where object_type=2;")
    dict_seq = dict()
    for row in rows:
        dict_seq.setdefault(row[0], 0)          #存储函数名
    for seqname in dict_seq.keys():
        str_select = "select show_create_sequence( )".split()[0] + ' ' + "select show_create_sequence( )".split()[
            1] + " 'public' " + "," + "'" + seqname + "'" + "select show_create_sequence( )".split()[2]
        rows = db_select(conn=conn,operate=str_select)
        dict_seq.update({seqname: rows[0][0]})
    return dict_seq





def show_create_index(conn):
    rows = db_select(conn=conn,operate="SELECT object_name FROM fd_database_info where object_type=4;")
    dict_index = dict()
    for row in rows:
        dict_index.setdefault(row[0], 0)
    for index_name in dict_index.keys():
        str_select = "select show_create_index( )".split()[0] + ' ' + "select show_create_index( )".split()[
            1] + "'" + index_name + "'" + "select show_create_index( )".split()[2]
        rows = db_select(conn=conn,operate=str_select)
        dict_index.update({index_name: rows[0][0]})
    return dict_index





def show_create_trigger(conn):
    rows = db_select(conn=conn,operate="SELECT object_name FROM fd_database_info where object_type=5;")
    dict_trigger = dict()
    for row in rows:
        dict_trigger.setdefault(row[0], 0)
    for trigger_name in dict_trigger.keys():
        str_select = "select show_create_trigger( )".split()[0] + ' ' + "select show_create_trigger( )".split()[
            1] + "'" + trigger_name + "'" + "select show_create_trigger( )".split()[2]
        rows = db_select(conn=conn,operate=str_select)
        dict_trigger.update({trigger_name: rows[0][0]})
    return dict_trigger



def show_create_view(conn):
    rows = db_select(conn=conn,operate="SELECT object_name FROM fd_database_info where object_type=6;")
    dict_view = dict()
    for row in rows:
        dict_view.setdefault(row[0], 0)
    for view_name in dict_view.keys():
        str_select = "select show_create_view( )".split()[0] + ' ' + "select show_create_view( )".split()[
            1] + "'" + view_name + "'" + "select show_create_view( )".split()[2]
        rows = db_select(conn=conn,operate=str_select)
        dict_view.update({view_name: 'CREATE VIEW "public"."' + view_name + '"'+' AS '+rows[0][0]})

    return dict_view


