import sys
sys.path.append("D:\\project_pg_diff\\connect_pg")
from tools.judge import *
from connect_pg.db_link import *
def new(os_key=None):
    judge()
    conn = db_conn('db_dev')
    new_func(S_fuc)
    new_seq(S_public_seq)
    new_table(S_public_table)
    new_index(S_public_index)
    new_trigger(S_public_trigger)
    if os_key is not None:
        add()
    new_view(S_view)
    db_close(conn)

def add():
    judge()
    conn=db_conn('db_dev')
    new_seq(S_private_seq)
    new_table(S_private_table)
    new_index(S_private_index)
    new_trigger(S_private_trigger)
    db_close(conn)







"""
def add_func():
    conn=db_conn('db')
    views=db_exe(conn=conn,operate="select pg_get_functiondef(to_regproc('show_create_view'))")
    tables=db_exe(conn=conn,operate="select pg_get_functiondef(to_regproc('show_create_table'))")
    funcs=db_exe(conn=conn,operate="select pg_get_functiondef(to_regproc('show_create_funcation'))")
    indexs=db_exe(conn=conn,operate="select pg_get_functiondef(to_regproc('show_create_index'))")
    triggers=db_exe(conn=conn,operate="select pg_get_functiondef(to_regproc('show_create_trigger'))")
    seqs= db_exe(conn=conn, operate="select pg_get_functiondef(to_regproc('show_create_seq'))")
    db_close(conn)
    conn=db_conn('db_dev')
    for op in views[0][0],tables[0][0],funcs[0][0],indexs[0][0],triggers[0][0],seqs[0][0]:
        db_exe(conn=conn,operate=op)
    db_close(conn)

"""
