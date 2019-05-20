import sys
from connect_pg.db_link import *
from tools.judge import *
sys.path.append("D:\\project_pg_diff\\connect_pg")

judge()                         #   扫描模板库，分类存储
def update_func():
    conn=db_conn("db_dev")                                                                  #连接线上
    dict_func_online=dict()                                                                 #线上key-value
    dict_func=show_create_func()
    rows=db_exe(conn=conn,operate="select proname from pg_proc where proname like 'dblink%")
    for row in rows:
        dict_func_online.setdefault(row[0],0)                           #线上key-value  funcname：0

    for funcname in dict_func_online.keys():
        str_select = "select show_create_funcation( )".split()[0] + ' ' + "select show_create_funcation( )".split()[
            1] + "'" + funcname + "'" + "select show_create_funcation( )".split()[2]
        rows = db_exe(conn=conn, operate=str_select)
        dict_func_online.update({funcname: rows[0][0]})
    for func_name in S_fuc:
        if func_name in dict_func_online.keys():                                   #可以找到，判断内容
            if dict_func[func_name] == dict_func_online[func_name]:                 #内容一样，不做变化
                pass
            else:
                str_r = str.replace('CREATE FUNCATION', 'CREATE OR REPLACE FUNCATION')                                                  #------------判断是否更新新增
                db_exe(conn=conn, operate=str_r)
        else:
            db_exe(conn=conn, operate=dict_func[func_name])                 # 模板库的函数在线上找不到，新建

    db_close('db_dev')


def update_view():
    conn = db_conn("db_dev")
    dict_view_online = dict()
    dict_view = show_create_view()
    rows = db_exe(conn=conn, operate="select viewname from pg_views where viewname not like 'pg%'")
    for row in rows:
        dict_view_online.setdefault(row[0], 0)
    for viewname in dict_view_online.keys():
        str_select = "select show_create_view( )".split()[0] + ' ' + "select show_create_view( )".split()[
            1] + "'" + viewname + "'" + "select show_create_view( )".split()[2]
        rows = db_exe(conn=conn, operate=str_select)
        dict_view_online.update({viewname: rows[0][0]})
    for viewname in S_view:
        if viewname in dict_view_online.keys():                         #可以找到，判断内容
            if dict_view[viewname] == dict_view_online[viewname]:       #内容一样，不做变化
                pass
            else:
                str = dict_view[viewname]
                str_r = str.replace('CREATE VIEW', 'CREATE OR REPLACE VIEW')
                db_exe(conn=conn, operate=str_r)                        #内容不一样，重新执行函数创建语句，覆盖之前的含糊
        else:
            db_exe(conn=conn, operate=dict_view[viewname])              #模板库的函数在线上找不到，新建
    db_close('db_dev')



def update_trigger(service):
    conn = db_conn('db_dev')                                                    #连接线上
    dict_trigger = show_create_trigger()
    dict_trigger_online = dict()
    rows = db_exe(conn=conn, operate="SELECT tgname from pg_trigger")  # 查询线上所有触发器
    for row in rows:
        dict_trigger_online.setdefault(row[0], 0)  # 线上视图字典
    for triggername in dict_trigger_online.keys():
        str_select = "select show_create_trigger( )".split()[0] + ' ' + "select show_create_trigger( )".split()[
            1] + "'" + triggername + "'" + "select show_create_trigger( )".split()[2]
        rows = db_exe(conn=conn, operate=str_select)
        dict_trigger_online.update({triggername: rows[0][0]})

    if service == 'public':
        for triggername in S_public_trigger:
            if triggername in dict_trigger_online:  # 可以找到，判断内容
                if dict_trigger[triggername] == dict_trigger_online[triggername]:  # 内容一样，不做变化
                    pass
                else:
                    str = dict_trigger[triggername]
                    str_r = str.replace('CREATE TRIGGER', 'CREATE OR REPLACE TRIGGER')
                    db_exe(conn=conn, operate=str_r)  # 内容不一样，重新执行函数创建语句，覆盖之前的含糊
            else:
                db_exe(conn=conn, operate=dict_trigger[triggername])  # 模板库的函数在线上找不到，新建
    if service == 'private':
        for triggername in S_private_trigger:
            if triggername in dict_trigger_online:  # 可以找到，判断内容
                if dict_trigger[triggername] == dict_trigger_online[triggername]:  # 内容一样，不做变化
                    pass
                else:
                    str = dict_trigger[triggername]
                    str_r = str.replace('CREATE TRIGGER', 'CREATE OR REPLACE TRIGGER')
                    db_exe(conn=conn, operate=str_r)  # 内容不一样，重新执行函数创建语句，覆盖之前的含糊
            else:
                db_exe(conn=conn, operate=dict_trigger[triggername])  # 模板库的函数在线上找不到，新建
    db_close('db_dev')




def update_seq(service):
    pass

def update_index(service):
    pass

def update_table(service):
    pass





