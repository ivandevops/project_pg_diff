import sys
from tools.show_temp_list__dict import *
sys.path.append("D:\\project_pg_diff\\connect_pg")


def update_func(conn_db,conn_dev):
    log_func=[]
    dict_func_online=dict()
    dict_func=show_create_func(conn_db)
    rows=db_exe(conn=conn_dev,operate="select proname from pg_proc where proname like 'dblink%")
    for row in rows:
        dict_func_online.setdefault(row[0],0)

    for funcname in dict_func_online.keys():
        str_select = "select show_create_funcation( )".split()[0] + ' ' + "select show_create_funcation( )".split()[1] + "'" + funcname + "'" + "select show_create_funcation( )".split()[2]
        rows = db_exe(conn=conn_dev, operate=str_select)
        dict_func_online.update({funcname: rows[0][0]})

    for func_name in S_fuc:
        if func_name in dict_func_online.keys():                                            #可以找到，判断内容
            if dict_func[func_name] == dict_func_online[func_name]:                         #内容一样，不做变化
                pass
            else:
                log_func.append(func_name+'此函数发生更新')
                str=dict_func[func_name]
                str_r = str.replace('CREATE FUNCATION', 'CREATE OR REPLACE FUNCATION')
                db_exe(conn=conn_dev, operate=str_r)
        else:
            db_exe(conn=conn_dev, operate=dict_func[func_name])                 # 模板库的函数在线上找不到，新建
    for func_name in dict_func_online.keys():
        if func_name in S_fuc:
            pass
        else:
            log_func.append(func_name+'此函数在模板库中不存在！')


def update_view(conn_db,conn_dev):
    log_view=[]
    dict_view_online = dict()
    dict_view = show_create_view(conn_db)
    rows = db_exe(conn=conn_dev, operate="select viewname from pg_views where viewname not like 'pg%'")
    for row in rows:
        dict_view_online.setdefault(row[0], 0)
    for viewname in dict_view_online.keys():
        str_select = "select show_create_view( )".split()[0] + ' ' + "select show_create_view( )".split()[
            1] + "'" + viewname + "'" + "select show_create_view( )".split()[2]
        rows = db_exe(conn=conn_dev, operate=str_select)
        dict_view_online.update({viewname: rows[0][0]})
    for viewname in S_view:
        if viewname in dict_view_online.keys():                         #可以找到，判断内容
            if dict_view[viewname] == dict_view_online[viewname]:       #内容一样，不做变化
                pass
            else:
                log_view.append(viewname+'此视图发生更新')
                str = dict_view[viewname]
                str_r = str.replace('CREATE VIEW', 'CREATE OR REPLACE VIEW')
                db_exe(conn=conn_dev, operate=str_r)                        #内容不一样，重新执行函数创建语句，覆盖之前的含糊
        else:
            db_exe(conn=conn_dev, operate=dict_view[viewname])              #模板库的函数在线上找不到，新建
    for view_name in dict_view_online.keys():
        if view_name in S_view:
            pass
        else:
            log_view.append(view_name+'此视图在模板库中不存在')


# 触发器名字相同，判断内容组件，，触发器对应的表不同直接新建，触发器对应的表相同，判断操作，操作不同，重建触发器，操作相同pass
def update_trigger(service,conn_db,conn_dev):
    log_trigger=[]
    dict_trigger = show_create_trigger(conn_db)
    dict_trigger_online = dict()
    rows = db_exe(conn=conn_dev, operate="SELECT tgname from pg_trigger")
    for row in rows:
        dict_trigger_online.setdefault(row[0], 0)
    for triggername in dict_trigger_online.keys():
        str_select = "select show_create_trigger( )".split()[0] + ' ' + "select show_create_trigger( )".split()[
            1] + "'" + triggername + "'" + "select show_create_trigger( )".split()[2]
        rows = db_exe(conn=conn_dev, operate=str_select)
        dict_trigger_online.update({triggername: rows[0][0]})

    trigger_list=[]
    if service == 'public':
        trigger_list=S_public_trigger
    if service == 'private':
        trigger_list=S_private_trigger
    for triggername in trigger_list:
        if triggername in dict_trigger_online:
            if dict_trigger[triggername] == dict_trigger_online[triggername]:       #代表了名字相同，操作相同，表相同
                pass
            else:
                online=dict_trigger_online[triggername].split()
                temp=dict_trigger[triggername].split()
                if online[8] == temp[8]:
                    if online[14] == temp[14]:
                        pass
                    else:
                        db_exe(conn=conn_dev,operate="DROP TRIGGER"+triggername+"on"+' '+online[8]) #删除触发器
                        db_exe(conn=conn_dev, operate=dict_trigger[triggername])
                else:
                    db_exe(conn=conn_dev, operate=dict_trigger[triggername])

        else:
            db_exe(conn=conn_dev, operate=dict_trigger[triggername])

    for triggername in dict_trigger_online.keys():
        if  triggername in trigger_list:
            pass
        else:
            log_trigger.append(triggername+'此触发器在模板库中不存在')


def update_seq(service,conn_db,conn_dev):
    log_seq = []
    dict_seq=show_create_sequence(conn_db)
    dict_seq_online=dict()
    rows = db_exe(conn=conn_dev, operate="SELECT relname from pg_class where relname like 'seq%' or '%seq' ")               #连接线上库，获取序列名
    for row in rows:
        dict_seq_online.setdefault(row[0],0)
    for seqname in dict_seq_online.keys():
        str_select = "select show_create_sequence( )".split()[0] + ' ' + "select show_create_sequence( )".split()[1] + "'" + seqname + "'" + "select show_create_sequence( )".split()[2]
        rows = db_exe(conn=conn_dev, operate=str_select)
        dict_seq_online.update({seqname: rows[0][0]})
    seq_list=[]
    if service == 'public':
        seq_list=S_public_seq
    if service == 'private':
        seq_list=S_private_seq


    for seqname in seq_list:
        if seqname in dict_seq_online.keys():
            if dict_seq[seqname] == dict_seq_online[seqname]:
                pass
            else:
                log_seq.append(seqname + '此序列发生更新')
                rows=db_exe(conn=conn_dev,operate="select currval('"+seqname+"');")     #查询序列当前值
                db_exe(conn=conn_dev,operate="DROP SEQUENCE"+seqname)                   #删除该序列
                db_exe(conn=conn_dev,operate=dict_seq[seqname])                         #新建序列
                str="select setval('"+seqname+"',"+rows[0][0]+");"
                db_exe(conn=conn_dev,operate=str)                                       #修改序列当前值
        else:
            db_exe(conn=conn_dev, operate=dict_seq[seqname])

def update_index(service,conn_db,conn_dev):
    log_index = []
    dict_index=show_create_index(conn_db)
    dict_index_online=dict()
    rows = db_exe(conn=conn_dev, operate="SELECT indexname from pg_indexes")
    for row in rows:
        dict_index_online.setdefault(row[0],0)
    for indexname in dict_index_online.keys():
        str_select = "select show_create_index( )".split()[0] + ' ' + "select show_create_index( )".split()[1] + "'" + indexname + "'" + "select show_create_index( )".split()[2]
        rows = db_exe(conn=conn_dev, operate=str_select)
        dict_index_online.update({indexname: rows[0][0]})
    index_list=[]
    if service == 'public':
        index_list=S_public_index
    if service == 'private':
        index_list=S_private_index

    for indexname in index_list:
        if indexname in dict_index_online.keys():
            if dict_index[indexname] == dict_index_online[indexname]:
                pass
            else:
                log_index.append(indexname+'该索引需要被更新')
                db_exe(conn=conn_dev,operate='DROP INDEX'+indexname)
                db_exe(conn=conn_dev,operate=dict_index[indexname])   #删除并新建索引
        else:
            db_exe(conn=conn_dev, operate=dict_index[indexname])       #新建索引




def update_table(service):
    pass





