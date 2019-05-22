import sys

from connect_pg.db_select import *
from diff import conn_tmp , conn_online
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
            if dict_trigger[triggername] == dict_trigger_online[triggername]:
                pass
            else:
                log_trigger.append(triggername+'此触发器发生更新')
                str = dict_trigger[triggername]
                str_r = str.replace('CREATE TRIGGER', 'CREATE OR REPLACE TRIGGER')
                db_exe(conn=conn_dev, operate=str_r)
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
        if seqname in dict_seq_online.keys():                           # 可以找到，判断内容
            if dict_seq[seqname] == dict_seq_online[seqname]:           # 内容一样，不做变化
                pass
            else:
                log_seq.append(seqname + '此序列发生更新')
                """
                这里更新序列
                str = dict_func[func_name]
                str_r = str.replace('CREATE FUNCATION', 'CREATE OR REPLACE FUNCATION')
                db_exe(conn=conn_dev, operate=str_r)
                
                """
        else:
            db_exe(conn=conn_dev, operate=dict_seq[seqname])                # 模板库的序列在线上找不到，新建







def update_index(service,conn_tmp,conn_oli):
    log_index=[]
    dict_index=show_create_index(conn_db)
    dict_index_online=dict()


def update_table(service):
    pass


def contrast_database():
    contrast_oskey("public",0)
    contrast_custom(3, 0, None, 1, 2)  # 知鱼用户
    contrast_custom(3, 0, None, 1, 2)  # max 用户


def contrast_custom(oli_type,oli_status,oli_create_time,tmp_is_private,tmp_custom_type):
    oskey_list=select_oskey(conn_online,oli_type,oli_status,oli_create_time);
    for oskey in  oskey_list :
        contrast_oskey(oskey,tmp_is_private,tmp_custom_type)


def contrast_oskey(oskey, is_private = None, custom_type = None):
    tmp_list = select_object(conn_tmp,is_private,3,custom_type)
    oli_list = select_tablename(conn_online,oskey)
    for tmp_tablename in tmp_list:
        if oskey != "public":
            tablename_prefix = tmp_tablename[0:tmp_tablename.find(tmp_tablename.split("_")[-1])]
            oli_tablename = tablename_prefix + oskey
        elif oskey == "public":
            oli_tablename = tmp_tablename

        if oli_list.index(oli_tablename) > 0:
            contrast_table(tmp_tablename,oli_tablename)
        else:
            print("表不存在，新建表")
            continue
        oli_list.pop(oli_tablename)
    if len(oli_list) > 0:
        print("线上表多余，是否要删除")


def contrast_table(tmp_tablename,oli_tablename):
    contrast_table_columns(tmp_tablename, oli_tablename)
    contrast_table_constraint(tmp_tablename, oli_tablename)
    contrast_table_remarks(tmp_tablename, oli_tablename)


def contrast_table_columns(tmp_tablename,oli_tablename):
    tmp_column_dist = select_table_columns(conn_tmp,tmp_tablename);
    oli_column_dist = select_table_columns(conn_online,oli_tablename);
    for column_name in tmp_column_dist.keys():
        tmp_column = tmp_column_dist.get(column_name)
        oli_column = oli_column_dist.get(column_name,False)
        if oli_column == False:
            print("字段不存在")
            continue
        if tmp_column.type != oli_column.type:
            print("字段类型不一致")
        if tmp_column.varchar_len != oli_column.varchar_len:
            print("字段varchar_len不一致")
        if tmp_column.double_len != oli_column.double_len:
            print("字段double_len不一致")
        if tmp_column.collate != oli_column.collate:
            print("字段collate不一致")
        if tmp_column.is_nullable != oli_column.is_nullable:
            print("字段is_nullable不一致")
        if tmp_column.default != oli_column.default:
            print("字段default不一致")
        oli_column_dist.pop(column_name)
    if len(oli_column_dist) > 0:
        print("线上字段多余，是否要删除")


def contrast_table_constraint(tmp_tablename,oli_tablename):
    tmp_constraint_dist = select_table_constraint(conn_tmp,tmp_tablename);
    oli_constraint_dist = select_table_constraint(conn_online,oli_tablename);
    for constraint_name in  tmp_constraint_dist.keys():
        tmp_constraint = tmp_constraint_dist.get(constraint_name)
        oli_constraint = oli_constraint_dist.get(constraint_name,False)
        if oli_constraint == False:
            print("约束不存在")
            continue
        if tmp_constraint.ck == oli_constraint.ck:
            print("约束ck不一致")
        if tmp_constraint.uk == oli_constraint.uk:
            print("约束uk不一致")
        if tmp_constraint.pk == oli_constraint.pk:
            print("约束pk不一致")
        if tmp_constraint.fk == oli_constraint.fk:
            print("约束fk不一致")
        oli_constraint_dist.pop(constraint_name)
    if len(oli_constraint_dist) > 0:
        print("线上索引多余，是否要删除")



def contrast_table_remarks(tmp_tablename,oli_tablename):
    tmp_remark_dist = select_table_remarks(conn_tmp,tmp_tablename)
    oli_remark_dist = select_table_remarks(conn_online, oli_tablename)
    for remark_name in tmp_remark_dist.keys():
        tmp_remark = tmp_remark_dist.get(remark_name)
        oli_remark = oli_remark_dist.get(remark_name,False)
        if oli_remark == False:
            print("备注不存在")
            continue
        if tmp_remark.dist == oli_remark.dist:
            print("备注dist不一致")
        oli_remark_dist.pop(remark_name)
    if len(oli_remark_dist) > 0:
        print("线上备注多余，是否要删除")






