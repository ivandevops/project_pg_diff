import sys

from connect_pg.db_select import *
from tools.show_temp_list__dict import *
sys.path.append("D:\\project_pg_diff\\connect_pg")


DROP_OSKEY = False
DROP_TABLE = False
DROP_TABLE_COLUMN = False
DROP_TABLE_CONSTRAINT = False
DROP_TABLE_REMARK = False


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










def update_table(conn_tmp, conn_online, service):
    if service == 'public':
        contrast_oskey(conn_tmp, conn_online, "public", 0)
    elif service == 'private':
        contrast_custom(conn_tmp, conn_online, 3, 0, None, 1, 2)  # 知鱼用户
        # contrast_custom(conn_tmp, conn_online, 3, 0, None, 1, 2)  # max 用户
    contrast_surplus_oskey(conn_tmp, conn_online)


# 对比线上库是否，存在dataos中没有的oskey
def contrast_surplus_oskey(conn_tmp, conn_online):
    oskey_dataos_list = select_dataos_oskey(conn_online)
    oskey_pgtables_list = select_pgtables_oskey(conn_online)
    for oskey in oskey_dataos_list:
        if oskey_pgtables_list.count(oskey) > 0:
            oskey_pgtables_list.remove(oskey)
    if len(oskey_pgtables_list) > 0:
        print("线上出现dataos表中，没有的oskey")
        if DROP_OSKEY == True:
            print("开始执行删除")
            for oskey in oskey_pgtables_list:
                tablename_pgtables_list = select_tablename(conn_online, oskey)
                for tablename in tablename_pgtables_list:
                    print("删除表--》" + tablename)
                    drop_table(conn_online, tablename)


# 对线上和模版库中不同的客户类型进行处理
def contrast_custom(conn_tmp, conn_online, oli_type,oli_status,oli_create_time,tmp_is_private,tmp_custom_type):
    oskey_dataos_list = select_dataos_oskey(conn_online,oli_type,oli_status,oli_create_time)
    for oskey in oskey_dataos_list :
        contrast_oskey(conn_tmp, conn_online, oskey, tmp_is_private, tmp_custom_type)


# 根据提供的oskey，对每一个表进行对比
def contrast_oskey(conn_tmp, conn_online, oskey, is_private = None, custom_type = None):
    tmp_tablename_list = select_object(conn_tmp, is_private, 3, custom_type)
    oli_tablename_list = select_tablename(conn_online, oskey)
    for tmp_tablename in tmp_tablename_list:
        if oskey != "public":
            oli_tablename = update_oskey_by_tmp_tablename(tmp_tablename, tmp_tablename, oskey)
        elif oskey == "public":
            oli_tablename = tmp_tablename
        if oli_tablename_list.count(oli_tablename) > 0:
            contrast_table(conn_tmp, conn_online, tmp_tablename, oli_tablename)
        else:
            print("表不存在，新建表")
            create_table(conn_tmp, conn_online, tmp_tablename, oli_tablename)
            continue
        oli_tablename_list.remove(oli_tablename)
    if len(oli_tablename_list) > 0:
        print("线上表多余，是否要删除")
        if DROP_TABLE == True:
            for oli_tablename in oli_tablename_list:
                drop_table(conn_online, oli_tablename)


# 对比单表
def contrast_table(conn_tmp, conn_online, tmp_tablename, oli_tablename):                        #传入连接。传入线上和模板的表名进行对比
    contrast_table_column(conn_tmp, conn_online, tmp_tablename, oli_tablename)
    contrast_table_constraint(conn_tmp, conn_online, tmp_tablename, oli_tablename)
    contrast_table_remark(conn_tmp, conn_online, tmp_tablename, oli_tablename)


# 对比表字段
def contrast_table_column(conn_tmp, conn_online, tmp_tablename, oli_tablename):
    tmp_column_dict = select_create_table_column(conn_tmp, tmp_tablename)
    oli_column_dict = select_create_table_column(conn_online, oli_tablename)
    for column_name in tmp_column_dict.keys():
        tmp_column = tmp_column_dict.get(column_name)
        oli_column = oli_column_dict.get(column_name, False)
        if oli_column == False:
            print("字段不存在")
            alter_table_add_column(conn_online, oli_tablename, tmp_column)
            continue
        oli_column_dict.pop(column_name)
        if tmp_column.type != oli_column.type:
            print("字段类型不一致")
            alter_table_alter_column(conn_online, oli_tablename, tmp_column)
            continue
        if tmp_column.varchar_len != oli_column.varchar_len:
            print("字段varchar_len不一致")
            alter_table_alter_column(conn_online, oli_tablename, tmp_column)
            continue
        if tmp_column.double_len != oli_column.double_len:
            print("字段double_len不一致")
            alter_table_alter_column(conn_online, oli_tablename, tmp_column)
            continue
        if tmp_column.collate != oli_column.collate:
            print("字段collate不一致")
            alter_table_alter_column(conn_online, oli_tablename, tmp_column)
            continue
        if tmp_column.is_nullable != oli_column.is_nullable:
            print("字段is_nullable不一致")
            alter_table_alter_column_nullable(conn_online, oli_tablename, tmp_column)
            continue
        if tmp_column.default != oli_column.default:
            if tmp_column.default.find("nextval") > 0 :
                tmp_default = update_oskey_by_tablename(tmp_column.default, tmp_tablename, oli_tablename)
                if tmp_default != oli_column.default:
                    print("字段default不一致")
                    alter_table_alter_column(conn_online, oli_tablename, tmp_column)
                    continue
            else:
                print("字段default不一致------情况特殊")
                continue
    if len(oli_column_dict) > 0:
        print("线上字段多余，是否要删除")
        if DROP_TABLE_COLUMN == True:
            for column_name in oli_column_dict.keys():
                oli_column = oli_column_dict.get(column_name)
                alter_table_drop_remarks(conn_online, oli_tablename, oli_column)


# 对比表约束
def contrast_table_constraint(conn_tmp, conn_online, tmp_tablename, oli_tablename):
    tmp_constraint_dict = select_create_table_constraint(conn_tmp, tmp_tablename)
    oli_constraint_dict = select_create_table_constraint(conn_online, oli_tablename)
    for constraint_name in tmp_constraint_dict.keys():
        tmp_constraint = tmp_constraint_dict.get(constraint_name)
        oli_constraint = oli_constraint_dict.get(update_oskey_by_tablename(constraint_name, tmp_tablename, oli_tablename), False)
        if oli_constraint == False:
            print("约束不存在")
            tmp_constraint.name = update_oskey_by_tablename(tmp_constraint.name, tmp_tablename, oli_tablename)
            alter_table_add_constraint(conn_online, oli_tablename, tmp_constraint)
            continue
        oli_constraint_dict.pop(oli_constraint.name)
        if tmp_constraint.ck != oli_constraint.ck:
            print("约束ck不一致")
            alter_table_alter_constraint(conn_online, oli_tablename, tmp_constraint)
            continue
        if tmp_constraint.uk != oli_constraint.uk:
            print("约束uk不一致")
            alter_table_alter_constraint(conn_online, oli_tablename, tmp_constraint)
            continue
        if tmp_constraint.pk != oli_constraint.pk:
            print("约束pk不一致")
            alter_table_alter_constraint(conn_online, oli_tablename, tmp_constraint)
            continue
        if tmp_constraint.fk != oli_constraint.fk:
            print("约束fk不一致")
            alter_table_alter_constraint(conn_online, oli_tablename, tmp_constraint)
            continue
    if len(oli_constraint_dict) > 0:
        print("线上索引多余，是否要删除")
        if DROP_TABLE_CONSTRAINT == True:
            for constraint_name in oli_constraint_dict.keys():
                oli_constraint = oli_constraint_dict.get(constraint_name)
                alter_table_drop_constraint(conn_online, oli_tablename, oli_constraint)


# 对比表备注
def contrast_table_remark(conn_tmp, conn_online, tmp_tablename, oli_tablename):
    tmp_remark_dict = select_create_table_remark(conn_tmp, tmp_tablename)
    oli_remark_dict = select_create_table_remark(conn_online, oli_tablename)
    for remark_name in tmp_remark_dict.keys():
        tmp_remark = tmp_remark_dict.get(remark_name)
        oli_remark = oli_remark_dict.get(remark_name, False)
        if oli_remark == False:
            print("备注不存在")
            alter_table_add_remarks(conn_online, oli_tablename, tmp_remark)
            continue
        oli_remark_dict.pop(remark_name)
        if tmp_remark.desc != oli_remark.desc:
            print("备注dict不一致")
            alter_table_alter_remarks(conn_online, oli_tablename, tmp_remark)
            continue
    if len(oli_remark_dict) > 0:
        print("线上备注多余，是否要删除")
        if DROP_TABLE_REMARK == True:
            for remark_name in oli_remark_dict.keys():
                oli_remark = oli_remark_dict.get(remark_name)
                alter_table_drop_remarks(conn_online, oli_tablename, oli_remark)


# 创建表
def create_table(conn_tmp, conn_online, tmp_tablename, oli_tablename):
    tmp_column_dict = select_create_table_column(conn_tmp, tmp_tablename)
    tmp_constraint_dict = select_create_table_constraint(conn_tmp, tmp_tablename)
    tmp_remark_dict = select_create_table_remark(conn_tmp, tmp_tablename)
    for column_name in tmp_column_dict.keys():
        tmp_column = tmp_column_dict.get(column_name)
        if tmp_column.default != None and tmp_column.default.find("nextval") > 0 :
            tmp_column.default = update_oskey_by_tablename(tmp_column.default, tmp_tablename, oli_tablename)
            tmp_column_dict.update({column_name: tmp_column})
    for constraint_name in tmp_constraint_dict.keys():
        tmp_constraint = tmp_constraint_dict.get(constraint_name)
        tmp_constraint.name = update_oskey_by_tablename(tmp_constraint.name, tmp_tablename, oli_tablename)
        tmp_constraint_dict.update({constraint_name: tmp_constraint})
    create_table_dict(conn_online, oli_tablename, tmp_column_dict, tmp_constraint_dict, tmp_remark_dict)


# 替换对象绑定的oskey序列名
def update_oskey(obj_name, tmp_oskey, oli_oskey):
    return obj_name.replace(tmp_oskey.upper(), oli_oskey.upper()).replace(tmp_oskey.lower(), oli_oskey.lower())


def update_oskey_by_tablename(obj_name, tmp_tablename, oli_tablename):
    return update_oskey(obj_name, tmp_tablename.split("_")[-1], oli_tablename.split("_")[-1])

def update_oskey_by_tmp_tablename(obj_name, tmp_tablename, oskey):
    return update_oskey(obj_name, tmp_tablename.split("_")[-1], oskey)

