import sys
from connect_pg.db_link import *
from tools.judge import *
sys.path.append("D:\\project_pg_diff\\connect_pg")


conn=db_conn('db_dev')              #连接线上

judge()                             #扫描模板库分类存储，不同的对象分别存储成不同的列表



table_list_online=[]
dict_table=show_create_table()
dict_table_online=dict()
rows=db_exe(conn=conn,operate="select tablename from pg_tables where tablename like 'fd%'" )
for row in rows:
    dict_table_online.setdefault(row[0],0)           #线上视图字典
    table_list_online.append(row)                    #线上视图名列表
for tablename in dict_table_online.keys():
    str_select = "select show_create_table( )".split()[0] + ' ' + "select show_create_table( )".split()[1] + " 'public' " + "," + "'" + tablename + "'" + "select show_create_table( )".split()[2]
    rows = db_exe(conn=conn, operate=str_select)
    dict_table_online.update({tablename:rows[0][0]})

for tablename in S_public_table:
    if tablename in table_list_online:                                    #可以找到，判断内容
        if dict_table[tablename]==dict_table_online[tablename]:             #内容一样，不做变化
            pass
        else:
            b = dict_table[tablename].replace('" (', ',').replace(')\n;', ',').split(',')
            a = dict_table_online[tablename].replace('" (', ',').replace(')\n;', ',').split(',')
            for i in b:
                if i not in a:
                    str="alter table"+tablename+"MODIFY"+i.strip()                                                   # 注意，修改时如果不带完整性约束条件，原有的约束条件将丢失，如果想保留修改时就得带上完整性约束条件
                    db_exe(conn=conn,operate=str)


    else:
        db_exe(conn=conn,operate=dict_table[tablename])                   #模板库的函数在线上找不到，新建


for tablename in S_private_table:
    if tablename in table_list_online:                                    #可以找到，判断内容
        if dict_table[tablename]==dict_table_online[tablename]:             #内容一样，不做变化
            pass
        else:
        #pass
    else:
        db_exe(conn=conn,operate=dict_table[tablename])                   #模板库的函数在线上找不到，新建



db_close('db_dev')


