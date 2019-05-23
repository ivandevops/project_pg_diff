from tools.create_user import *
import sys,getopt
from tools.show_temp_list__dict import *
sys.path.append("D:\\project_pg_diff\\connect_pg")


def main():
    try:
        opts,args=getopt.getopt(sys.argv[1:],'a:n:u')
        conn_tmp=db_conn('db')
        conn_online=db_conn('db_dev')
        for name,value in opts:
            if name == '-a':
                judge(conn=conn_tmp)
                add(value,conn_db=conn_tmp,conn_dev=conn_online)                     #增加私有组件
            if name == '-n':
                judge(conn_tmp)
                new(value,conn_1=conn_tmp,conn_2=conn_online)
            if name == '-u':
                pass
        db_close(conn_tmp)
        db_close(conn_online)
    except getopt.GetoptError:
        print(sys.argv[0] + ' -a <add user>[os_key]  -n <new user>[os_key]  -u<update online> [--private|--public]')
    except Exception as result:
        print("数据库error：%s "%result)


if __name__ == '__main__':
        main()


