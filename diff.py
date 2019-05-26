from select_service.service import *
import sys,getopt
from tools.show_temp_list__dict import *
sys.path.append("D:\\project_pg_diff\\connect_pg")


def main():
    try:
        opts,args=getopt.getopt(sys.argv[1:], 'a:n:u')
        conn_tmp=db_conn('db_template')
        conn_online=db_conn('db_online')
        for name,value in opts:
            if name == '-a':
                os_key = 'public'
                new_add_user(conn_tmp, conn_online, os_key)
            if name == '-n':
                custom_type = input("please input custom_type:")
                new_add_user(conn_tmp, conn_online, value, custom_type)
            if name == '-u':
                online_update(conn_tmp, conn_online)
        db_close(conn_tmp)
        db_close(conn_online)
    except getopt.GetoptError:
        print(sys.argv[0] + ' -a <add user>[os_key]  -n <new user>[os_key]  -u<update online> [--private|--public]')
    except Exception as result:
        print("数据库error：%s "%result)


if __name__ == '__main__':
        main()


#程前