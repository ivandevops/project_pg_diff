from select_service.service import *
if __name__ == '__main__':
    service=input("PLEASE INPUT SERVICE:(add or new or update)")            #判断参数

    conn_tmp = db_conn("db")
    conn_onl = db_conn("db_dev")                                            #-----------------判断参数在连接前，参数不合法直接返回错误原因

    if conn_tmp is null ;

    if service == "add":
        add_user(conn_tmp,conn_onl,os_key)                                  #------------------连接，输入、参数
    if service == "new":
        new_user()
    if service == "uddate":                                                 #-------------------main
        online_update()

    db_close(conn_tmp)
    db_close(conn_onl)
