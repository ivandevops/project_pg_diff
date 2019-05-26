import sys
sys.path.append("D:\\project_pg_diff\\tools")
from tools.update import *
from tools.create import *

def online_update(conn_tmp, conn_online):
    update_table(conn_tmp, conn_online, 'public')
    update_table(conn_tmp, conn_online, 'private')
    # update_func()
    # update_seq('public')
    # update_table('public')
    # update_index('public')
    # update_trigger('public')
    # update_seq('private')
    # update_table('private')
    # update_index('private')
    # update_trigger('private')
    # update_view()


def new_add_user(conn_tmp, conn_online, os_key, custom_type=None):
    if os_key == 'public':
        create_user(conn_tmp, conn_online, 'public', 0)
    else:
        if custom_type == 0:
            create_user(conn_tmp, conn_online, os_key, 1, 0)
        if custom_type == 1:
            create_user(conn_tmp, conn_online, os_key, 1, 1)
        if custom_type == 2:
            create_user(conn_tmp, conn_online, os_key, 1, 2)
