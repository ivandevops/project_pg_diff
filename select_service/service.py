import sys
sys.path.append("D:\\project_pg_diff\\tools")
from tools.update import *

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

