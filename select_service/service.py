import sys
sys.path.append("D:\\project_pg_diff\\tools")
from tools.update import *

def online_update():
    update_func()
    update_seq('public')
    update_table('public')
    update_index('public')
    update_trigger('public')
    update_seq('private')
    update_table('private')
    update_index('private')
    update_trigger('private')
    update_view()

