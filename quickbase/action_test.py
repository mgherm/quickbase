# import quickbase
import datetime

import __init__ as quickbase
from quickbase import generate_quickbase_app



if __name__ == '__main__':
    CIC = generate_quickbase_app('quickbase\CIC.cfg')
    CIC.roles = quickbase.getTableFIDDict(CIC, 'roles', return_alphanumeric=True)
    clist = [CIC.roles['friend_of_cic_c3_host'], '3']
    focic_roles = quickbase.QuickbaseAction(CIC, 'roles', 'query', query="{"+CIC.roles['price___qb_item_description']+".CT.'Friend of'}", clist=clist)
    focic_roles.performAction()
    print(focic_roles)
    # app = generate_quickbase_app()
    #
    # query = "query={3.EX.'93'}"
    # clist = '1.2.3.4.5.6.7.8.9'
    # # r = quickbase.QuickbaseAction(app, 'clients', 'query', query=query, clist=clist)
    # clist = '3.778.94.306.147.301.244.34.125'
    # r = quickbase.QuickbaseAction(app, 'clients', 'edit', clist=clist, data={'8060': ['Health', 'foobsdfasdfcar foasdfo', '123 abc asdfst', 'bar asdffoo', 'Client', 'rsmith@cic.com', str(quickbase.DateToEpoch(datetime.date.today())), 'Internet Search']})
    # # r = quickbase.QuickbaseAction(app, 'clients', 'querycount', query=query, clist=clist)
    # # r = quickbase.QuickbaseAction(app, 'keys', 'purge', query=query, confirmation=True)
    #
    # s = r.performAction()
    # print(r)
