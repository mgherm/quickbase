# import quickbase
import datetime

import __init__ as quickbase


def generate_quickbase_app(file='CIC.cfg'):
    """
    Generates a quickbase.QuickbaseApp to be used in all queries against the quickbase database
    :return CIC: quickbase.QuickbaseApp with all necessary parameters to perform queries and actions
    """
    cic_tables = quickbase.generateTableDict(file)
    baseUrl = "https://cictr.quickbase.com/db/"
    ticket = cic_tables['ticket']
    CIC = quickbase.QuickbaseApp(baseUrl, ticket, cic_tables)
    return CIC

if __name__ == '__main__':
    r = datetime.datetime.now()
    s = quickbase.DateToEpoch(r, include_time=True, convert_to_eastern_time=True)
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
