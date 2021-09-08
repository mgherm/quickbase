# import quickbase

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
    app = generate_quickbase_app()
    query = "query={3.EX.'93'}"
    clist = '1.2.3.4.5.6.7.8.9'
    r = quickbase.QuickbaseAction(app, 'clients', 'query', query=query, clist=clist)
    s = r.performAction()
    print(r)
