# import quickbase
import datetime

import __init__ as quickbase
from quickbase import generate_quickbase_app



if __name__ == '__main__':

    CIC = quickbase.QuickbaseApp()
    CIC.roles = quickbase.getTableFIDDict(CIC, 'roles', return_alphanumeric=True)
    clist = [CIC.roles[x] for x in CIC.roles if int(CIC.roles[x])%10==0]
    # clist = '1.2.3.4.5.6.7.8.9'
    fuckery = quickbase.QuickbaseAction(CIC, 'roles', 'query', query="{1.OAF.'"+str(quickbase.DateToEpoch(datetime.date(year=2020, month=1, day=1)))+"'}", clist=clist)

    fuckery.performAction()
    print(fuckery)
    # key_field_labels = ['record_id_',
    #                     'role___person___full_name',
    #                     'role___client',
    #                     'role___client___community',
    #                     'key_ext_id',
    #                     'role___client___main_office_region',
    #                     'role___client___main_office_floor',
    #                     'snapshot___full_name',
    #                     'date_issued',
    #                     'date_returned_or_deactivated',
    #                     'role___person']
    # CIC = quickbase.QuickbaseApp()
    # CIC.keys = quickbase.getTableFIDDict(CIC, 'keys', return_alphanumeric=True)
    # end_date = datetime.date(year=2022, month=1, day=1)
    # start_date = datetime.date(year=2010, month=1, day=1)
    # key_query = "{" + CIC.keys['type'] + ".EX.'CIC Salto fob'}AND" \
    #                                      "{" + CIC.keys['date_issued'] + ".OBF.'" + str(
    #     quickbase.DateToEpoch(end_date)) + "'}AND" \
    #                                        "({" + CIC.keys['date_returned_or_deactivated'] + ".OAF.'" + str(
    #     quickbase.DateToEpoch(start_date)) + "'}OR" \
    #                                          "{" + CIC.keys['date_returned_or_deactivated'] + ".EX.''})AND" \
    #                                                                                           "{" + CIC.keys[
    #                 'role___client___relationship_to_cic'] + ".EX.'Client'}AND" \
    #                                                          "{" + CIC.keys[
    #                 'role___billability_type'] + ".EX.'Regular'}"
    #
    # keys_clist = '3.7.19.157.233.262.266.273.11.12.18'
    # count = quickbase.QuickbaseAction(CIC, 'keys', 'query', query=key_query, clist=keys_clist)
    # count.performAction()
    # print(count)
    # # app = generate_quickbase_app()
    # #
    # # query = "query={3.EX.'93'}"
    # # clist = '1.2.3.4.5.6.7.8.9'
    # # # r = quickbase.QuickbaseAction(app, 'clients', 'query', query=query, clist=clist)
    # # clist = '3.778.94.306.147.301.244.34.125'
    # # r = quickbase.QuickbaseAction(app, 'clients', 'edit', clist=clist, data={'8060': ['Health', 'foobsdfasdfcar foasdfo', '123 abc asdfst', 'bar asdffoo', 'Client', 'rsmith@cic.com', str(quickbase.DateToEpoch(datetime.date.today())), 'Internet Search']})
    # # # r = quickbase.QuickbaseAction(app, 'clients', 'querycount', query=query, clist=clist)
    # # # r = quickbase.QuickbaseAction(app, 'keys', 'purge', query=query, confirmation=True)
    # #
    # # s = r.performAction()
    # # print(r)
