__author__ = 'Herman'
# !/usr/bin/env python3
# -*- coding: UTF-8 -*-
import urllib.request, urllib.parse
import datetime, time
import xml.etree.ElementTree as etree
import csv


class QuickbaseApp():
    def __init__(self, baseurl, ticket, tables, token=None, **kwargs):
        """Basic unit storing useful information for communicating with Quickbase

        :param baseurl: String, https://<domain>.quickbase.com/db/
        :param ticket: String, taken from quickbase cookie
        :param tables: Dict, map of dbid labels and dbid values
        :param token: For future use
        :return:
        """
        self.base_url = baseurl
        self.ticket = ticket
        self.token = token
        self.tables = tables
        if kwargs:
            self.__dict__.update(kwargs)


class QuickbaseAction():
    def __init__(self, app, dbid_key, action, query=None, clist=None, slist=None, return_records=None, data=None,
                 skip_first="0", time_in_utc=False, confirmation=False, options=None):
        """

        :param app: class QuickbaseApp
        :param dbid_key: dbid label
        :param action: query, add, edit, qid or csv
        :return:
        """
        if time_in_utc:
            send_time_in_utc = "1"
        else:
            send_time_in_utc = "0"
        self.app = app
        if dbid_key in self.app.tables:
            self.request = urllib.request.Request(self.app.base_url + self.app.tables[dbid_key])
        else:
            self.request = urllib.request.Request(self.app.base_url + dbid_key)
        self.action_string = action.lower()
        if action.lower() == "query" or action.lower() == 'qid' or action.lower() == 'qname':
            self.action = "API_DoQuery"
        elif action.lower() == "add":
            self.action = "API_AddRecord"
        elif action.lower() == "edit" or action.lower() == "csv":
            self.action = "API_ImportFromCSV"
        elif action.lower() == "purge":
            self.action = "API_PurgeRecords"
        self.request.add_header("Content-Type", "application/xml")
        self.request.add_header("QUICKBASE-ACTION", self.action)
        self.return_records = return_records
        self.response = None
        self.slist = slist
        self.data = data
        if type(clist) == list:
            clist_string = ""
            for fid in clist:
                clist_string += fid + "."
            self.clist = clist_string[:-1]
        else:
            self.clist = clist
        self.query = query
        if self.action_string == "query" or self.action_string == "qid" or self.action_string == 'qname':
            if self.query:
                if "query=" in self.query or "qid=" in self.query or "qname=" in self.query:
                    v, self.query = self.query.split("=", 1)
                if self.slist == "0":
                    self.data = """
                        <qdbapi>
                            <ticket>%s</ticket>
                            <%s>%s</%s>
                            """ % (self.app.ticket, self.action_string, self.query, self.action_string)
                    if clist:
                        self.data = self.data + """<clist>%s</clist>
                        """ % (self.clist)

                else:
                    self.data = """
                        <qdbapi>
                            <ticket>%s</ticket>
                            <%s>%s</%s>
                            """% (self.app.ticket, self.action_string, self.query, self.action_string)
                    if clist:
                        self.data = self.data + """<clist>%s</clist>
                        """ % (self.clist)
                    self.data = self.data + """<slist>%s</slist>
                        """ % (self.slist)

            else:
                if self.slist == "0":
                    self.data = """
                        <qdbapi>
                            <ticket>%s</ticket>
                            """% (self.app.ticket)
                    if clist:
                        self.data = self.data + """<clist>%s</clist>
                        """ % (self.clist)
                else:
                    self.data = """
                        <qdbapi>
                            <ticket>%s</ticket>
                            """ % (self.app.ticket)
                    if clist:
                        self.data = self.data + """<clist>%s</clist>
                        """ % (self.clist)
                    self.data = self.data + """<slist>%s</slist>
                        """ % (self.slist)

        elif self.action_string == "purge":
            if not confirmation:
                return "Purge requires confirmation."
            if not self.query:
                return "Purging without a query will delete all records, and is disabled."  # use qid=1 instead
            if confirmation and self.query:
                if "query=" in self.query:
                    v, self.query = self.query.split("=", 1)
                    query_type = "query"
                elif "qid=" in self.query:
                    v, self.query = self.query.split("=", 1)
                    query_type = "qid"
                else:
                    query_type = "query"
                self.data = """
                    <qdbapi>
                        <ticket>%s</ticket>
                        <%s>%s</%s>
                    """ % (self.app.ticket, query_type, self.query, query_type)

        elif self.action_string == "add":
            assert type(self.data) == dict
            recordInfo = ""
            for field in self.data:
                recordInfo += '<field fid="' + str(field) + '">' + str(self.data[field]) + "</field>\n"
            self.data = """
                <qdbapi>
                    <msInUTC>%s</msInUTC>
                    <ticket>%s</ticket>
                    %s

                """ % (send_time_in_utc, self.app.ticket, recordInfo)

        elif self.action_string == "edit" or self.action_string == "csv":
            if type(self.data) == str:
                if '"' in self.data:
                    self.data = self.data.replace('"', '""')
                    self.data = '"' + self.data + '"'
                # if "'" in item:
                #     item.replace("'", "''")
                elif "," in self.data:
                    self.data = '"' + self.data + '"'
                if '\n' in self.data  and not (self.data[0] == '"' and self.data[-1] == '"'):
                    self.data = '"' + self.data + '"'
                self.data = """
                <qdbapi>
                    <msInUTC>%s</msInUTC>
                    <ticket>%s</ticket>
                    <records_csv>
                        <![CDATA[
                            %s
                        ]]>
                    </records_csv>
                    <clist>%s</clist>
                    <skipfirst>%s</skipfirst>

                    """ % (send_time_in_utc, self.app.ticket, self.data, self.clist, skip_first)
            elif type(self.data) == list:
                # print('list found')
                csv_lines = ""
                if type(self.data[0]) == list:
                    # print('2nd list found')
                    for line in self.data:
                        for item in line:
                            assert type(item) == str
                            if '"' in item:
                                item = item.replace('"', '""')
                                item = '"' + item + '"'
                            # if "'" in item:
                            #     item.replace("'", "''")
                            elif "," in item:
                                item = '"' + item + '"'
                            if '\n' in item and not (item[0] == '"' and item[-1] == '"'):
                                item = '"' + item + '"'
                            csv_lines += item + ","
                        csv_lines = csv_lines[:-1] + "\n"
                elif type(self.data[0]) == str:
                    for item in self.data:
                        try:
                            assert type(item) == str
                        except AssertionError:
                            print("wrong item type")
                            print(type(item))
                            print(item)
                            if item is None:
                                item = ""
                            # print(data)
                        # if item == None:
                        #     item = ""
                        if '"' in item:
                            item = item.replace('"', '""')
                            item = '"' + item + '"'
                        # if "'" in item:
                        #     item.replace("'", "\'")
                        elif "," in item:
                            item = '"' + item + '"'
                            # print(item)
                        if '\n' in item and not (item[0] == '"' and item[-1] == '"'):
                            item = '"' + item + '"'
                        csv_lines += item + ","
                    csv_lines = csv_lines[:-1] + "\n"
                self.data = """
                <qdbapi>
                    <msInUTC>%s</msInUTC>
                    <ticket>%s</ticket>
                    <records_csv>
                        <![CDATA[
                            %s
                        ]]>
                    </records_csv>
                    <clist>%s</clist>
                    <skipfirst>%s</skipfirst>

                    """ % (send_time_in_utc, self.app.ticket, csv_lines, self.clist, skip_first)
            elif type(self.data) == dict:
                csv_lines = ""
                for record_id in self.data:
                    assert type(record_id) == str and type(self.data[record_id]) == list
                    line = self.data[record_id]
                    csv_lines += record_id + ','
                    for item in line:
                        assert type(item) == str
                        if '"' in item:
                            item = item.replace('"', '""')
                            item = '"' + item + '"'
                        # if "'" in item:
                        #     item.replace("'", "\'")
                        elif "," in item:
                            item = '"' + item + '"'
                        if '\n' in item and not (item[0] == '"' and item[-1] == '"'):
                            item = '"' + item + '"'
                        csv_lines += item + ","
                    csv_lines = csv_lines[:-1] + "\n"
                self.data = """
                <qdbapi>
                    <msInUTC>%s</msInUTC>
                    <ticket>%s</ticket>
                    <records_csv>
                        <![CDATA[
                            %s
                        ]]>
                    </records_csv>
                    <clist>%s</clist>
                    <skipfirst>%s</skipfirst>

                    """ % (send_time_in_utc, self.app.ticket, csv_lines, self.clist, "0")

        if options is not None:
            self.options = options
            self.data = self.data + """
            <options>%s</options>
            """ % self.options

        self.data = self.data + """
                    </qdbapi>
                        """
        self.request.data = self.data.encode('utf-8')


    def performAction(self):
        """Performs the action defined by the QuickbaseAction object, and maps the response to an attribute

        :return: response
        """
        self.content = urllib.request.urlopen(self.request).read().replace(b'<BR/>', b'')
        self.etree_content = etree.fromstring(self.content)
        if not self.action_string == 'csv':
            self.raw_response = etree.fromstring(self.content).findall('record')
            self.response = QuickbaseResponse(self.raw_response)
            self.fid_dict = dict()
            if not self.action_string == "add" and not self.action_string == "purge":
                if self.clist:
                    fid_list = self.clist.split('.')
                    try:
                        field_list = list(self.raw_response[0])
                        counter = 0
                        for fid in fid_list:
                            self.fid_dict[fid] = field_list[counter].tag
                            counter += 1
                    except IndexError:
                        self.fid_dict = None
                else:
                    self.fid_dict = None
                if not self.return_records:
                    return self.content
                else:
                    return self.raw_response
            else:
                response_dict = {'errcode': self.etree_content.find('errcode').text,
                                 'errtext': self.etree_content.find('errtext').text}
                if self.etree_content.find('rid') is not None:
                    response_dict['rid'] = self.etree_content.find('rid').text
                else:
                    response_dict['rid'] = None
                if self.etree_content.find('errdetail') is not None:
                    response_dict['errdetail'] = self.etree_content.find('errdetail').text
                else:
                    response_dict['errdetail'] = None
                return response_dict
        if self.action_string == 'csv':
            # print(self.content)
            resp = etree.fromstring(self.content)
            if resp.find('num_recs_input') is not None:
                self.num_recs_input = resp.find('num_recs_input').text
            else:
                self.num_recs_input = "0"
            if resp.find('num_recs_added') is not None:
                self.num_recs_added = resp.find('num_recs_added').text
            else:
                self.num_recs_added = "0"
            if resp.find('num_recs_updated') is not None:
                self.num_recs_updated = resp.find('num_recs_updated').text
            else:
                self.num_recs_updated = "0"
            self.rid_list = list()
            rids = etree.fromstring(self.content).find('rids')
            try:
                for rid in rids.findall('rid'):
                    self.rid_list.append(rid.text)
                return self.rid_list
            except AttributeError as err:
                print(err)
                print(self.content)


class QuickbaseResponse():
    def __init__(self, response):

        self.records = response
        self.values = []
        for record in self.records:
            record_dict = dict()
            for item in record:
                record_dict[item.tag] = item.text
            self.values.append(record_dict)


class Eastern_tzinfo(datetime.tzinfo):
    """Implementation of the Eastern timezone."""

    def utcoffset(self, dt):
        return datetime.timedelta(hours=-5) + self.dst(dt)

    def _FirstSunday(self, dt):
        """First Sunday on or after dt."""
        return dt + datetime.timedelta(days=(6 - dt.weekday()))

    def dst(self, dt):
        # 2 am on the second Sunday in March
        dst_start = self._FirstSunday(datetime.datetime(dt.year, 3, 8, 2))
        # 1 am on the first Sunday in November
        dst_end = self._FirstSunday(datetime.datetime(dt.year, 11, 1, 1))

        if dst_start <= dt.replace(tzinfo=None) < dst_end:
            return datetime.timedelta(hours=1)
        else:
            return datetime.timedelta(hours=0)

    def tzname(self, dt):
        if self.dst(dt) == datetime.timedelta(hours=0):
            return "EST"
        else:
            return "EDT"


class UTC(datetime.tzinfo):
    def utcoffset(self, dt):
        return datetime.timedelta(hours=0)

    def dst(self, dt):
        return datetime.timedelta(hours=0)

    def tzname(self, dt):
        return "UTC"


def generateTableDict(import_filename):
    table_dict = dict()
    with open(import_filename, 'r') as csv_file:
        r = csv.reader(csv_file)
        for row in r:
            table_dict[row[1]] = row[2]
            table_dict[row[1].lower()] = row[2]
    return table_dict


def QBQuery(url, ticket, dbid, request, clist, slist="0", returnRecords=False):
    """
    This function takes the base Quickbase URL, an authentication ticket, a database ID (DBID), a query and a clist, and
    returns an XML file.
    URL format: string, 'https://<basedomain>.quickbase.com/db/
    Authentication ticket: You can get this from your Quickbase cookie in your browser. This should eventually be
    changed to use a ticket and token
    DBID: the ID of the table you want to reference (what comes after the /db/ and before the ?act=)
    Query:query={CONDITIONS}. Should not contain any HTML encoding
    Clist: a period-separated list of fields you want returned
    """
    query = urllib.request.Request(url + dbid)
    query.add_header("Content-Type", "application/xml")
    query.add_header("QUICKBASE-ACTION", "API_DoQuery")
    if "query=" in request:
        v, request = request.split("=", 1)
    if slist == "0":
        data = """
            <qdbapi>
                <msInUTC>%s</msInUTC>
                <ticket>%s</ticket>
                <query>%s</query>
                <clist>%s</clist>
            </qdbapi>
        """ % ('0', ticket, request, clist)
    else:
        data = """
            <qdbapi>
                <msInUTC>%s</msInUTC>
                <ticket>%s</ticket>
                <query>%s</query>
                <clist>%s</clist>
                <slist>%s</slist>
            </qdbapi>
        """ % ('0', ticket, request, clist, slist)
    query.data = data.encode('utf-8')
    content = urllib.request.urlopen(query).read()
    if not returnRecords:
        return content
    else:
        return etree.fromstring(content).findall('record')


def QBAdd(url, ticket, dbid, fieldValuePairs):
    """
    This function adds a record in Quickbase. fieldValuePairs should be a dictionary of fid and values, and must include
    all required fields (especially related client).
    fieldValuePairs must use fid values as key, not field names
    """
    query = urllib.request.Request(url + dbid)
    query.add_header("Content-Type", "application/xml")
    query.add_header("QUICKBASE-ACTION", "API_AddRecord")
    recordInfo = ""
    for field in fieldValuePairs:
        recordInfo += '<field fid="' + str(field) + '">' + str(fieldValuePairs[field]) + "</field>\n"
    data = """
        <qdbapi>
            <msInUTC>%s</msInUTC>
            <ticket>%s</ticket>
            %s
        </qdbapi>
    """ % ('0', ticket, recordInfo)

    query.data = data.encode('utf-8')
    response = urllib.request.urlopen(query)
    return response


def EpochToDate(epochTime, include_time=False, convert_to_eastern_time=False):
    """
    Takes a Quickbase-generated time value (ms since the start of the epoch) and returns a datetime.date object
    If pulling directly from Quickbase, should be converted to eastern time
    """

    if epochTime and not include_time:
        tupleTime = time.gmtime(int(epochTime) / 1000)
        realDate = datetime.date(tupleTime.tm_year, tupleTime.tm_mon, tupleTime.tm_mday)
        return realDate
    elif epochTime and include_time:
        tupleTime = time.gmtime(int(epochTime) / 1000)
        realDateTime = datetime.datetime(tupleTime.tm_year, tupleTime.tm_mon, tupleTime.tm_mday, tupleTime.tm_hour,
                                         tupleTime.tm_min, tupleTime.tm_sec, tzinfo=UTC())
        if convert_to_eastern_time:
            realDateTime = realDateTime.astimezone(tz=Eastern_tzinfo())
        # realDateTime = realDateTime.astimezone(tz=Eastern_tzinfo())
        return realDateTime
    else:
        return None


def DateToEpoch(regDate, include_time=False, convert_to_eastern_time=False):
    """
    takes a datetime object and returns an epoch time integer in a format that
    quickbase can use. Assumes time in localtime and does necessary alterations to make it work with UTC if
    convert_to_eastern_time is true
    """

    if not include_time:
        date_object = datetime.datetime(regDate.year, regDate.month, regDate.day, tzinfo=UTC())
        if convert_to_eastern_time:
            utc_date_object = date_object.astimezone(tz=Eastern_tzinfo())
            # structTime = time.strptime(str(date_object.year) + str(date_object.month) + str(date_object.day) + " " +
            #                            str(date_object.tzinfo),
            #                            "%Y%m%d %Z")
            epochTime = int(time.mktime(utc_date_object.timetuple()) * 1000)
        else:
            epochTime = int(time.mktime(date_object.timetuple()) * 1000)
    else:
        datetime_object = datetime.datetime(regDate.year, regDate.month, regDate.day, regDate.hour, regDate.minute,
                                            regDate.second, tzinfo=UTC())
        if convert_to_eastern_time:
            utc_datetime_object = datetime_object.astimezone(tz=Eastern_tzinfo())
            # structTime = time.strptime(str(date_object.year) + str(date_object.month) + str(date_object.day) + " "
            #                            + str(date_object.hour) + ":" + str(date_object.minute) + ":"
            #                            + str(date_object.second) + " " + str(date_object.tzinfo),
            #                            "%Y%m%d %H:%M:%S %Z")
            epochTime = int(time.mktime(utc_datetime_object.timetuple()) * 1000)
        epochTime = int(time.mktime(datetime_object.timetuple()) * 1000)
    return (epochTime)


def MonthDict(testDate):
    """
    This function takes a date and returns a dictionary and an array to allow referencing the length of the month from
    the date
    """
    if (testDate.year % 4 == 0):
        monthLengthDict = {'Jan': 31, 'Feb': 29, 'Mar': 31, 'Apr': 30, 'May': 31, 'Jun': 30, 'Jul': 31,
                           'Aug': 31, 'Sep': 30, 'Oct': 31, 'Nov': 30, 'Dec': 31,
                           1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31,
                           8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
        monthLength = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    else:
        monthLengthDict = {'Jan': 31, 'Feb': 28, 'Mar': 31, 'Apr': 30, 'May': 31, 'Jun': 30, 'Jul': 31,
                           'Aug': 31, 'Sep': 30, 'Oct': 31, 'Nov': 30, 'Dec': 31,
                           1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31,
                           8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
        monthLength = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    return (monthLengthDict, monthLength)


def QBEdit(url, ticket, dbid, rid, field, value):
    query = urllib.request.Request(url + dbid)
    query.add_header("Content-Type", "application/xml")
    query.add_header("QUICKBASE-ACTION", "API_EditRecord")
    data = """
        <qdbapi>
            <msInUTC>%s</msInUTC>
            <ticket>%s</ticket>
            <rid>%s</rid>
            <field fid="%s">%s</field>
        </qdbapi>
    """ % ('0', ticket, rid, field, value)
    query.data = data.encode('utf-8')
    response = urllib.request.urlopen(query)
    return response


def UploadCsv(url, ticket, dbid, csvData, clist, skipFirst=0):
    """ Given a csv-formatted string, list, or dict, upload records to Quickbase

    :param url: base url (https://<domain>.quickbase.com/db/
    :param ticket: authentication ticket
    :param dbid: Table ID
    :param csvData: data to upload. Can be a string of comma-separated values with line breaks, a list, a list of lists,
    or a dict. If it is a dict, the key is included as the first item in each line of the csv, and the value must be a
    list
    :param clist: string of period-separated field IDs mapping the CSV data to fields in Quickbase
    :param skipFirst: If 1, the first line is skipped (useful if uploading a csv which contains labels). Will always be
    0 when uploading a dict, because dicts are unordered.
    :return: response contains troubleshooting information including error code and value, count of records added,
    and count of records edited.
    """
    request = urllib.request.Request(url + dbid)
    request.add_header("Content-Type", "application/xml")
    request.add_header("QUICKBASE-ACTION", "API_ImportFromCSV")
    if type(csvData) == str:
        data = """
        <qdbapi>
            <msInUTC>%s</msInUTC>
            <ticket>%s</ticket>
            <records_csv>
                <![CDATA[
                    %s
                ]]>
            </records_csv>
            <clist>%s</clist>
            <skipfirst>%s</skipfirst>
        </qdbapi>
            """ % ('0', ticket, csvData, clist, skipFirst)
    elif type(csvData) == list:
        csv_lines = ""
        if type(csvData[0]) == list:
            for line in csvData:
                for item in line:
                    assert type(item) == str
                    csv_lines += item + ","
                csv_lines = csv_lines[:-1] + "\n"
        elif type(csvData[0]) == str:
            for item in csvData:
                assert item == str
                csv_lines += item + ","
            csv_lines = csv_lines[:-1] + "\n"
        data = """
        <qdbapi>
            <msInUTC>%s</msInUTC>
            <ticket>%s</ticket>
            <records_csv>
                <![CDATA[
                    %s
                ]]>
            </records_csv>
            <clist>%s</clist>
            <skipfirst>%s</skipfirst>
        </qdbapi>
            """ % ('0', ticket, csv_lines, clist, skipFirst)
    elif type(csvData) == dict:
        csv_lines = ""
        for record_id in csvData:
            assert type(record_id) == str and type(csvData[record_id]) == list
            line = csvData[record_id]
            csv_lines += record_id + ','
            for item in line:
                assert type(item) == str
                csv_lines += item + ","
            csv_lines = csv_lines[:-1] + "\n"
        data = """
        <qdbapi>
            <msInUTC>%s</msInUTC>
            <ticket>%s</ticket>
            <records_csv>
                <![CDATA[
                    %s
                ]]>
            </records_csv>
            <clist>%s</clist>
            <skipfirst>%s</skipfirst>
        </qdbapi>
            """ % ('0', ticket, csv_lines, clist, "0")
    else:
        return None
    request.data = data.encode('utf-8')
    response = urllib.request.urlopen(request).read()
    return response


def DownloadCSV(base_url, ticket, dbid, report_id, file_name="report.csv"):
    csv_file = file_name
    urllib.request.urlretrieve(base_url + dbid + "?a=q&qid=" + str(report_id) + "&dlta=xs%7E&ticket=" + ticket,
                               csv_file)


def csvSort(input_file, output_file, sort_keys=[0], contains_labels=False, format='utf-8', quotechar=None, delimiter=None):
    """Takes an input csv filename, sorts it, and writes to output_file
    :param input_file: The file to be read from
    :param output_file: The file to write to
    :param sort_keys: A list of position indices to sort (from highest to lowest sort level)
    :param contains_labels: Whether the first line is column labels
    :param format: blank for utf-8, otherwise a string containing the formatting
    :return:
    """
    if delimiter is None:
        delimiter = ","
    if quotechar is None:
        quotechar = '"'
    with open(input_file, 'r', newline='', encoding=format) as csv_input_file:
        r = csv.reader(csv_input_file, quotechar=quotechar, delimiter=delimiter)
        unsorted_lines = []
        # if contains_labels:
        #     file_labels = next(r)
        # else:
        #     file_labels = None
        first_line = True

        for line in r:
            if first_line and contains_labels:
                file_labels = line
                first_line = False
            else:
                try:
                    unsorted_lines.append(line)
                except UnicodeDecodeError as err:
                    print(err)
                    print(line)
                    print(r)
        sorted_lines = unsorted_lines
        sort_keys.reverse()
        for sort_key in sort_keys:
            try:
                int(sorted_lines[0][sort_key])
                sorted_lines = sorted(sorted_lines, key=lambda item: int(item[sort_key]))
            except ValueError:
                # print("sorting on string")
                sorted_lines = sorted(sorted_lines, key=lambda item: item[sort_key].lower())
            except IndexError:
                print(sorted_lines[0])
                print(sort_key)
                print(len(sorted_lines[0]))
                # else:
                #     # print("not a string")
                #     sorted_lines = sorted(sorted_lines, key=lambda item: item[sort_key])
    with open(output_file, 'w', newline='', encoding='utf-8') as csv_output_file:
        w = csv.writer(csv_output_file, quotechar=quotechar, delimiter=delimiter)
        if file_labels:
            w.writerow(file_labels)
        for line in sorted_lines:
            w.writerow(line)

def downloadFile(dbid, ticket, rid, fid, filename, vid='0', baseurl='https://cictr.quickbase.com/'):
    request = urllib.request.Request(
        baseurl + 'up/' + dbid + '/a/r' + rid + '/e' + fid + '/v' + vid + '?ticket=' + ticket)
    response = urllib.request.urlopen(request).read()
    with open(filename, 'wb') as downloaded_file:
        downloaded_file.write(response)
