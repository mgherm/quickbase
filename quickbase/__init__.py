__author__ = 'Herman'
#!/usr/bin/env python3
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
                 skip_first="0"):
        """

        :param app: class QuickbaseApp
        :param dbid_key: dbid label
        :param action: query, add, edit or csv
        :return:
        """
        self.app = app
        if dbid_key in self.app.tables:
            self.request = urllib.request.Request(self.app.base_url + self.app.tables[dbid_key])
        else:
            self.request = urllib.request.Request(self.app.base_url + dbid_key)
        self.action_string = action.lower()
        if action.lower() == "query":
            self.action = "API_DoQuery"
        elif action.lower() == "add":
            self.action = "API_AddRecord"
        elif action.lower() == "edit" or action.lower() == "csv":
            self.action = "API_ImportFromCSV"
        self.request.add_header("Content-Type", "application/xml")
        self.request.add_header("QUICKBASE-ACTION", self.action)
        self.return_records = return_records
        self.response = None
        self.clist = clist
        if self.action_string == "query":
            if "query=" in query:
                v, query = query.split("=", 1)
            if slist == "0":
                self.data = """
                <qdbapi>
                <ticket>%s</ticket>
                <query>%s</query>
                <clist>%s</clist>
                </qdbapi>
                """ % (self.app.ticket, query, clist)
            else:
                self.data = """
                <qdbapi>
                <ticket>%s</ticket>
                <query>%s</query>
                <clist>%s</clist>
                <slist>%s</slist>
                </qdbapi>
                """ % (self.app.ticket, query, clist, slist)
            self.request.data = self.data.encode('utf-8')
        elif self.action_string == "add":
            assert type(data) == dict
            recordInfo = ""
            for field in data:
                recordInfo += '<field fid="' + str(field) + '">' + str(data[field]) + "</field>\n"
            self.data = """
            <qdbapi>
            <ticket>%s</ticket>
            %s
            </qdbapi>
            """ % (self.app.ticket, recordInfo)
            self.request.data = data.encode('utf-8')
        elif self.action_string == "edit" or self.action_string == "csv":
            if type(data) == str:
                data = """
                <qdbapi>
                    <ticket>%s</ticket>
                    <records_csv>
                        <![CDATA[
                            %s
                        ]]>
                    </records_csv>
                    <clist>%s</clist>
                    <skipfirst>%s</skipfirst>
                </qdbapi>
                    """ % (self.app.ticket, data, clist, skip_first)
            elif type(data) == list:
                csv_lines = ""
                if type(data[0]) == list:
                    for line in data:
                        for item in line:
                            assert type(item) == str
                            csv_lines += item + ","
                        csv_lines = csv_lines[:-1] + "\n"
                elif type(data[0]) == str:
                    for item in data:
                        assert item == str
                        csv_lines += item + ","
                    csv_lines = csv_lines[:-1] + "\n"
                data = """
                <qdbapi>
                    <ticket>%s</ticket>
                    <records_csv>
                        <![CDATA[
                            %s
                        ]]>
                    </records_csv>
                    <clist>%s</clist>
                    <skipfirst>%s</skipfirst>
                </qdbapi>
                    """ % (self.app.ticket, csv_lines, clist, skip_first)
            elif type(data) == dict:
                csv_lines = ""
                for record_id in data:
                    assert type(record_id) == str and type(data[record_id]) == list
                    line = data[record_id]
                    csv_lines += record_id + ','
                    for item in line:
                        assert type(item) == str
                        csv_lines += item + ","
                    csv_lines = csv_lines[:-1] + "\n"
                data = """
                <qdbapi>
                    <ticket>%s</ticket>
                    <records_csv>
                        <![CDATA[
                            %s
                        ]]>
                    </records_csv>
                    <clist>%s</clist>
                    <skipfirst>%s</skipfirst>
                </qdbapi>
                    """ % (self.app.ticket, csv_lines, clist, "0")
            self.request.data = data.encode('utf-8')

    def performAction(self):
        """Performs the action defined by the QuickbaseAction object, and maps the response to an attribute

        :return: response
        """
        self.content = urllib.request.urlopen(self.request).readall()
        self.raw_response = etree.fromstring(self.content).findall('record')
        self.response = QuickbaseResponse(self.raw_response)
        self.fid_dict = dict()
        fid_list = self.clist.split('.')
        try:
            field_list = list(self.raw_response[0])
            counter = 0
            for fid in fid_list:
                self.fid_dict[fid] = field_list[counter].tag
                counter += 1
        except IndexError:
            self.fid_dict = None
        if not self.return_records:
            return self.content
        else:
            return self.raw_response


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
<ticket>%s</ticket>
<query>%s</query>
<clist>%s</clist>
</qdbapi>
""" % (ticket, request, clist)
    else:
        data = """
<qdbapi>
<ticket>%s</ticket>
<query>%s</query>
<clist>%s</clist>
<slist>%s</slist>
</qdbapi>
""" % (ticket, request, clist, slist)
    query.data = data.encode('utf-8')
    content = urllib.request.urlopen(query).readall()
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
<ticket>%s</ticket>
%s
</qdbapi>
""" % (ticket, recordInfo)

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
<ticket>%s</ticket>
<rid>%s</rid>
<field fid="%s">%s</field>
</qdbapi>
""" % (ticket, rid, field, value)
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
    request = urllib.request.Request(url+dbid)
    request.add_header("Content-Type", "application/xml")
    request.add_header("QUICKBASE-ACTION", "API_ImportFromCSV")
    if type(csvData) == str:
        data = """
        <qdbapi>
            <ticket>%s</ticket>
            <records_csv>
                <![CDATA[
                    %s
                ]]>
            </records_csv>
            <clist>%s</clist>
            <skipfirst>%s</skipfirst>
        </qdbapi>
            """ % (ticket, csvData, clist, skipFirst)
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
            <ticket>%s</ticket>
            <records_csv>
                <![CDATA[
                    %s
                ]]>
            </records_csv>
            <clist>%s</clist>
            <skipfirst>%s</skipfirst>
        </qdbapi>
            """ % (ticket, csv_lines, clist, skipFirst)
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
            <ticket>%s</ticket>
            <records_csv>
                <![CDATA[
                    %s
                ]]>
            </records_csv>
            <clist>%s</clist>
            <skipfirst>%s</skipfirst>
        </qdbapi>
            """ % (ticket, csv_lines, clist, "0")
    else:
        return None
    request.data = data.encode('utf-8')
    response = urllib.request.urlopen(request).readall()
    return response

def DownloadCSV(base_url, ticket, dbid, report_id, file_name="report.csv"):
    csv_file = file_name
    urllib.request.urlretrieve(base_url + dbid + "?a=q&qid=" + str(report_id) + "&dlta=xs%7E&ticket=" + ticket, csv_file)

def csvSort(input_file, output_file, sort_keys=[0], contains_labels=False):
    """Takes an input csv filename, sorts it, and writes to output_file
    :param input_file: The file to be read from
    :param output_file: The file to write to
    :param sort_keys: A list of position indices to sort (from highest to lowest sort level)
    :param contains_labels: Whether the first line is column labels
    :return:
    """
    with open(input_file, 'r', newline='', encoding='utf-8') as csv_input_file:
        r=csv.reader(csv_input_file)
        unsorted_lines = []
        # if contains_labels:
        #     file_labels = next(r)
        # else:
        #     file_labels = None
        first_line = True
        try:
            for line in r:
                if first_line and contains_labels:
                    file_labels = line
                    first_line = False
                else:
                    unsorted_lines.append(line)
        except UnicodeDecodeError:
            print(r.read())
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
        w=csv.writer(csv_output_file)
        if file_labels:
            w.writerow(file_labels)
        for line in sorted_lines:
            w.writerow(line)