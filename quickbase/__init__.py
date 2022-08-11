"""
This is the API used to connect all of the software authored by Mike Herman to Quickbase.
"""
__author__ = 'Herman'
# !/usr/bin/env python3
# -*- coding: UTF-8 -*-
import urllib.request, urllib.parse
import datetime, time
import xml.etree.ElementTree as etree
import csv
import smtplib
import json
import base64
import re
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

import os
import pytz
from urllib.parse import urlparse
from influxdb import InfluxDBClient

import quickbase
DEFAULT_TIMEOUT = 10  # default request timeout in seconds

class AuthenticationError(Exception):

    def __init__(self, message='Authentication String Invalid'):
        self.message = message
        super().__init__(self.message)


class QuickbaseError(Exception):
    def __init__(self, message='Quickbase has returned an error 75 when making a query for less than 100 records. Something is wrong.'):
        self.message = message
        super().__init__(self.message)


class QuickbaseQueryError(Exception):
    def __init__(self, message='Quickbase has returned an error 82, indicating it refuses to perform this query because it takes too long.'):
        self.message = message
        super().__init__(self.message)


class Analytics:
    """ Class for gathering Quickbase usage statistics """

    def __init__(self, events_db_url=None, verify_ssl=True):
        """
        Initializes class for collecting Quickbase usage statistics. It should gracefully abort on failure
        :param events_db_url: URL to Influx DB. Optional - it could also be passed by env variable. If no events_db_url
        is given, it fails gracefully
        """
        self.collecting = False

        try:
            if not events_db_url:
                events_db_url = os.environ.get('EVENTS_DB_URL')

            if events_db_url:
                parsed_url = urlparse(events_db_url)
                self.influxdb_host = parsed_url.hostname
                self.influxdb_port = parsed_url.port
                self.influxdb_user = parsed_url.username
                self.influxdb_pass = parsed_url.password
                self.influxdb_db = parsed_url.path[1:]
                self.influxdb_scheme = parsed_url.scheme

                if parsed_url.scheme == 'https':
                    self.influxdb_ssl = True
                else:
                    self.influxdb_ssl = False

                self.influxdb = InfluxDBClient(
                    self.influxdb_host,
                    self.influxdb_port,
                    self.influxdb_user,
                    self.influxdb_pass,
                    self.influxdb_db,
                    self.influxdb_ssl,
                    verify_ssl
                )

                self.collecting = True
        except Exception:
            pass  # it's done on purpose to gracefully disable analytics in case of any unexpected error

    def collect(self, measurement='quickbase_api_call', tags=None, ts=None, fields=None):
        """
        Collects a data point
        :param measurement: Name of measurement
        :param tags: A dict of tags
        :param ts: Datetime object. Optional
        :param fields: A dict of fields. Defaults to {'value': 1}
        """

        if self.collecting:
            try:
                point = {
                    'measurement': measurement,
                    'tags': {
                        'env': os.environ.get('GSET_ENV', ''),
                        'container': os.environ.get('GSET_CONTAINER', '')
                    },
                    'time': datetime.datetime.now(tz=pytz.UTC),
                    'fields': {
                        'value': 1
                    }
                }

                if tags:
                    point['tags'].update(tags)

                if ts:
                    point['time'] = ts

                if fields:
                    point['fields'].update(fields)

                self.influxdb.write_points([point], database=self.influxdb_db)
            except Exception as e:
                pass  # it's done on purpose to gracefully disable analytics in case of any unexpected error

class QuickbaseApp():
    def __init__(self, baseurl='https://cictr.quickbase.com/db/', ticket=None, tables=None, token=None, **kwargs):
        """Basic unit storing useful information for communicating with Quickbase

        :param baseurl: String, https://<domain>.quickbase.com/db/
        :param ticket: String, taken from quickbase cookie
        :param tables: Dict, map of dbid labels and dbid values
        :param token: For future use
        :return:
        """
        self.base_url = baseurl   # generally https://cictr.quickbase.com/db/, the base url for all CIC quickbase apps
        self.ticket = ticket    # authentication ticket
        self.token = token      # authentication token

        if tables == None:
            self.tables = generateTableDict('./CIC.cfg')
        else:
            self.tables = tables
        if ticket is None and token is None:
            if 'token' in self.tables:
                token = self.tables['token']
                self.authentication_string = "<usertoken>%s</usertoken>" % token
                self.authentication_type = 'usertoken'
                self.token = token
            elif 'ticket' in self.tables:
                ticket = self.tables['ticket']
                self.authentication_string = "<ticket>%s</ticket>" % ticket
                self.authentication_type = 'ticket'
                self.ticket = ticket
        elif token is not None:
            self.authentication_string = "<usertoken>%s</usertoken>" % token
            self.authentication_type = 'usertoken'
        else:
            assert ticket is not None
            self.authentication_string = "<ticket>%s</ticket>" % ticket
            self.ticket = ticket
            self.authentication_type = 'ticket'
        if kwargs:
            self.__dict__.update(kwargs)    # optional arguments



class QuickbaseAction():
    """
    QuickbaseAction objects contain the parameters for a request to quickbase, and after being executed (performAction)
    also contain any response from Quickbase
    """
    def __init__(self, app, dbid_key, action, query=None, clist=None, slist=None, return_records=None, data=None,
                 skip_first="0", time_in_utc=False, confirmation=False, options=None, force_utf8=False,
                 custom_body=None, record_return=None, error_75_retry=False, record_count=None):
        """

        :param app: class QuickbaseApp
        :param dbid_key: dbid label
        :param action: query, add, edit, qid or csv
        :param force_utf8: adds encoding tag in request
        :return:
        """

        if time_in_utc:
            self.send_time_in_utc = "1"
        else:
            self.send_time_in_utc = "0"
        self.app = app
        self.force_utf8 = force_utf8
        self.skip_first = skip_first
        self.dbid_key = dbid_key
        self.record_return = record_return
        self.record_count = record_count
        self.error_75_retry = error_75_retry
        self.options = options
        if dbid_key in self.app.tables: # build the request url
            self.request = urllib.request.Request(self.app.base_url + self.app.tables[dbid_key])
        else:   # assume any dbid_key not in app.tables is the actual dbid string
            self.request = urllib.request.Request(self.app.base_url + dbid_key)
        self.action_string = action.lower() # assign the correct Quickbase API command based on the action string
        if self.action_string in ["query", "qid", "qname", "querycount"]:
            self.action = "API_DoQuery"
            if self.action_string == 'querycount':
                self.action = "API_DoQueryCount"
                self.action_string = "query"
        elif self.action_string == "add":
            self.action = "API_AddRecord"
        elif self.action_string == "edit" or action.lower() == "csv":
            self.action = "API_ImportFromCSV"
        elif self.action_string == "purge":
            self.action = "API_PurgeRecords"
        elif self.action_string == "variable":
            self.action = "API_SetDBVar"
        # elif self.action_string == "querycount":
        #     self.action = "API_DoQueryCount"
        #     self.action_string = "query"
        else:
            self.action = action
        self.request.add_header("Content-Type", "application/xml")
        self.request.add_header("QUICKBASE-ACTION", self.action)
        self.return_records = return_records    # return the records from the response, or the response itself
        self.response = None
        self.slist = slist  # sort list
        self.data = data    # query/command data
        if type(clist) == list: # clist can be a list or a string
            clist_string = ""
            for fid in clist:
                clist_string += fid + "."
            self.clist = clist_string[:-1]
        else:
            self.clist = clist
        self.query = query
        if self.action_string == "query" or \
                self.action_string == "qid" or \
                self.action_string == 'qname' or \
                self.action_string == 'querycount':
            self.buildQuery()
        elif self.action_string == "purge": # purge removes all matching records and should be used with caution
            self.confirmation = confirmation
            self.buildPurge()
        elif self.action_string == "add":   # add a single record
            self.buildAdd()
        elif self.action_string == "edit" or self.action_string == "csv":   # it is easy enough to edit records using
                                                                            # the csv method.
            self.buildCSV()

        elif self.action_string == "variable":
            self.buildVariable()

        else:   # implies an action not otherwise handled
            self.data = """
            <qdbapi>
                %s""" % custom_body

        if self.options is not None:  # custom options
            self.data = self.data + """
            <options>%s</options>
            """ % self.options
        self.data = self.data + """
                    </qdbapi>
                        """
        self.request.data = self.data.encode('utf-8')


    def performAction(self, retry=False, **kwargs):
        """Performs the action defined by the QuickbaseAction object, and maps the response to an attribute

        :return: response
        """
        self.response_object = urllib.request.urlopen(self.request, timeout=DEFAULT_TIMEOUT) # do the thing

        Analytics().collect(tags={'action': self.action})
        self.status = self.response_object.status   # status response. Hopefully starts with a 2
        self.content = self.response_object.read().replace(b'<BR/>', b'')
        self.head_content = etree.fromstring(self.content.split(b'</errtext>')[0]+b'</errtext>\r\n</qdbapi>')
        self.errcode = self.head_content.find('errcode').text
        self.errtext = self.head_content.find('errtext').text

        if self.errcode == '4':
            if retry:
                raise AuthenticationError
            self.app.authentication_string = self.app.authentication_string.replace('ticket', 'usertoken')
            self.data = self.data.replace('ticket', 'usertoken')
            self.request.data = self.request.data.replace(b'ticket', b'usertoken')
            self.app.authentication_type = 'usertoken'
            self.performAction(retry=True)
        elif self.errcode == '83':
            if retry:
                raise AuthenticationError
            self.app.authentication_string = self.app.authentication_string.replace('usertoken', 'ticket')
            self.data = self.data.replace('usertoken', 'ticket')
            self.request.data = self.request.data.replace(b'usertoken', b'ticket')
            self.app.authentication_type = 'ticket'
            self.performAction(retry=True)
        elif self.errcode == '75':  # this will happen with very large queries
            self.error_75_retry = True
            self.response = recursive_query(self).response
        elif self.errcode == '82':
            raise QuickbaseQueryError
        else:
            if self.action == "API_DoQuery":
                self.etree_content = parseQueryContent(self.content)
            else:
                try:
                    self.etree_content = etree.fromstring(self.content)
                except etree.ParseError as err:
                    try:
                        parser = etree.XMLParser(encoding='cp1252')
                        self.etree_content = etree.fromstring(self.content, parser=parser)
                    except Exception as err2:
                        try:
                            self.etree_content = etree.fromstring(self.content.decode('cp1252'))
                        except Exception as err3:
                            print("triple exception caught")
                            raise Exception(str(err3))
            self.fid_dict = dict()
            if self.action == 'API_DoQueryCount':
                self.raw_response = self.etree_content.find('numMatches')
                self.response = QuickbaseResponse(self.raw_response)
                return self.raw_response.text
            elif self.action_string != 'csv':
                if len(self.etree_content) != 0 and \
                        type(self.etree_content) == list and \
                        type(self.etree_content[0]) == dict:
                    try:
                        self.raw_response = etree.fromstring(self.content).findall('record')
                    except:
                        self.raw_response = None
                    self.response = QuickbaseResponse([])
                    self.response.values = self.etree_content
                    self.response.records = self.content
                elif type(self.etree_content) != list:
                    self.raw_response = self.etree_content.findall('record')
                    self.response = QuickbaseResponse(self.raw_response)    # map the response to a QuickbaseResponse object
                else:
                    self.raw_response = list()
                    for content in self.etree_content:
                        self.raw_response.append(content.getchildren())
                    self.response = QuickbaseResponse(self.raw_response)
                if not self.action_string == "add" and not self.action_string == "purge":
                    if self.clist and len(self.response.values) != 0:
                        fid_list = self.clist.split('.')
                        try:
                            field_list = [x for x in self.response.values[0]]
                            counter = 0
                            for fid in fid_list:
                                self.fid_dict[fid] = field_list[counter]  # map field names to field id numbers
                                counter += 1
                        except IndexError:
                            self.fid_dict[fid] = None   #CAUTION: Quickbase will not tell you if you include an invalid field ID in a clist!
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
                        response_dict['rid'] = self.etree_content.find('rid').text  # record ids of new records
                    else:
                        response_dict['rid'] = None
                    if self.etree_content.find('errdetail') is not None:
                        response_dict['errdetail'] = self.etree_content.find('errdetail').text
                    else:
                        response_dict['errdetail'] = None
                    return response_dict
            if self.action_string == 'csv' or self.action_string == 'edit':
                resp = etree.fromstring(self.content)
                if resp.find('num_recs_input') is not None: # records received from the query
                    self.num_recs_input = resp.find('num_recs_input').text
                else:
                    self.num_recs_input = "0"
                if resp.find('num_recs_added') is not None: # records created
                    self.num_recs_added = resp.find('num_recs_added').text
                else:
                    self.num_recs_added = "0"
                if resp.find('num_recs_updated') is not None:   # records updated
                    self.num_recs_updated = resp.find('num_recs_updated').text
                else:
                    self.num_recs_updated = "0"
                self.rid_list = list()
                rids = etree.fromstring(self.content).find('rids')  # record id numbers
                try:
                    for rid in rids.findall('rid'):
                        self.rid_list.append(rid.text)
                    return self.rid_list
                except AttributeError as err:
                    print(err)
                    print(self.content)

    def buildQuery(self):
        encoding = '<encoding>utf-8</encoding>' if self.force_utf8 else ''
        if self.query:  # build the query request
            if "query=" in self.query or "qid=" in self.query or "qname=" in self.query:
                v, self.query = self.query.split("=", 1)
            if self.slist == "0":   # sort the responses on the listed field IDs
                self.data = """
                                <qdbapi>
                                    %s
                                    %s
                                    <%s>%s</%s>
                                    """ % (
                encoding, self.app.authentication_string, self.action_string, self.query, self.action_string)
                if clist:
                    self.data = self.data + """<clist>%s</clist>
                                """ % (self.clist)

            else:   # responses sorted by the default field
                self.data = """
                                <qdbapi>
                                    %s
                                    %s
                                    <%s>%s</%s>
                                    """ % (
                encoding, self.app.authentication_string, self.action_string, self.query, self.action_string)
                if self.clist:
                    self.data = self.data + """<clist>%s</clist>
                                """ % (self.clist)
                self.data = self.data + """<slist>%s</slist>
                                """ % (self.slist)

        else:  # queries with an empty query string are allowed and should return all records from the table
            if self.slist == "0":
                self.data = """
                                <qdbapi>
                                    %s
                                    %s
                                    """ % (encoding, self.app.authentication_string)
                if self.clist:
                    self.data = self.data + """<clist>%s</clist>
                                """ % (self.clist)
            else:
                self.data = """
                                <qdbapi>
                                    %s
                                    %s
                                    """ % (encoding, self.app.authentication_string)
                if self.clist:
                    self.data = self.data + """<clist>%s</clist>
                                """ % (self.clist)
                self.data = self.data + """<slist>%s</slist>
                                """ % (self.slist)

    def buildAdd(self):
        assert type(self.data) == dict
        recordInfo = ""
        for field in self.data:
            recordInfo += '<field fid="' + str(field) + '">' + str(self.data[field]) + "</field>\n"
        self.data = """
                        <qdbapi>
                            <msInUTC>%s</msInUTC>
                            %s
                            %s

                        """ % (self.send_time_in_utc, self.app.authentication_string, recordInfo)

    def buildPurge(self):
        assert self.confirmation
        assert self.query # use qid=1 instead
        if self.confirmation and self.query:
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
                               %s
                               <%s>%s</%s>
                           """ % (self.app.authentication_string, query_type, self.query, query_type)
        return None

    def buildVariable(self):
        assert type(self.data) == dict
        assert len(self.data) == 1
        for variable in self.data:
            variable_name = variable
            variable_value = self.data[variable]
            self.data = """
                            <qdbapi>
                                <msInUTC>%s</msInUTC>
                                %s
                                <varname>%s</varname>
                                <value>%s</value>
                            """ % (self.send_time_in_utc, self.app.authentication_string, variable_name, variable_value)

    def buildCSV(self):
        if type(self.data) == str:  # data can be type string, list or dict
            if '"' in self.data:
                self.data = self.data.replace('"', '""')  # Quickbase requires double quotes for quotes within
                self.data = '"' + self.data + '"'  # data
            elif "," in self.data:  # commas are special characters so strings containing them need to be quoted
                self.data = '"' + self.data + '"'
            if '\n' in self.data and not (self.data[0] == '"' and self.data[-1] == '"'):  # \n is also a special
                self.data = '"' + self.data + '"'  # character
            self.data = """
            <qdbapi>
                <msInUTC>%s</msInUTC>
                %s
                <records_csv>
                    <![CDATA[
                        %s
                    ]]>
                </records_csv>
                <clist>%s</clist>
                <skipfirst>%s</skipfirst>

                """ % (self.send_time_in_utc, self.app.authentication_string, self.data, self.clist, self.skip_first)
        elif type(self.data) == list:
            csv_lines = ""
            if type(self.data[0]) == list:  # a list of lists works as well
                for line in self.data:
                    for item in line:
                        if item is None:  # quickbase chokes unless None is converted to an empty string
                            item = ''
                        assert type(item) == str
                        if '"' in item:
                            item = item.replace('"', '""')
                            item = '"' + item + '"'
                        elif "," in item:
                            item = '"' + item + '"'
                        if '\n' in item and not (item[0] == '"' and item[-1] == '"'):
                            item = '"' + item + '"'
                        csv_lines += item + ","
                    csv_lines = csv_lines[:-1] + "\n"
            elif type(self.data[0]) == str:
                for item in self.data:
                    if item is None:
                        item = ''
                    try:
                        assert type(item) == str
                    except AssertionError:
                        print("wrong item type")
                        print(type(item))
                        print(item)
                        if item is None:  # quickbase chokes unless None is converted to an empty string
                            item = ""
                    if '"' in item:
                        item = item.replace('"', '""')
                        item = '"' + item + '"'
                    elif "," in item:
                        item = '"' + item + '"'
                    if '\n' in item and not (item[0] == '"' and item[-1] == '"'):
                        item = '"' + item + '"'
                    csv_lines += item + ","
                csv_lines = csv_lines[:-1] + "\n"
            self.data = """
            <qdbapi>
                <msInUTC>%s</msInUTC>
                %s
                <records_csv>
                    <![CDATA[
                        %s
                    ]]>
                </records_csv>
                <clist>%s</clist>
                <skipfirst>%s</skipfirst>

                """ % (self.send_time_in_utc, self.app.authentication_string, csv_lines, self.clist, self.skip_first)
        elif type(self.data) == dict:  # dicts are preferred for editing existing records. Dict key is record ID
            csv_lines = ""
            assert '3' in self.clist.split('.')
            for record_id in self.data:
                assert type(record_id) == str and type(self.data[record_id]) == list
                line = self.data[record_id]
                csv_lines += record_id + ','
                for item in line:
                    if item is None:  # quickbase chokes unless None is converted to an empty string
                        item = ''
                    assert type(item) == str
                    if '"' in item:
                        item = item.replace('"', '""')
                        item = '"' + item + '"'
                    elif "," in item:
                        item = '"' + item + '"'
                    if '\n' in item and not (item[0] == '"' and item[-1] == '"'):
                        item = '"' + item + '"'
                    csv_lines += item + ","
                csv_lines = csv_lines[:-1] + "\n"
            self.data = """
            <qdbapi>
                <msInUTC>%s</msInUTC>
                %s
                <records_csv>
                    <![CDATA[
                        %s
                    ]]>
                </records_csv>
                <clist>%s</clist>
                <skipfirst>%s</skipfirst>

                """ % (self.send_time_in_utc, self.app.authentication_string, csv_lines, self.clist, "0")

class QuickbaseResponse():
    """
    object which maps details from the response to a dict for easy retrieval
    """
    def __init__(self, response):

        self.records = response
        self.values = []    # each record is a dict key=field name, value=field value
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

def generate_quickbase_app(config='CIC.cfg', baseUrl="https://cictr.quickbase.com/db/", auth_key=None, **kwargs):
    """
    Generates a quickbase.QuickbaseApp to be used in all queries against the quickbase database
    :return CIC: quickbase.QuickbaseApp with all necessary parameters to perform queries and actions
    """
    cic_tables = generateTableDict(config)
    # baseUrl = "https://cictr.quickbase.com/db/"
    if auth_key is not None:
        CIC = QuickbaseApp(baseUrl, ticket=auth_key, tables=cic_tables, **kwargs)
    elif 'ticket' in cic_tables:
        ticket = cic_tables['ticket']
        CIC = QuickbaseApp(baseUrl, ticket=ticket, tables=cic_tables, **kwargs)
    else:
        assert 'token' in cic_tables
        token = cic_tables['token']
        CIC = QuickbaseApp(baseUrl, token=token, tables=cic_tables, **kwargs)
    return CIC


def parseQueryContent(content):
    records = content.split(b'</record>')
    full_content = list()
    for record in records:
        if not b'<record>' in record:
            continue
        record += b'</record>'
        if b'</chdbids>' in record:
            record = record.split(b'</chdbids>')[1]
        try:
            etree_content = {x.tag: x.text for x in etree.fromstring(record)}
        except Exception as err:
            etree_content = {x.tag: x.text for x in etree.fromstring(record.decode('cp1252'))}
        full_content.append(etree_content)
    return full_content


def parseSchemaContent(content, include_field_details=False):
    field_section = content.split(b'<fields>')[1]
    fields = field_section.split(b'</field>')
    full_content = dict()
    for field in fields:
        if not b'<field' in field:
            continue
        field += b'</field>'
        try:
            field_string = etree.fromstring(field)
        except Exception as err:
            field_string = etree.fromstring(field.decode('cp1252'))
        field_id = field_string.attrib['id']
        field_details = {x.tag: x.text for x in field_string}
        if include_field_details:
            field_details['field_type'] = field_string.attrib['field_type']
            field_details['id'] = field_id
            if 'mode' in field_string.attrib:
                field_details['field_mode'] = field_string.attrib['mode']
            full_content[field_details['label']] = field_details
        else:
            full_content[field_details['label']] = field_id
    return full_content


def getTableFIDDict(app_object, dbid, return_alphanumeric=False, return_standard=True, return_field_details=False, return_reverse=False):
    """
    Uses API_GetSchema to generate a dict of FIDs by field name. Note that the responses here include a lot of extra
    information and generate a large (up to 1MB or more for some tables) response. This module should only be run as
    necessary. Also note that the field names returned by API_GetSchema are as defined in the Quickbase app, and thus
    include non-alphanumeric characters and original capitalization. To use this in conjunction with an API query
    response, it will be necessary to use a regular expression to convert all non-alphanumeric characters to underscores
    and also convert all field names to lower case.
    :param app_object:
    :param dbid:
    :return:
    """
    if dbid in app_object.tables:
        table = app_object.tables[dbid]
    else:
        table = dbid
    request = urllib.request.Request(app_object.base_url + table)
    request.add_header("Content-type", "application/xml")
    request.add_header("QUICKBASE-ACTION", "API_GetSchema")
    data = """
    <qdbapi>
        %s
    </qdbapi>""" % (app_object.authentication_string)
    request.data = data.encode('utf-8')
    response = urllib.request.urlopen(request, timeout=DEFAULT_TIMEOUT)
    status = response.status
    field_dict = dict()
    alphanumeric_regex = re.compile('\W')
    if status == 200:
        response_content = response.read().replace(b'<BR/>', b'')
        full_content = parseSchemaContent(response_content, return_field_details)
        if return_standard:
            for field_label in full_content:
                field_dict[field_label] = full_content[field_label]
        if return_alphanumeric:
            for field_label in full_content:
                alphanumeric_key = alphanumeric_regex.sub("_", field_label).lower()
                field_dict[alphanumeric_key] = full_content[field_label]
        if return_reverse:
            temp_dict = field_dict.copy()
            for field_label in temp_dict:
                field_dict[field_dict[field_label]] = alphanumeric_regex.sub("_", field_label).lower()
    return field_dict


def generateTableDict(import_filename):
    """
    Takes a csv generated from the Quickbase App Management/Show Support Information page and returns a dict of table
    names and dbids
    :param import_filename: name of the file to parse
    :return table_dict: the table dict
    """
    table_dict = dict()
    with open(import_filename, 'r') as csv_file:
        r = csv.reader(csv_file)
        for row in r:
            if row[0] not in table_dict:
                table_dict[row[0]] = row[2]
                table_dict[row[0].lower()] = row[2]
            table_dict[row[1]] = row[2]
            table_dict[row[1].lower()] = row[2]
    return table_dict


def QBQuery(url, ticket, dbid, request, clist, slist="0", returnRecords=False):
    """
    DEPRECATED
    This function takes the base Quickbase URL, an authentication ticket, a database ID (DBID), a query and a clist, and
    returns an XML file.
    URL format: string, 'https://<basedomain>.quickbase.com/db/
    Authentication ticket: You can get this from your Quickbase cookie in your browser. This should eventually be
    changed to use a ticket and token
    DBID: the ID of the table you want to reference (what comes after the /db/ and before the ?act=)
    Query:query={CONDITIONS}. Should not contain any HTML encoding
    Clist: a period-separated list of fields you want returned
    """
    action = 'API_DoQuery'

    query = urllib.request.Request(url + dbid)
    query.add_header("Content-Type", "application/xml")
    query.add_header("QUICKBASE-ACTION", action)
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
    content = urllib.request.urlopen(query, timeout=DEFAULT_TIMEOUT).read()

    Analytics().collect(tags={'action': action})

    if not returnRecords:
        return content
    else:
        return etree.fromstring(content).findall('record')


def QBAdd(url, ticket, dbid, fieldValuePairs):
    """
    DEPRECATED
    This function adds a record in Quickbase. fieldValuePairs should be a dictionary of fid and values, and must include
    all required fields (especially related client).
    fieldValuePairs must use fid values as key, not field names
    """
    action = 'API_AddRecord'

    query = urllib.request.Request(url + dbid)
    query.add_header("Content-Type", "application/xml")
    query.add_header("QUICKBASE-ACTION", action)
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
    response = urllib.request.urlopen(query, timeout=DEFAULT_TIMEOUT)

    Analytics().collect(tags={'action': action})

    return response


def EpochToDate(epochTime, include_time=False, convert_to_eastern_time=False, include_timezone=True):
    """
    Takes a Quickbase-generated time value (ms since the start of the epoch) and returns a datetime.date object
    If pulling directly from Quickbase, should be converted to eastern time
    """
    if include_timezone:
        if epochTime and not include_time:
            tupleTime = time.gmtime(int(epochTime) / 1000)
            realDate = datetime.date(tupleTime.tm_year, tupleTime.tm_mon, tupleTime.tm_mday)
            return realDate
        elif epochTime and include_time:
            tupleTime = time.gmtime(int(epochTime) / 1000)
            realDateTime = datetime.datetime(tupleTime.tm_year, tupleTime.tm_mon, tupleTime.tm_mday, tupleTime.tm_hour,
                                             tupleTime.tm_min, tupleTime.tm_sec, tzinfo=pytz.UTC)
            if convert_to_eastern_time:
                realDateTime = realDateTime.astimezone(tz=pytz.timezone('US/Eastern'))
            # realDateTime = realDateTime.astimezone(tz=Eastern_tzinfo())
            return realDateTime
        else:
            return None
    else:
        if epochTime and not include_time:
            tupleTime = time.gmtime(int(epochTime) / 1000)
            realDate = datetime.date(tupleTime.tm_year, tupleTime.tm_mon, tupleTime.tm_mday)
            return realDate
        elif epochTime and include_time:
            tupleTime = time.gmtime(int(epochTime) / 1000)
            realDateTime = datetime.datetime(tupleTime.tm_year, tupleTime.tm_mon, tupleTime.tm_mday, tupleTime.tm_hour,
                                             tupleTime.tm_min, tupleTime.tm_sec)
            # if convert_to_eastern_time:
            #     realDateTime = realDateTime.astimezone(tz=pytz.timezone('US/Eastern'))
            # realDateTime = realDateTime.astimezone(tz=Eastern_tzinfo())
            return realDateTime
        else:
            return None


def DateToEpoch(regDate, include_time=False, convert_to_eastern_time=False, include_timezone=True):
    """
    takes a datetime object and returns an epoch time integer in a format that
    quickbase can use. Assumes time in localtime and does necessary alterations to make it work with UTC if
    convert_to_eastern_time is true
    """
    if include_timezone:
        if not include_time:
            date_object = datetime.datetime(regDate.year, regDate.month, regDate.day, tzinfo=pytz.UTC)
            if convert_to_eastern_time:
                utc_date_object = date_object.astimezone(tz=pytz.timezone('US/Eastern'))
                # structTime = time.strptime(str(date_object.year) + str(date_object.month) + str(date_object.day) + " " +
                #                            str(date_object.tzinfo),
                #                            "%Y%m%d %Z")
                epochTime = int(time.mktime(utc_date_object.timetuple()) * 1000)
            else:
                epochTime = int(time.mktime(date_object.timetuple()) * 1000)
        else:
            datetime_object = datetime.datetime(regDate.year, regDate.month, regDate.day, regDate.hour, regDate.minute,
                                                regDate.second, tzinfo=pytz.UTC)
            if convert_to_eastern_time:
                utc_datetime_object = datetime_object.astimezone(tz=pytz.timezone('US/Eastern'))
                # structTime = time.strptime(str(date_object.year) + str(date_object.month) + str(date_object.day) + " "
                #                            + str(date_object.hour) + ":" + str(date_object.minute) + ":"
                #                            + str(date_object.second) + " " + str(date_object.tzinfo),
                #                            "%Y%m%d %H:%M:%S %Z")
                epochTime = int(time.mktime(utc_datetime_object.timetuple()) * 1000)
            else:
                epochTime = int(time.mktime(datetime_object.timetuple()) * 1000)
    else:
        if not include_time:
            date_object = datetime.datetime(regDate.year, regDate.month, regDate.day, tzinfo=pytz.UTC)
            if convert_to_eastern_time:
                utc_date_object = date_object.astimezone(tz=pytz.timezone('US/Eastern'))
                # structTime = time.strptime(str(date_object.year) + str(date_object.month) + str(date_object.day) + " " +
                #                            str(date_object.tzinfo),
                #                            "%Y%m%d %Z")
                epochTime = int(time.mktime(utc_date_object.timetuple()) * 1000)
            else:
                epochTime = int(time.mktime(date_object.timetuple()) * 1000)
        else:
            datetime_object = datetime.datetime(regDate.year, regDate.month, regDate.day, regDate.hour, regDate.minute,
                                                regDate.second)
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
    """
    DEPRECATED
    :param url:
    :param ticket:
    :param dbid:
    :param rid:
    :param field:
    :param value:
    :return:
    """
    action = 'API_EditRecord'

    query = urllib.request.Request(url + dbid)
    query.add_header("Content-Type", "application/xml")
    query.add_header("QUICKBASE-ACTION", action)
    data = """
        <qdbapi>
            <msInUTC>%s</msInUTC>
            <ticket>%s</ticket>
            <rid>%s</rid>
            <field fid="%s">%s</field>
        </qdbapi>
    """ % ('0', ticket, rid, field, value)
    query.data = data.encode('utf-8')
    response = urllib.request.urlopen(query, timeout=DEFAULT_TIMEOUT)

    Analytics().collect(tags={'action': action})

    return response


def UploadCsv(url, ticket, dbid, csvData, clist, skipFirst=0):
    """
    DEPRECATED
    Given a csv-formatted string, list, or dict, upload records to Quickbase

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
    action = 'API_ImportFromCSV'

    request = urllib.request.Request(url + dbid)
    request.add_header("Content-Type", "application/xml")
    request.add_header("QUICKBASE-ACTION", action)
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
    response = urllib.request.urlopen(request, timeout=DEFAULT_TIMEOUT).read()

    Analytics().collect(tags={'action': action})

    return response


def DownloadCSV(base_url, ticket, dbid, report_id, file_name="report.csv"):
    """
    DEPRECATED
    :param base_url:
    :param ticket:
    :param dbid:
    :param report_id:
    :param file_name:
    :return:
    """
    csv_file = file_name
    urllib.request.urlretrieve(base_url + dbid + "?a=q&qid=" + str(report_id) + "&dlta=xs%7E&ticket=" + ticket,
                               csv_file)

    Analytics().collect(tags={'action': 'download_csv'})


def csvSort(input_file,
            output_file,
            sort_keys=[0],
            contains_labels=False,
            format='utf-8',
            quotechar=None,
            delimiter=None):
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
                sorted_lines = sorted(sorted_lines, key=lambda item: item[sort_key].lower())
            except IndexError:
                print(sorted_lines[0])
                print(sort_key)
                print(len(sorted_lines[0]))
    with open(output_file, 'w', newline='', encoding='utf-8') as csv_output_file:
        w = csv.writer(csv_output_file, quotechar=quotechar, delimiter=delimiter)
        if file_labels:
            w.writerow(file_labels)
        for line in sorted_lines:
            w.writerow(line)

def downloadFile(dbid, ticket, rid, fid, filename, vid='0', baseurl='https://cictr.quickbase.com/'):
    """
    DEPRECATED
    :param dbid:
    :param ticket:
    :param rid:
    :param fid:
    :param filename:
    :param vid:
    :param baseurl:
    :return:
    """

    request = urllib.request.Request(
        baseurl + 'up/' + dbid + '/a/r' + rid + '/e' + fid + '/v' + vid + '?ticket=' + ticket)
    response = urllib.request.urlopen(request, timeout=DEFAULT_TIMEOUT).read()
    with open(filename, 'wb') as downloaded_file:
        downloaded_file.write(response)

    Analytics().collect(tags={'action': 'download_file'})

def email(sub, destination=None, con=None, file_path=None, file_name=None, fromaddr=None, smtp_cfg=None, user="noreply@cictr.com"):
    """

    :rtype : object
    """
    COMMASPACE = ', '
    if fromaddr is None:
        fromaddr = "noreply@cictr.com"
    if not destination:
        toaddr = ["herman@cictr.com"]
    else:
        toaddr = destination
    if smtp_cfg is None:
        smtp_cfg = "smtp.cfg"
    subject = sub
    content = ""
    if con:
        for line in con:
            content += (str(line) + '\r\n')

    if file_path:
        attachment = MIMEBase('appplication', 'octet-stream')
        with open(file_path, 'rb') as attached_file:
            attachment.set_payload(attached_file.read())
        encoders.encode_base64(attachment)
        attachment.add_header('Content-disposition', 'attachment; filename='+file_name)

    authenticator = dict()
    with open(smtp_cfg, 'r') as config_file:
        r = csv.reader(config_file)
        for row in r:
            authenticator[row[0]] = row[1]

    passwd = authenticator[user]

    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = fromaddr
    msg['To'] = COMMASPACE.join(toaddr)
    msg.attach(MIMEText(content))
    if file_path:
        msg.attach(attachment)
    smtp = smtplib.SMTP("costner.cictr.com", port=587)
    # smtp.set_debuglevel(1)
    smtp.ehlo()
    smtp.starttls()
    smtp.ehlo()
    smtp.login(user, passwd)
    # smtp.docmd('AUTH', 'XOAUTH2 ' + authString.decode('utf-8'))
    smtp.send_message(msg)
    smtp.quit()
    # syslog.syslog("Email sent to " + str(toaddr))


def recursive_query(query_object):
    if query_object.record_return is None:  # We start with a best guess of 5000 as an allowed number of records returned
        query_object.record_count = QuickbaseAction(query_object.app, query_object.dbid_key, 'querycount',
                                                    query=query_object.query).performAction()
        if int(query_object.record_count) >= 8192:
            query_object.record_return = 8192
        else:
            query_object.record_return = int(int(query_object.record_count) / 2)
    elif query_object.record_return < 100 and query_object.error_75_retry:
        raise QuickbaseError
    elif query_object.error_75_retry:  # If that does not work, we halve the number of records returned
        query_object.record_return = int(query_object.record_return / 2)
        query_object.error_75_retry = False
        query_object = recursive_query(query_object)
    options = str()
    if query_object.options is not None:
        existing_options = query_object.options.split('.')
        for option in existing_options:
            if 'num-' not in option and 'skp-' not in option:
                options += '.' + option
    options += '.num-' + str(query_object.record_return)
    if query_object.response is not None:
        if len(query_object.response.values) >= int(query_object.record_count):
            return query_object
        options += '.skp-' + str(len(query_object.response.values))
    if options[0] == '.':
        options = options[1:]
    fractional_query = QuickbaseAction(query_object.app,
                                       query_object.dbid_key,
                                       'query',
                                       query=query_object.query,
                                       clist=query_object.clist,
                                       options=options,
                                       slist=query_object.slist,
                                       record_return=query_object.record_return,
                                       record_count=query_object.record_count)
    fractional_query.performAction()
    if query_object.response is None:
        query_object.response = fractional_query.response
    else:
        query_object.response.values.extend(fractional_query.response.values)
    query_object = recursive_query(query_object)
    return query_object
