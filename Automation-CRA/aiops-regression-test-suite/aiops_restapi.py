import requests
import re
import math
import json
import time
import duckdb
import logging as log
import ibm_boto3
import base64
import tempfile
from retrying import retry
from bs4 import BeautifulSoup as bs
from ibm_botocore.client import Config
from elasticsearch import Elasticsearch
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta, timezone

headers = {"Content-Type":"application/json"}

hostname_patterns = [r"host_hostname\s*=\s*'([^']+)'",
    r"hostname[:\s=]+([a-zA-Z0-9][a-zA-Z0-9\-\.]*[a-zA-Z0-9])",
    r"([a-z]+\d+-[a-z]+\d+-[a-z]+\d+-[a-z]+\d+-[a-z]+\d+)" ]

uuid_patterns = [r'r[0-9a-z]{3}[_-][0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',
                 r'0[0-9a-z]{3}[_-][0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',
                 r'"CORRID":"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})']

region_list = ['us-south', 'us-east', 'eu-gb', 'eu-de', 'eu-fr2',
               'jp-tok', 'au-syd', 'jp-osa', 'ca-tor', 'br-sao', 'eu-es', 'ca-mon']

cr_impact_list = ['critical', 'high', 'moderate', 'low', 'no_impact']
cr_state_list = ['New', 'Scheduled', 'Implement', 'Closed']
cr_search_list = ['planned_start', 'planned_end', 'actual_start', 'actual_end', 'created', 'updated']

status_internal_server = { "error": {"status_code": 500,
                                      "reason": "Internal Server Error",
                                      "details": "Something went wrong on the server"}}

completed_location_details = [
    {"datacenter": "dal10", "mzone": "mzone717", "zone": "us-south-1", "region": "us-south"},
    {"datacenter": "dal12", "mzone": "mzone727", "zone": "us-south-2", "region": "us-south"},
    {"datacenter": "dal13", "mzone": "mzone737", "zone": "us-south-3", "region": "us-south"},
    {"datacenter": "dal14", "mzone": "mzone747", "zone": "us-south-4", "region": "us-south"},
    {"datacenter": "wdc04", "mzone": "mzone757", "zone": "us-east-1", "region": "us-east"},
    {"datacenter": "wdc06", "mzone": "mzone767", "zone": "us-east-2", "region": "us-east"},
    {"datacenter": "wdc07", "mzone": "mzone777", "zone": "us-east-3", "region": "us-east"},
    {"datacenter": "lon04", "mzone": "mzone787", "zone": "eu-gb-1", "region": "eu-gb"},
    {"datacenter": "lon05", "mzone": "mzone797", "zone": "eu-gb-2", "region": "eu-gb"},
    {"datacenter": "lon06", "mzone": "mzone7a7", "zone": "eu-gb-3", "region": "eu-gb"},
    {"datacenter": "fra02", "mzone": "mzone2b7", "zone": "eu-de-1", "region": "eu-de"},
    {"datacenter": "fra04", "mzone": "mzone2c7", "zone": "eu-de-2", "region": "eu-de"},
    {"datacenter": "fra05", "mzone": "mzone2d7", "zone": "eu-de-3", "region": "eu-de"},
    {"datacenter": "par04", "mzone": "mzone2k7", "zone": "eu-fr2-1", "region": "eu-fr2"},
    {"datacenter": "par05", "mzone": "mzone2l7", "zone": "eu-fr2-2", "region": "eu-fr2"},
    {"datacenter": "par06", "mzone": "mzone2m7", "zone": "eu-fr2-3", "region": "eu-fr2"},
    {"datacenter": "tok02", "mzone": "mzone2e7", "zone": "jp-tok-1", "region": "jp-tok"},
    {"datacenter": "tok04", "mzone": "mzone2f7", "zone": "jp-tok-2", "region": "jp-tok"},
    {"datacenter": "tok05", "mzone": "mzone2g7", "zone": "jp-tok-3", "region": "jp-tok"},
    {"datacenter": "syd01", "mzone": "mzone2h7", "zone": "au-syd-1", "region": "au-syd"},
    {"datacenter": "syd04", "mzone": "mzone2i7", "zone": "au-syd-2", "region": "au-syd"},
    {"datacenter": "syd05", "mzone": "mzone2j7", "zone": "au-syd-3", "region": "au-syd"},
    {"datacenter": "osa21", "mzone": "mzone2n7", "zone": "jp-osa-1", "region": "jp-osa"},
    {"datacenter": "osa22", "mzone": "mzone2o7", "zone": "jp-osa-2", "region": "jp-osa"},
    {"datacenter": "osa23", "mzone": "mzone2p7", "zone": "jp-osa-3", "region": "jp-osa"},
    {"datacenter": "tor01", "mzone": "mzone2q7", "zone": "ca-tor-1", "region": "ca-tor"},
    {"datacenter": "tor04", "mzone": "mzone2r7", "zone": "ca-tor-2", "region": "ca-tor"},
    {"datacenter": "tor05", "mzone": "mzone2s7", "zone": "ca-tor-3", "region": "ca-tor"},
    {"datacenter": "sao01", "mzone": "mzone2t7", "zone": "br-sao-1", "region": "br-sao"},
    {"datacenter": "sao04", "mzone": "mzone2u7", "zone": "br-sao-2", "region": "br-sao"},
    {"datacenter": "sao05", "mzone": "mzone2v7", "zone": "br-sao-3", "region": "br-sao"},
    {"datacenter": "mad02", "mzone": "mzone2w7", "zone": "eu-es-1", "region": "eu-es"},
    {"datacenter": "mad04", "mzone": "mzone2x7", "zone": "eu-es-2", "region": "eu-es"},
    {"datacenter": "mad05", "mzone": "mzone2y7", "zone": "eu-es-3", "region": "eu-es"},
    {"datacenter": "mon04", "mzone": "mzone5a7", "zone": "ca-mon-1", "region": "ca-mon"},
    {"datacenter": "mon04", "mzone": "mzone5b7", "zone": "ca-mon-2", "region": "ca-mon"},
    {"datacenter": "mon04", "mzone": "mzone5c7", "zone": "ca-mon-3", "region": "ca-mon"}
]

class RestApi:
    def __init__(self, test_config = None, url_suffix = "", payload = None, username=None, password=None, cert_path=None):
        self.url = f'{test_config}{url_suffix}'
        self.payload = payload
        self.username = username
        self.password = password
        self.cert_path = cert_path
        if username and password:
            self.auth = HTTPBasicAuth(username, password)
        else:
            self.auth = None

    @retry(stop_max_attempt_number=5, wait_fixed=1000)
    def post(self, assert_action=False):
        log.info(f'Sending POST request to {self.url} with payload: {self.payload}')
        if 'github-feedback' in self.url:
            resp = requests.post(self.url, data=self.payload)
        else:
            resp = requests.post(self.url, headers=headers, json=self.payload,
                                 auth=self.auth, verify=self.cert_path, timeout=600)
        if resp.status_code == 429:
            log.error(f'HTTPError: 429 Client Error: '
                      f'Too Many Requests for url: {self.url}. Retrying...!')
            time.sleep(10)
            assert False
        if assert_action:
            resp.raise_for_status()
        log.info(f'POST request status:{resp.status_code}, reason:{resp.reason}')
        if resp.status_code == 500 and resp.json() == []:
            return status_internal_server
        return resp.json()
    def post_db_status(self, api_key=None):
        log.info(f'Sending POST request to {self.url} with payload: {self.payload}')
        header = {"Content-Type": "application/json",
                   "x-api-key": api_key}
        resp = requests.post(self.url, headers=header, json=self.payload,
                             auth=self.auth, verify=self.cert_path, timeout=600)
        log.info(f'POST request Status code: {resp.status_code}, reason:{resp.reason}')
        return resp.json()
    def get(self, api_type=None, api_key=None, params=None, assert_action=False):
        log.info(f'Sending GET request to {self.url} with params: {params}')
        if api_type == 'snow':
            self.auth = HTTPBasicAuth("apikey", api_key)
        resp = requests.get(self.url, headers=headers, auth=self.auth,
                            verify=self.cert_path, params=params, timeout=600)
        if assert_action:
            resp.raise_for_status()
        log.info(f'GET request Status code: {resp.status_code}, reason:{resp.reason}')
        return parse_get_response(resp)

def parse_get_response(resp):
    content_type = resp.headers.get('Content-Type')
    if content_type and 'application/json' in content_type:
        try:
            return resp.json()
        except json.JSONDecodeError as e:
            log.error(f"Failed to parse JSON: {e}")
            return resp.text
    elif content_type and 'text/html' in content_type:
        data = bs(resp.text, 'html.parser')
        return data
    else:
        return resp.text

def get_incident_details_hostuuid(resp, find):
    """get the host or uuid of the incident"""
    if find == 'uuid':
        patterns = uuid_patterns
    else:
        patterns = hostname_patterns
    host_uuid = []
    for pattern in patterns:
        matches = re.findall(pattern, str(resp))
        host_uuid.extend(matches)
    return sorted(set(host_uuid))

def get_api_details(url, route, data=None):
    """get the api details and return the response"""
    log.info('Checking api details from UI')
    aiops_api = RestApi(url, route)
    return aiops_api.get(params=data)

def get_incident_details(url, route, incident, find=None):
    """get the incident details and return the response & host/uuid"""
    log.info(f'Checking incident details from UI #{incident}')
    aiops_api = RestApi(url, route + incident)
    resp = aiops_api.get()
    host = get_incident_details_hostuuid(resp, find)
    log.info(f'hosts/uuid:{host}, found for the incident: {incident} from UI')
    return resp, host

def get_fewer_incident_info(url, route, payload):
    """get the post response of api"""
    aiops_api = RestApi(url, route, payload)
    return aiops_api.post()

def get_timerange(timestamp, range=None):
    """calculate the start and end time for the given timestamp"""
    input_time = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')
    start_time = input_time.strftime('%Y-%m-%dT%H:%M')
    output_time = input_time + timedelta(minutes=30)
    end_time = output_time.strftime('%Y-%m-%dT%H:%M')
    log.info(f'check acct info for timerange between {start_time} and {end_time}')
    if range == 'wrong':
        return f'>={end_time}<={start_time}'
    return f'>={start_time}<={end_time}'

def get_custom_timerange(timestamp=None, interval=None):
    """calculate the start and end time for the given timestamp"""
    if timestamp:
        input_time = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')
    else:
        input_time = datetime.now(timezone.utc)
    if interval == '15mins':
        start_time = (input_time - timedelta(minutes=15)).strftime('%Y-%m-%dT%H:%M:%SZ')
        end_time = (input_time + timedelta(minutes=15)).strftime('%Y-%m-%dT%H:%M:%SZ')
    elif interval == '24hours':
        start_time = (input_time - timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%SZ')
        end_time = input_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    elif interval == '48hours':
        start_time = (input_time - timedelta(hours=48)).strftime('%Y-%m-%dT%H:%M:%SZ')
        end_time = input_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    else:
        start_time = input_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_time = (input_time + timedelta(minutes=30)).strftime('%Y-%m-%dT%H:%M:%SZ')
    log.info(f'check for timerange between {start_time} and {end_time}')
    return start_time, end_time


def get_host_details(url, host, payload, assert_action=False):
    """get the host details """
    log.info(f'###### checking host details for {host} ######')
    aiops_api = RestApi(url, 'host-details', payload)
    resp = aiops_api.post(assert_action)
    return resp

def get_account_details_ui(url, host, payload, assert_action=False):
    """get the account details for the host"""
    aiops_api = RestApi(url, 'impacted-accounts/count', payload)
    resp = aiops_api.post(assert_action)
    acct_names = []
    if resp.get('success', False) and resp.get('count') > 0:
        if resp.get('count') > 10:
            total_pages = math.ceil(resp.get('count') / 10)
            for page in range(1, total_pages + 1):
                payload['pageNumber'] = page
                payload['pageSize'] = resp.get('count', 10)
                aiops_api = RestApi(url, 'impacted-accounts/paginated', payload)
                resp = aiops_api.post(assert_action)
                acct_names.extend(resp.get('account_names'))
                time.sleep(3)
            acct_names = [item['DISTINCT acc.name'] for item in acct_names]
            acct_names = list(set(acct_names))
            log.info(f'hosts:{host}, available accounts:{len(acct_names)} in UI: {acct_names}')
            return acct_names
        else:
            payload['pageNumber'] = 1
            payload['pageSize'] = resp.get('count', 10)
            aiops_api = RestApi(url, 'impacted-accounts/paginated', payload)
            resp = aiops_api.post(assert_action)
            if resp.get('success', False):
                acct_names = [item['DISTINCT acc.name'] for item in resp.get('account_names', [])]
                acct_names = list(set(acct_names))
                log.info(f"hosts:{host}, available accounts:{len(resp.get('account_names'))} in UI: {acct_names}")
            return acct_names
    else:
        log.warning(f'host: {host}, available accounts in UI: []')
        return []

def get_account_details_data_sync(url, host, payload, data_sync_key):
    """get the account details from data sync for the host"""
    aiops_api = RestApi(url, '', payload)
    resp = aiops_api.post_db_status(data_sync_key)
    acct_names = [item['DISTINCT acc.name'] for item in resp]
    acct_names = list(set(acct_names))
    log.info(f'Found: {len(resp)} accounts in data_sync for the Host: {host} Acct: {acct_names}')
    return acct_names

def db_payload(host, timestamp, partial=False):
    """create the DB payload with the provided details"""
    if partial:
        host_info = f"incs.accountId = acc.id AND incs.hostId LIKE '{host}%'"
    else:
        host_info = f"incs.accountId = acc.id AND incs.hostId LIKE '{host}'"
    data = {"select": ["DISTINCT acc.name"], "from": ["instances AS incs", "accounts AS acc"],
            "where": host_info,
            "page": 1, "page_size": 500,
            "timeRange": timestamp }
    return data

def servicenow_data(url, incident, snow_key, find=None):
    """get the servicenow data for the incident"""
    log.info(f'Checking host information details from SNOW #{incident}')
    aiops_api = RestApi(url, incident)
    resp = aiops_api.get('snow', snow_key)
    host = None
    if resp.get('number', None) == incident:
        host = get_incident_details_hostuuid(resp, find)
    log.info(f'hosts/uuid:{host}, found for the incident {incident} from SNOW')
    return resp, host

def compare_api_db_response(resp_api, resp_db):
    """compare API and DB response"""
    if resp_api and resp_db:
        set1 = sorted(resp_api)
        set2 = sorted(resp_db)
    elif resp_api == resp_db:
        set1 = set2 = resp_api
    elif resp_api == [] and resp_db.get('error',[]) == 'External API error':
        set1 = set2 = []
    else:
        set1 = resp_api
        set2 = resp_db
    if set1 == set2:
        log.info('API & data_sync response are same')
        return True
    else:
        log.error(f'API & data_sync response are different')
        return False

def compare_host_ui_snow(ui_data, snow_data, slack_data):
    log.info('combine & compare the UI and (SNOW & SLACK) hosts/uuid data')
    merge_data  = sorted(set(snow_data + slack_data))
    if ui_data == merge_data == []:
        log.warning(f'host/uuid from UI:{ui_data} & MERGE:{merge_data} response are empty')
        return True
    elif ui_data == merge_data:
        log.info(f'host/uuid from UI:{ui_data} & MERGE:{merge_data} response are same')
        return True
    else:
        log.error(f'UI:{ui_data} & MERGE:{merge_data} response are different')
        return False

def convert_date_format(date_str, witht=False, nospace=False):
    dt = datetime.strptime(date_str, '%d:%m:%Y:%H:%M')
    if witht:
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    elif nospace:
        return dt.strftime('%Y%m%d%H%M%S')
    else:
        return dt.strftime('%Y-%m-%d %H:%M:%S')

def filter_acct_parquet_file(instances, accounts, host, start_end_time):
    log.info(f'Check acct info for the host: {host} in parquet files')
    query_params = f"""
    SELECT DISTINCT acc.name FROM {instances} AS incs JOIN {accounts} AS acc
    ON incs.accountId = acc.id WHERE incs.hostId LIKE '{host}'
    AND incs.lastRefresh >= '{start_end_time[0]}'
    AND incs.lastRefresh <= '{start_end_time[1]}'
    """
    if 0:
        # Minimum and maximum timestamp in instances Parquet file
        query_instances = f"""
        SELECT MIN(lastRefresh) AS min_timestamp, MAX(lastRefresh) AS max_timestamp
        FROM '{instances}'
        """
        result = duckdb.query(query_instances).to_df()
        timestamps = [str(dt).split('.')[0]  for dt in result.values[0]]
        log.info(f'start time of {instances} is {timestamps[0]}')
        log.info(f'end time of {instances} is {timestamps[1]}')
    log.info(f'parquet query:{query_params}')
    # Execute the query
    results = duckdb.query(query_params).to_df()
    if results.any().tolist()[0]:
        acct_info = results['name'].drop_duplicates().tolist()
        log.info(f'hosts:{host}, available accts cnt:{len(acct_info)} available accts:{acct_info}')
        return acct_info
    else:
        log.warning(f'hosts:{host}, available accounts in parquet files: []')
        return []

def filter_cr_parquet_file(ui_resp, cr_parquet_file, start_end_time,
                           impact=None, state=None, dc=None, search=None, service=None):
    log.info(f'Check change request ids in parquet file:{cr_parquet_file}')
    tribe = ui_resp['tribe']
    region = ui_resp['location_identifier']['region'][0]
    dc_list = ui_resp['location_identifier']['dc'] + [region]
    search_field = search if search else 'actual_end'
    query_params = f"""
    SELECT DISTINCT number FROM {cr_parquet_file} 
    WHERE {search_field} >= '{start_end_time[0]}' AND {search_field} <= '{start_end_time[1]}'
    AND tribe = '{tribe}' AND '{region}' = ANY(regions) """
    if impact:
        query_params += f""" AND customer_impact IN {tuple(impact.split(','))} """
    if state:
        query_params += f""" AND state IN {tuple(state.split(','))} """
    if dc:
        query_params += f""" AND ARRAY[{', '.join(['\'%s\'' % dc for dc in dc_list])}] && dc """
    if service:
        query_params += f""" AND '{service}' = ANY(service_names) """
    log.info(f'change request parquet query:{query_params}')
    # Execute the query
    results = duckdb.query(query_params).to_df()
    if results.any().tolist()[0]:
        cr_info = results['number'].drop_duplicates().tolist()
        log.info(f"region:{region}, parquet_cr_id:{cr_info}")
        return cr_info
    else:
        log.warning(f"available cr's:[]")
        return 'no cr'

def filter_incident_parquet_file(cr_parquet_file, start_end_time,
                                 region=None, service=None):
    log.info(f'Check incident ids in parquet files: {cr_parquet_file}')
    #query_params = f"""
    #SELECT DISTINCT number FROM {cr_parquet_file}
    #WHERE created >= '{start_end_time[0]}' AND created <= '{start_end_time[1]}'
    #AND  'TIP.SA' = ANY(created_by) AND '{region}' = ANY(regions) """
    query_params = f"""
    SELECT DISTINCT number, regions FROM {cr_parquet_file} 
    WHERE created >= '{start_end_time[0]}' AND created <= '{start_end_time[1]}'
    AND  created_by LIKE '%TIP.SA%' AND regions LIKE '%{region}%' """
    #if service:
    #    query_params += f""" AND '{service}' = ANY(service_names) """
    if service:
        query_params += f""" AND service_names LIKE '%{service}%'"""
    log.info(f'change request parquet query:{query_params}')
    # Execute the query
    results = duckdb.query(query_params).to_df()
    if results.any().tolist()[0]:
        cr_info = results['number'].drop_duplicates().tolist()
        log.info(f"region:{region}, parquet_cr_id:{cr_info}")
        return create_incident_path([region])['dc'], cr_info
    else:
        log.warning(f"available cr's:[]")
        return False

def filter_cr_parquet_file_without_incident(cr_parquet_file, start_end_time, region=None,
                                            impact=None, dc=None, search=None, state=None,
                                            service=None, tribe=None):
    log.info(f'Check change request ids in parquet files')
    dc_list = create_incident_path([region])['dc'] + [region]
    search_field = search if search else 'actual_end'
    query_params = f"""
    SELECT DISTINCT number FROM {cr_parquet_file} 
    WHERE {search_field} >= '{start_end_time[0]}' AND {search_field} <= '{start_end_time[1]}'
    AND tribe = '{tribe}' AND '{region}' = ANY(regions) """
    if impact:
        query_params += f""" AND customer_impact IN {tuple(impact.split(','))} """
    if state:
        query_params += f""" AND state IN {tuple(state.split(','))} """
    if service:
        query_params += f""" AND '{service}' = ANY(service_names) """
    if dc:
        query_params += f""" AND ARRAY[{', '.join(['\'%s\'' % dc for dc in dc_list])}] && dc """
    log.info(f'change request parquet query:{query_params}')
    # Execute the query
    results = duckdb.query(query_params).to_df()
    if results.any().tolist()[0]:
        cr_info = results['number'].drop_duplicates().tolist()
        log.info(f"available cr's for the given timerange in parquet file: {cr_parquet_file}")
        log.info(f"region:{region}, parquet_cr_id:{cr_info}")
        return cr_info
    else:
        log.warning(f"available cr's:[] in parquet file: {cr_parquet_file}")
        return 'no cr'

def validate_key_information_fields(ui_data, snow_data):
    fields = [ ('status', 'Status'), ('number', 'Incident Number'),
               ('assignment_group', 'Assignment Group'), ('assigned_to', 'Assigned To'),
               ('created_by', 'Created By'), ('resolved_by', 'Resolved By'),
               ('resolved', 'Resolved At'), ('created', 'Created At'),
               ('disruption_time', 'Outage Duration')]
    full_data = {}
    for ui_snow_key, ui_name in fields:
        if ui_data.get(ui_snow_key, None) == snow_data.get(ui_snow_key, None):
            if 'Outage Duration' == ui_name:
                output = ui_data.get(ui_snow_key, None)//60
            else:
                output = ui_data.get(ui_snow_key, None)
            full_data[ui_name] = {str(output): True}
        else:
            full_data[ui_name] = {None: False}
    return full_data

def validate_reference_information_fields(ui_data, snow_data):
    fields = [ ('tribe', 'Tribe'), ('crn_masks', 'Region'),
               ('crn_masks', 'Configuration Item'), ('affected_activity', 'Affected Activity'),
               ('problem','Problem Record'), ('caused_by_change_number','Caused by Change')]
    full_data = {}
    for ui_snow_key, ui_name in fields:
        if ui_data.get(ui_snow_key, None) == snow_data.get(ui_snow_key, None):
            ui_updated_data = ui_data.get(ui_snow_key, None)
            if ui_name == 'Region':
                if ui_data['location_identifier']['region'] == snow_data['location_identifier']['region']:
                    ui_updated_data = ui_data['location_identifier']['region'][0]
                else:
                    ui_updated_data = 'dummy_region'
            if ui_name == 'Configuration Item':
                ui_updated_data = ui_data.get(ui_snow_key, None)[0].split(':')[-6]
            full_data[ui_name] = {ui_updated_data: True}
        else:
            full_data[ui_name] = {None: False}
    return full_data

def convert_str_dict(input):
    output = {}
    for line in input.strip().split('\n'):
        if line.startswith('- **'):
            key_value = line.strip('- ').split(':**')
            if len(key_value) == 2:
                key = key_value[0].strip('*').strip()
                value = key_value[1].strip()
                if value.lower() == 'true':
                    value = True
                elif value.lower() == 'false':
                    value = False
                output[key] = value
    return output

def compare_database_and_snow_general_info(db_data, snow_data):
    fields = [('number', 'Incident Number'), ('severity', 'Severity'),
              ('created_by', 'Created By'), ('tribe', 'Tribe'),
              ('was_customer_impacted','Was Customer Impacted?'),
              ('caused_by_change','Caused by Change?')]
    # convert str to dict
    db_data = convert_str_dict(db_data)
    full_data = {}
    for db_snow_key, ui_name in fields:
        if str(db_data.get(ui_name, None)) == str(snow_data.get(db_snow_key, None)):
            full_data[ui_name] = {str(db_data.get(ui_name, None)): True}
        else:
            full_data[ui_name] = {str(db_data.get(ui_name, None)): False}
    return full_data

def validate_impact_description_fields(ui_data, snow_data):
    fields = [ ('customer_facing_impact', 'Customer Impact'), ('long_description', 'Description')]
    full_data = {}
    for ui_snow_key, ui_name in fields:
        if ui_data.get(ui_snow_key, None) == snow_data.get(ui_snow_key, None):
            full_data[ui_name] = {ui_data.get(ui_snow_key, None): True}
        else:
            full_data[ui_name] = {ui_data.get(ui_snow_key, None): False}
    return full_data

def validate_comments_list_fields(ui_data, snow_data):
    fields = [ ('comment_list', 'Comments')]
    ui_snow_key = ""
    for ui_snow_key, ui_name in fields:
        if ui_data.get(ui_snow_key, None) == snow_data.get(ui_snow_key, None):
            return True, ui_data.get(ui_snow_key)
    else:
        return False, ui_data.get(ui_snow_key)

def download_cr_cos_parquet_file(conf, module):
    log.info('download cr parquet file for the last 60days')
    cos = ibm_boto3.client('s3', ibm_api_key_id=conf['cr_cos_api_key'],
                                 ibm_service_instance_id=conf['cos_instance'],
                                 config=Config(signature_version='oauth'),
                                 endpoint_url=conf['cos_endpoint'])
    bucket = module['bucket']
    filename = module['filename']
    cr_file_object = cos.get_object(Bucket=bucket, Key=filename)
    data = json.loads(cr_file_object['Body'].read())
    cr_file_list = cos.list_objects_v2(Bucket=bucket, Prefix=data[module['prefix']])
    for cr_file_name in cr_file_list.get('Contents', []):
        file_name = cr_file_name['Key'].split('/')[-1]
        try:
            cr_file = cos.get_object(Bucket=bucket, Key=cr_file_name['Key'])
            file_content = cr_file['Body'].read()
            with open(file_name, 'wb') as file:
                file.write(file_content)
            log.info(f"File {file_name} downloaded successfully.")
        except Exception as e:
            assert False, f"Error downloading file {file_name}: {e}"

def download_parquet_file(conf):
    log.info('downloading parquet file to test static incident')
    cos = ibm_boto3.client('s3', ibm_api_key_id=conf['parquet_cos_api_key'],
                                 ibm_service_instance_id=conf['parquet_cos_instance'],
                                 config=Config(signature_version='oauth'),
                                 endpoint_url=conf['cos_endpoint'])
    bucket_name = conf['parquet_bucket_name']
    timestamp = conf['parquet_timestamp'].split(':')
    prefix = (f'vpc_objects_2rep/version=v1.0.0/env=production/'
              f'year={timestamp[2]}/month={timestamp[1]}/day={timestamp[0]}/'
              f'hour={timestamp[3]}/minute={timestamp[4]}/')
    response = cos.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    objects = response.get('Contents', [])
    for obj in objects:
        file_name = obj['Key'].split('/')[-1]
        if file_name.startswith('instances_') or file_name.startswith('accounts_'):
            try:
                response = cos.get_object(Bucket=bucket_name, Key=obj['Key'])
                file_data = response['Body'].read()
                with open(file_name, 'wb') as file:
                    file.write(file_data)
                log.info(f"File {file_name} downloaded successfully.")
            except Exception as e:
                assert False, f"Error downloading file {file_name}: {e}"

def load_slack_warroom_data(conf, incident, find=None):
    log.info('Checking host information details from SLACK')
    if find == 'uuid':
        patterns = uuid_patterns
    else:
        patterns = hostname_patterns
    bucket_name = conf['slack_bucket_name']
    def process_war_room(room_name):
        slack_cos = ibm_boto3.client('s3', ibm_api_key_id=conf['cos_api_key'],
                                     ibm_service_instance_id=conf['cos_instance'],
                                     ibm_auth_endpoint='https://iam.cloud.ibm.com/identity/token',
                                     config=Config(signature_version='oauth'),
                                     endpoint_url=conf['cos_endpoint'])
        try:
            resp = slack_cos.list_objects_v2(Bucket=bucket_name,
                    Prefix=f'slack_warrooms/{room_name}/{room_name}.json')
            hosts = []
            if resp.get('Contents'):
                for obj in resp.get('Contents', []):
                    if obj['Key']:
                        file_key = obj['Key']
                        file_obj = slack_cos.get_object(Bucket=bucket_name, Key=file_key)
                        file_content = file_obj['Body'].read().decode('utf-8')
                        json_data = json.loads(file_content)
                        if json_data:
                            log.info(f'response found for the slack_warroom:{room_name}, checking for hosts/uuid information')
                            data_str = json.dumps(json_data)
                            for pattern in patterns:
                                matches = re.findall(pattern, data_str)
                                hosts.extend(matches)
                            log.warning(f'hosts/uuid: {sorted(set(hosts))} found in slack_warroom:{room_name}')
            else:
                log.error(f'slack_warroom:{room_name} not found')
            return sorted(set(hosts))
        except Exception as e:
            log.error(f'Error processing {room_name}: {str(e)}')
            return []
    def process_archived_war_room(room_name):
        slack_cos = ibm_boto3.client('s3', ibm_api_key_id=conf['cos_api_key'],
                                     ibm_service_instance_id=conf['cos_instance'],
                                     config=Config(signature_version='oauth'),
                                     endpoint_url=conf['cos_endpoint'])
        hosts = []
        # Specify the bucket name and object name
        object_name = f'slack_warrooms/{room_name}/{room_name}.json'
        file_name = f'{room_name}.json'
        # Download the file from COS
        try:
            slack_cos.download_file(bucket_name, object_name, file_name)
            log.info(f"File {object_name} downloaded successfully")
        except Exception as e:
            log.error(f'Error downloading file: {e}')
            return []
        # Read the archived Slack channel file
        with open(file_name, 'r') as file:
            json_data = json.load(file)
            if json_data:
                log.info(f'response found for the archived slack_warroom:{room_name}, checking for hosts/uuid information')
                data_str = json.dumps(json_data)
                for pattern in patterns:
                    matches = re.findall(pattern, data_str)
                    hosts.extend(matches)
                log.warning(f'hosts/uuid: {sorted(set(hosts))} found in archived slack_warroom:{room_name}')
            else:
                log.error(f'archived slack_warroom:{room_name} not found')
        return sorted(set(hosts))

    slack_room_name = f'warroom-vpc-{incident.lower()}'
    log.info('Checking host/uuid details in SLACK warroom')
    hosts = process_war_room(slack_room_name)
    if not hosts:
        log.info('Checking host/uuid details in archived SLACK warroom')
        hosts = process_archived_war_room(slack_room_name)
    return hosts

def get_parquet_timerange(start, end):
    start_time = datetime.strptime(start, '%d:%m:%Y:%H:%M')
    end_time = datetime.strptime(end, '%d:%m:%Y:%H:%M')
    # Round up to the next 30-minute mark
    if start_time.minute % 30 != 0:
        start_time += timedelta(minutes=30 - start_time.minute % 30)
    # Round up end time to the next 30-minute mark
    if end_time.minute % 30 != 0:
        end_time += timedelta(minutes=30 - end_time.minute % 30)

    timestamps = []
    while start_time <= end_time:
        timestamps.append(start_time.strftime('%d:%m:%Y:%H:%M'))
        start_time += timedelta(minutes=30)
    return timestamps

def get_database_response(conf, incident_id):
    es_client = Elasticsearch(conf['elastic_host'], verify_certs=False,
                              basic_auth=(conf['elastic_user'],
                                          conf['elastic_password']))
    response = es_client.search(index=conf['elastic_index'],
                                query={"match": {'incidentid': incident_id}})
    data = dict(response)
    return data['hits']['hits'][0]['_source']['incident_summary']

def create_incident_path(inputs=None):
    """To create & add an incident path into ui & snow api response"""
    regions = []
    datacenters = []
    mzones = []
    zones = []
    for item in inputs:
        if re.match(r'^[a-z]{2}-[a-z0-9]{2,6}$', item):
            regions.append(item)
        elif re.match(r'^[a-z]{3}[0-9]{2}$', item):
            datacenters.append(item)
        elif item.startswith('mzone'):
            mzones.append(item)
        elif re.match(r'^[a-z]{2}-[a-z0-9]{2,6}-[0-9]$', item):
            zones.append(item)
    matches = []
    for item in completed_location_details:
        if (regions and item['region'] in regions) or (datacenters and item['datacenter'] in datacenters) or (
                mzones and item['mzone'] in mzones) or (zones and item['zone'] in zones):
            matches.append(item)
    if matches:
        regions_matched = list(set([match['region'] for match in matches]))
        mzones_matched = list(set([match['mzone'] for match in matches]))
        zones_matched = list(set([match['zone'] for match in matches]))
        dcs_matched = list(set([match['datacenter'] for match in matches]))
        # Return lists directly instead of comma-separated strings
        return {
            "region": sorted(regions_matched),
            "mzone": sorted(mzones_matched),
            "dc": sorted(dcs_matched),
            "zone": sorted(zones_matched),
        }
    # Default: empty lists when nothing matches
    return {"region": [], "mzone": [], "dc": [], "zone": []}

def extract_service_names(incident_data):
    """
    Extract service names from CRN masks in incident data
    """
    service_names = []
    if 'crn_masks' in incident_data and incident_data['crn_masks']:
        for crn_mask in incident_data['crn_masks']:
            parts = crn_mask.split(':')
            if len(parts) > 4 and parts[4]:
                service_names.append(parts[4])
    return service_names

def is_tip_generated(root_incident_id, timeline_resp):
    """Return TIP status for all incidents in timeline for a root incident.
    Args:
        root_incident_id: Incident id from test_config/var_incident_list.
        timeline_resp: List of incident dictionaries returned by alert-timeline.
    Returns:
        {root_incident_id: {timeline_incident_id: True/False, ...}}
    """
    all_results = []
    for incident in timeline_resp:
        tip_status_map = {}
        incident_id_resp = incident.get('number')
        is_tip = incident.get('created_by') == 'TIP.SA'
        tip_status_map[incident_id_resp] = is_tip if is_tip else False
        if incident['regions']:
            tip_status_map['datacenter'] = create_incident_path([incident['regions'].strip('["]').strip('"')])['dc']
        elif incident['crn_masks'].split(':')[5]:
            tip_status_map['datacenter'] = create_incident_path([incident['crn_masks'].split(':')[5]])['dc']
        else:
            tip_status_map['datacenter'] = incident['metadata']['zones'][0]['datacenter']
        all_results.append(tip_status_map)
    return all_results

def write_ca_cert(base64_cert: str) -> str:
    pem_bytes = base64.b64decode(base64_cert)
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")
    temp_file.write(pem_bytes)
    temp_file.close()
    return temp_file.name



def compute_region_dc_tribe_groups(docs):
    """
    Group CRs by region → dc → tribe.
    Returns:
        {
           (region, dc, tribe): {
               "count": int,
               "cr_ids": [ ... ]
           }
        }
    """
    
    # If docs is dict convert to list
    if isinstance(docs, dict):
        docs = docs.values()

    groups = {}

    for wrapper in docs:

        # Pick REST or Elastic representation
        if "rest_data" in wrapper:
            doc = wrapper["rest_data"]
        elif "elastic_data" in wrapper:
            doc = wrapper["elastic_data"]
        else:
            doc = wrapper

        regions = doc.get("regions") or []
        dcs = doc.get("dc") or []
        tribe = doc.get("tribe", "")

        cr_id = doc.get("id")

        # normalize
        if not isinstance(regions, list):
            regions = [regions]
        if not isinstance(dcs, list):
            dcs = [dcs]
        if not isinstance(tribe, list):
            tribe = [tribe]

        # Clean empty values
        regions = [r for r in regions if r]
        dcs = [d for d in dcs if d]
        tribe = [t for t in tribe if t]

        # Use placeholders if empty
        if not regions:
            regions = ["[No Region]"]
        if not dcs:
            dcs = ["[No DC]"]
        if not tribe:
            tribe = ["[No Tribe]"]

        # Build grouping
        for region in regions:
            for dc in dcs:
                for t in tribe:
                    key = (region, dc, t)

                    if key not in groups:
                        groups[key] = {
                            "count": 0,
                            "cr_ids": []
                        }

                    groups[key]["count"] += 1
                    groups[key]["cr_ids"].append(cr_id)

    return groups

def calculate_time_range(hours=None):
    now = datetime.now(timezone.utc)

    if hours:
        start = now
        end = now + timedelta(hours=int(hours))
    else:
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1) - timedelta(milliseconds=1)

    return start, end

def pretty_print(title, data):
    log.info("=" * 40)
    log.info(title)
    log.info(json.dumps(data, indent=2, default=str))
    log.info("=" * 40)
