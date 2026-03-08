import logging as log
from pytest import mark
import aiops_restapi as restapi

def validate_host_details(resp):
    """validate the host details"""
    host_details = {'host': None, 'accounts':[]}
    if not resp.get('success', False):
        log.error(f'host details validation failed: {resp.get("error")}')
        return False, host_details
    else:
        for host in  resp['hosts']:
            host_details['host'] = host
            acct_details = []
            for acct in host.get('content', []):
                if  acct.get('accountId', None):
                    acct_details.append({'id': acct.get('accountId'), 'name': acct.get('name'),
                                         'state': acct.get('state')})
            host_details['accounts'] = acct_details
    return True, host_details

def process_host_details(config, ui_host_list, timestamp):
    acct_results = {}
    # 8. iterate through the fetched host details.
    for host in ui_host_list:
        data = {'hostId': host, 'timeRange': timestamp}
        # 9. fetch account details from ui via host-details api.
        host_details_resp = restapi.get_host_details(config['var_endpoint_ui'], host, data)
        host_details_status, host_details = validate_host_details(host_details_resp)
        ins_cnt_list = host_details['accounts']
        acct_cnt_list = list(set([host['id'] for host in host_details['accounts']]))
        log.info(f'host_ui_status: {host_details_status}, host_ui_inst_count: {len(ins_cnt_list)},'
                 f'host_ui_acct_count: {len(acct_cnt_list)}')
        data_db = restapi.db_payload(host, timestamp)
        # 10. fetch account details from data-sync via queryWithDownload api.
        resp_local = restapi.get_account_details_data_sync(config['var_endpoint_db'],
                                                           host, data_db, config['data_sync_api_key'])
        log.info(f'host_db_count: {len(resp_local)}, host_db_details: {resp_local}')
        # 11. compare the length of collected account details between ui and data-sync.(should be same)
        if len(acct_cnt_list) == len(resp_local):
            log.info(f'ui & data sync host details length are equal for host: {host}')
            acct_results[host] = True
        else:
            log.error(f'ui & data sync host details length are not equal for {host}')
            log.info(f'ui host details:{acct_cnt_list}, datasync host details:{resp_local}')
            acct_results[host] = False
    return acct_results if acct_results else {'no host': 'No resources available'}

def assert_incidents_failed(results):
    for incident in results:
        if incident['host_results'] is False or False in incident['acct_results'].values():
            assert False, f'Not all incidents:{incident} returned excepted values'


@mark.smoke
@mark.funct_host_details
def test_aiops_host_details1_fetch_account_details(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. check & extract host details of incident from UI using ui-api.
    3. fetch the host details from service now via snow-api.
    4. fetch the host details from slack war-room cos bucket.
    5. if war-room cos bucket not available, then fetch from slack archive cos bucket.
    6. compare the collected host details between ui and (snow + slack).(should be same)
    7. setting the timerange between incident created time and additional 30 mins.
    8. iterate through the fetched host details.
    9. fetch account details from ui via host-details api.
    10. fetch account details from data-sync via queryWithDownload api.
    11. compare the length of collected account details between ui and data-sync.(should be same)
    """
    log.info(test_aiops_host_details1_fetch_account_details.__doc__)
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        acct_results = {}
        results = {'incident': incident_list}
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # 2. check & extract host details of incident from UI using ui-api.
        ui_resp = ui_resp_data[incident_list]['ui_data']
        ui_host_info = ui_resp_data[incident_list]['host_id']
        # 3. fetch the host details from service now via snow-api.
        snow_resp = snow_resp_data[incident_list]['snow_data']
        snow_host_info = snow_resp_data[incident_list]['host_id']
        # 4. fetch the host details from slack war-room cos bucket.
        # 5. if war-room cos bucket not available, then fetch from slack archive cos bucket.
        slack_host_info = restapi.load_slack_warroom_data(test_config, incident_list.lower())
        # 6. compare the collected host details between ui and (snow + slack).(should be same)
        results['host_results'] = restapi.compare_host_ui_snow(ui_host_info, snow_host_info, slack_host_info)
        # 7. setting the timerange between incident created time and additional 30 mins.
        timerange = restapi.get_timerange(ui_resp['created'])
        log.info(f'Found Host count: {len(ui_host_info)}, Host info: {ui_host_info}')
        if ui_host_info:
            acct_results = process_host_details(test_config, ui_host_info, timerange)
        results['acct_results'] = acct_results or {'no host': 'No resources available'}
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    assert_incidents_failed(all_results)


@mark.funct_host_details
def test_aiops_host_details2_fetch_host_partial_name(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. check & extract host details of incident from UI using ui-api.
    3. fetch the host details from service now via snow-api.
    4. fetch the host details from slack war-room cos bucket.
    5. if war-room cos bucket not available, then fetch from slack archive cos bucket.
    6. compare the collected host details between ui and (snow + slack).(should be same)
    7. setting the timerange between incident created time and additional 30 mins.
    8. fetch account details from ui via host-details api with partial host-name.
    9. fetch account details from data-sync via queryWithDownload api.
    10. compare the length of collected account details between ui and data-sync.(should be same)
    """
    log.info(test_aiops_host_details2_fetch_host_partial_name.__doc__)
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        acct_results = {}
        results = {'incident': incident_list}
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # 2. check & extract host details of incident from UI using ui-api.
        ui_resp = ui_resp_data[incident_list]['ui_data']
        ui_host_info = ui_resp_data[incident_list]['host_id']
        # 3. fetch the host details from service now via snow-api.
        snow_resp = snow_resp_data[incident_list]['snow_data']
        snow_host_info = snow_resp_data[incident_list]['host_id']
        # 4. fetch the host details from slack war-room cos bucket.
        # 5. if war-room cos bucket not available, then fetch from slack archive cos bucket.
        slack_host_info = restapi.load_slack_warroom_data(test_config, incident_list.lower())
        # 6. compare the collected host details between ui and (snow + slack).(should be same)
        results['host_results'] = restapi.compare_host_ui_snow(ui_host_info, snow_host_info, slack_host_info)
        # 7. setting the timerange between incident created time and additional 30 mins.
        timerange = restapi.get_timerange(ui_resp['created'])
        log.info(f'Found Host count: {len(ui_host_info)}, Host info: {ui_host_info}')
        if ui_host_info:
            ui_host_info = ['-'.join(ui_host_info[-1].split('-')[0:4])]
            acct_results = process_host_details(test_config, ui_host_info, timerange)
        results['acct_results'] = acct_results or {'no host': 'No resources available'}
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    assert_incidents_failed(all_results)


@mark.funct_host_details
def test_aiops_host_details3_fetch_host_wrong_name(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. check & extract host details of incident from UI using ui-api.
    3. fetch the host details from service now via snow-api.
    4. fetch the host details from slack war-room cos bucket.
    5. if war-room cos bucket not available, then fetch from slack archive cos bucket.
    6. compare the collected host details between ui and (snow + slack).(should be same)
    7. setting the timerange between incident created time and additional 30 mins.
    8. fetch account details from ui via host-details api with partial host-name.
    9. fetch account details from data-sync via queryWithDownload api.
    10. compare the length of collected account details between ui and data-sync.(should be same)
    """
    log.info(test_aiops_host_details3_fetch_host_wrong_name.__doc__)
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        acct_results = {}
        results = {'incident': incident_list}
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # 2. check & extract host details of incident from UI using ui-api.
        ui_resp = ui_resp_data[incident_list]['ui_data']
        ui_host_info = ui_resp_data[incident_list]['host_id']
        # 3. fetch the host details from service now via snow-api.
        snow_resp = snow_resp_data[incident_list]['snow_data']
        snow_host_info = snow_resp_data[incident_list]['host_id']
        # 4. fetch the host details from slack war-room cos bucket.
        # 5. if war-room cos bucket not available, then fetch from slack archive cos bucket.
        slack_host_info = restapi.load_slack_warroom_data(test_config, incident_list.lower())
        # 6. compare the collected host details between ui and (snow + slack).(should be same)
        results['host_results'] = restapi.compare_host_ui_snow(ui_host_info, snow_host_info, slack_host_info)
        # 7. setting the timerange between incident created time and additional 30 mins.
        timerange = restapi.get_timerange(ui_resp['created'])
        log.info(f'Found Host count: {len(ui_host_info)}, Host info: {ui_host_info}')
        if ui_host_info:
            ui_host_info = ['host_dummy']
            acct_results = process_host_details(test_config, ui_host_info, timerange)
        results['acct_results'] = acct_results or {'no host': 'No resources available'}
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    assert_incidents_failed(all_results)
