import glob
import logging as log
from pytest import mark
import aiops_restapi as restapi

def assert_incidents_failed(results):
    for incident in results:
        if incident['host_results'] is False or False in incident['acct_results'].values():
            assert False, f'Not all incidents:{incident} returned excepted values'

def process_host_details(config, incident, hosts, ui_data, partial=None):
    """ process host details """
    acct_results = {}
    log.info(f'Found Host count: {len(hosts)}, Host info: {hosts}')
    log.info(f'VALIDATE ACCOUNT INFORMATIONS')
    # setting timerange between incident created time and additional 30 mins
    timerange = restapi.get_timerange(ui_data['created'])
    for host in hosts:
        data = {'hostId': host, 'createdAtRange': timerange}
        log.info(f'check account details with UI api for the Host: {host}')
        # get the account details with ui api
        resp_acct_api = restapi.get_account_details_ui(config['var_endpoint_ui'], host, data)
        if partial: host = host + '%'
        if incident == 'INC9258603':
            # fetch account details from the static parquet files.
            result = process_account_details_inc9258603(config, host, resp_acct_api)
        else:
            # fetch account details from data-sync via queryWithDownload api.
            result = process_account_details(config, host, resp_acct_api, timerange)
        acct_results.update({host: result})
    return acct_results

def process_account_details_inc9258603(config, host, ui_acct_info):
    """ process account details for INC9258603 """
    tf_name = restapi.convert_date_format(config['static_parquet_tf'], nospace=True)
    instances = [file for file in glob.glob('instances*.parquet') if tf_name in file][0]
    accounts = [file for file in glob.glob('accounts*.parquet') if tf_name in file][0]
    start_end_time = (restapi.convert_date_format(config['static_st_parquet_tf']),
                      restapi.convert_date_format(config['static_ed_parquet_tf']))
    # get account info details from parquet files
    parquet_acct_info = restapi.filter_acct_parquet_file(instances, accounts, host, start_end_time)
    result = sorted(ui_acct_info) == sorted(parquet_acct_info)
    log.info(f'Account details are {"same" if result else "not same"} '
             f'for host {host} between UI & parquet')
    return result

def process_account_details(config, host, ui_acct_info, timestamp):
    """ process account details """
    data_db = restapi.db_payload(host, timestamp)
    log.info(f'check account details in datasync for the Host: {host} with Payload: {data_db}')
    # get the account details with datasync api
    resp_local = restapi.get_account_details_data_sync(config['var_endpoint_db'],
                                                       host, data_db, config['data_sync_api_key'])
    return restapi.compare_api_db_response(ui_acct_info, resp_local)

@mark.smoke
@mark.funct_impacted_account
def test_aiops_impacted_acc1_fetch_host_details(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. check & extract host details of incident from UI using ui-api.
    3. if incident is closed/resolved then validate the historical data else real-time data need to be validated.
    4. fetch the host details from service now via snow-api.
    5. fetch the host details from slack war-room cos bucket.
    6. if war-room cos bucket not available, then fetch from slack archive cos bucket.
    7. compare the collected host details between ui and (snow + slack).(should be same)
    """
    log.info(test_aiops_impacted_acc1_fetch_host_details.__doc__)
    all_results = []
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        log.info(f'VALIDATE HOST INFORMATIONS')
        # get incident details from UI API
        ui_resp = ui_resp_data[incident_list]['ui_data']
        ui_host_info = ui_resp_data[incident_list]['host_id']
        log.info(f"Incident:{incident_list} in state: {ui_resp['state']}, fetching historical data ")
        # get incident details from SNOW API
        # snow_resp = snow_resp_data[incident_list]['snow_data']
        snow_host_info = snow_resp_data[incident_list]['host_id']
        # get incident details from slack war room
        slack_host_info = restapi.load_slack_warroom_data(test_config, incident_list.lower())
        # compare the UI and (SNOW & SLACK) method
        result = restapi.compare_host_ui_snow(ui_host_info, snow_host_info, slack_host_info)
        results = {'incident' : incident_list, 'host_results': result}
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        if incident['host_results'] is False:
            assert False, f'Not all incidents:{incident} returned excepted values'


@mark.smoke
@mark.funct_impacted_account
def test_aiops_impacted_acc2_fetch_account_details(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. check & extract host details of incident from UI using ui-api.
    3. if incident is closed/resolved then validate the historical data else real-time data need to be validated.
    --> historical data workflow(closed/resolved incident)
        4. fetch the host details from service now via snow-api.
        5. fetch the host details from slack war-room cos bucket.
        6. if war-room cos bucket not available, then fetch from slack archive cos bucket.
        7. compare the collected host details between ui and (snow + slack).(should be same)
        ### Validate account details ###
        8. iterate through the fetched host details.
        9. setting the timerange between incident created time and additional 30 mins.
        10. fetch account details from ui via count & paginated ui-api.
        --> if incident is INC9258603
            11. fetch account details from the static parquet files.
            12. compare the collected account details between ui and parquet files.(should be same)
        --> if incident is not INC9258603
            13. fetch account details from data-sync via queryWithDownload api.
            14. compare the collected account details between ui and data-sync.(should be same)
    --> real-time data workflow(new/in-progress incident)
        pass
    """
    log.info(test_aiops_impacted_acc2_fetch_account_details.__doc__)
    all_results = []
    for incident_list in test_config['var_incident_list']:
        results = {}
        acct_results = {}
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        log.info(f'VALIDATE HOST INFORMATIONS')
        # get incident details from UI API
        ui_resp = ui_resp_data[incident_list]['ui_data']
        ui_host_info = ui_resp_data[incident_list]['host_id']
        if ui_resp['state'] in ['resolved', 'closed']:
            log.info(f"Incident:{incident_list} in state: {ui_resp['state']}, fetching historical data ")
            # get incident details from SNOW API
            #snow_resp = snow_resp_data[incident_list]['snow_data']
            snow_host_info = snow_resp_data[incident_list]['host_id']
            # get incident details from slack war room
            slack_host_info = restapi.load_slack_warroom_data(test_config, incident_list.lower())
            # compare the UI and (SNOW & SLACK) method
            result = restapi.compare_host_ui_snow(ui_host_info, snow_host_info, slack_host_info)
            results = {'incident' : incident_list, 'host_results': result}
            if ui_host_info:
                acct_results = process_host_details(test_config, incident_list, ui_host_info, ui_resp)
            else:
                log.error(f'No Host info for the incident: {incident_list}')
        else:
            log.info(f"Incident:{incident_list} in state: {ui_resp['state']}, fetching real time data ")
        results['acct_results'] = acct_results or {'no host': 'No resources available'}
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    assert_incidents_failed(all_results)


@mark.funct_impacted_account
def test_aiops_impacted_acc3_fetch_host_partial_name(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. check & extract host details of incident from UI using ui-api.
    3. if incident is closed/resolved then validate the historical data else real-time data need to be validated.
    --> historical data workflow(closed/resolved incident)
        4. fetch the host details from service now via snow-api.
        5. fetch the host details from slack war-room cos bucket.
        6. if war-room cos bucket not available, then fetch from slack archive cos bucket.
        7. compare the collected host details between ui and (snow + slack).(should be same)
        ### Validate account details ###
        8. set the host name as a common prefix of all collected hosts.
        9. setting the timerange between incident created time and additional 30 mins.
        10. fetch account details from ui via count & paginated ui-api.
        --> if incident is INC9258603
            11. fetch account details from the static parquet files.
            12. compare the collected account details between ui and parquet files.(should be same)
        --> if incident is not INC9258603
            13. fetch account details from data-sync via queryWithDownload api.
            14. compare the collected account details between ui and data-sync.(should be same)
    --> real-time data workflow(new/in-progress incident)
        pass
    """
    log.info(test_aiops_impacted_acc3_fetch_host_partial_name.__doc__)
    all_results = []
    for incident_list in test_config['var_incident_list']:
        results = {}
        acct_results = {}
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        log.info(f'VALIDATE HOST INFORMATIONS')
        # get incident details from UI API
        ui_resp = ui_resp_data[incident_list]['ui_data']
        ui_host_info = ui_resp_data[incident_list]['host_id']
        if ui_resp['state'] in ['resolved', 'closed']:
            log.info(f"Incident:{incident_list} in state: {ui_resp['state']}, fetching historical data ")
            # get incident details from SNOW API
            #snow_resp = snow_resp_data[incident_list]['snow_data']
            snow_host_info = snow_resp_data[incident_list]['host_id']
            # get incident details from slack war room
            slack_host_info = restapi.load_slack_warroom_data(test_config, incident_list.lower())
            # compare the UI and (SNOW & SLACK) method
            result = restapi.compare_host_ui_snow(ui_host_info, snow_host_info, slack_host_info)
            results = {'incident' : incident_list, 'host_results': result}
            if ui_host_info:
                ui_host_info = ['-'.join(ui_host_info[-1].split('-')[0:4])]
                acct_results = process_host_details(test_config, incident_list, ui_host_info, ui_resp, True)
            else:
                log.error(f'No Host info for the incident: {incident_list}')
        else:
            log.info(f"Incident:{incident_list} in state: {ui_resp['state']}, fetching real time data ")
        results['acct_results'] = acct_results or {'no host': 'No resources available'}
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    assert_incidents_failed(all_results)


@mark.funct_impacted_account
def test_aiops_impacted_acc4_fetch_host_wrong_name(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. check & extract host details of incident from UI using ui-api.
    3. if incident is closed/resolved then validate the historical data else real-time data need to be validated.
    --> historical data workflow(closed/resolved incident)
        4. fetch the host details from service now via snow-api.
        5. fetch the host details from slack war-room cos bucket.
        6. if war-room cos bucket not available, then fetch from slack archive cos bucket.
        7. compare the collected host details between ui and (snow + slack).(should be same)
        ### Validate account details ###
        8. set the host name as a dummy name(unknown)
        9. setting the timerange between incident created time and additional 30 mins.
        10. fetch account details from ui via count & paginated ui-api.
        --> if incident is INC9258603
            11. fetch account details from the static parquet files.
            12. compare the collected account details between ui and parquet files.(should be same)
        --> if incident is not INC9258603
            13. fetch account details from data-sync via queryWithDownload api.
            14. compare the collected account details between ui and data-sync.(should be same)
    --> real-time data workflow(new/in-progress incident)
        pass
    """
    log.info(test_aiops_impacted_acc4_fetch_host_wrong_name.__doc__)
    all_results = []
    for incident_list in test_config['var_incident_list']:
        results = {}
        acct_results = {}
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        log.info(f'VALIDATE HOST INFORMATIONS')
        # get incident details from UI API
        ui_resp = ui_resp_data[incident_list]['ui_data']
        ui_host_info = ui_resp_data[incident_list]['host_id']
        if ui_resp['state'] in ['resolved', 'closed']:
            log.info(f"Incident:{incident_list} in state: {ui_resp['state']}, fetching historical data ")
            # get incident details from SNOW API
            # snow_resp = snow_resp_data[incident_list]['snow_data']
            snow_host_info = snow_resp_data[incident_list]['host_id']
            # get incident details from slack war room
            slack_host_info = restapi.load_slack_warroom_data(test_config, incident_list.lower())
            # compare the UI and (SNOW & SLACK) method
            result = restapi.compare_host_ui_snow(ui_host_info, snow_host_info, slack_host_info)
            results = {'incident' : incident_list, 'host_results': result}
            if ui_host_info:
                ui_host_info = ['host_dummy']
                acct_results = process_host_details(test_config, incident_list, ui_host_info, ui_resp)
            else:
                log.error(f'No Host info for the incident: {incident_list}')
        else:
            log.info(f"Incident:{incident_list} in state: {ui_resp['state']}, fetching real time data ")
        results['acct_results'] = acct_results or {'no host': 'No resources available'}
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    assert_incidents_failed(all_results)


@mark.funct_impacted_account
def test_aiops_impacted_acc5_fetch_host_custom_timerange(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. check & extract host details of incident from UI using ui-api.
    3. if incident is closed/resolved then validate the historical data else real-time data need to be validated.
    --> historical data workflow(closed/resolved incident)
        4. fetch the host details from service now via snow-api.
        5. fetch the host details from slack war-room cos bucket.
        6. if war-room cos bucket not available, then fetch from slack archive cos bucket.
        7. compare the collected host details between ui and (snow + slack).(should be same)
        ### Validate account details ###
        8. setting the timerange between incident created time and resolved time.
        9. fetch account details from ui via count & paginated ui-api.
        --> if incident is INC9258603
            10. fetch account details from the static parquet files.
            11. compare the collected account details between ui and parquet files.(should be same)
        --> if incident is not INC9258603
            12. fetch account details from data-sync via queryWithDownload api.
            13. compare the collected account details between ui and data-sync.(should be same)
    --> real-time data workflow(new/in-progress incident)
        pass
    """
    log.info(test_aiops_impacted_acc5_fetch_host_custom_timerange.__doc__)
    all_results = []
    for incident_list in test_config['var_incident_list']:
        results = {}
        acct_results = {}
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        log.info(f'VALIDATE HOST INFORMATIONS')
        # get incident details from UI API
        ui_resp = ui_resp_data[incident_list]['ui_data']
        ui_host_info = ui_resp_data[incident_list]['host_id']
        if ui_resp['state'] in ['resolved', 'closed']:
            log.info(f"Incident:{incident_list} in state: {ui_resp['state']}, fetching historical data "
            )
            # get incident details from SNOW API
            # snow_resp = snow_resp_data[incident_list]['snow_data']
            snow_host_info = snow_resp_data[incident_list]['host_id']
            # get incident details from slack war room
            slack_host_info = restapi.load_slack_warroom_data(test_config, incident_list.lower())
            # compare the UI and (SNOW & SLACK) method
            result = restapi.compare_host_ui_snow(ui_host_info, snow_host_info, slack_host_info)
            results = {'incident' : incident_list, 'host_results': result}
            if ui_host_info:
                log.info(f'Found Host count: {len(ui_host_info)}, Host info: {ui_host_info}')
                log.info(f'VALIDATE ACCOUNT INFORMATIONS')
                # setting timerange between incident created time and additional 30 mins
                st_time = restapi.convert_date_format(test_config['start_parquet_timestamp'], True)
                ed_time = restapi.convert_date_format(test_config['end_parquet_timestamp'], True)
                timerange = f'>={st_time}<={ed_time}'
                for host in ui_host_info:
                    data = {'hostId': host, 'createdAtRange': timerange}
                    log.info(f'check account details with UI api for the Host: {host}')
                    # get the account details with ui api
                    resp_acct_api = restapi.get_account_details_ui(test_config['var_endpoint_ui'], host, data)
                    if incident_list == 'INC9258603':
                        instances = glob.glob('instances*.parquet')
                        accounts = glob.glob('accounts*.parquet')
                        start_end_time = (restapi.convert_date_format(test_config['start_parquet_timestamp']),
                                          restapi.convert_date_format(test_config['end_parquet_timestamp']))
                        # get account info details from parquet files
                        parquet_acct_info = []
                        for ins, acs in zip(instances, accounts):
                            parquet_acct_info.extend(restapi.filter_acct_parquet_file(ins, acs, host, start_end_time))
                        result =  sorted(resp_acct_api) == sorted(parquet_acct_info)
                        log.info(f'Account details are {"same" if result else "not same"} '
                                 f'for host {host} between UI & parquet')
                    else:
                        # fetch account details from data-sync via queryWithDownload api.
                        result = process_account_details(test_config, host, resp_acct_api, timerange)
                    acct_results.update({host : result})
            else:
                log.error(f'No Host info for the incident: {incident_list}')
        else:
            log.info(f"Incident:{incident_list} in state: {ui_resp['state']}, fetching real time data ")
        results['acct_results'] = acct_results or {'no host': 'No resources available'}
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    assert_incidents_failed(all_results)


@mark.funct_impacted_account
def test_aiops_impacted_acc6_wrong_timestamp(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. check & extract host details of incident from UI using ui-api.
    3. if incident is closed/resolved then validate the historical data else real-time data need to be validated.
    4. fetch the host details from service now via snow-api.
    5. fetch the host details from slack war-room cos bucket.
    6. if war-room cos bucket not available, then fetch from slack archive cos bucket.
    7. compare the collected host details between ui and (snow + slack).(should be same)
    ### Validate account details ###
    8. iterate through the fetched host details.
    9. setting start_time as greatest and end_time as oldest.
    10. fetch account details from ui via count. (Expecting status code:500)
    """
    log.info(test_aiops_impacted_acc6_wrong_timestamp.__doc__)
    all_results = []
    for incident_list in test_config['var_incident_list']:
        results = {}
        acct_results = {}
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        log.info(f'VALIDATE HOST INFORMATIONS')
        # get incident details from UI API
        ui_resp = ui_resp_data[incident_list]['ui_data']
        ui_host_info = ui_resp_data[incident_list]['host_id']
        if ui_resp['state'] in ['resolved', 'closed']:
            log.info(f"Incident:{incident_list} in state: {ui_resp['state']}, fetching historical data ")
            # get incident details from SNOW API
            snow_resp = snow_resp_data[incident_list]['snow_data']
            snow_host_info = snow_resp_data[incident_list]['host_id']
            # get incident details from slack war room
            slack_host_info = restapi.load_slack_warroom_data(test_config, incident_list.lower())
            # compare the UI and (SNOW & SLACK) method
            result = restapi.compare_host_ui_snow(ui_host_info, snow_host_info, slack_host_info)
            results = {'incident' : incident_list, 'host_results': result}
            if ui_host_info:
                log.info(f'Found Host count: {len(ui_host_info)}, Host info: {ui_host_info}')
                log.info(f'VALIDATE ACCOUNT INFORMATIONS')
                # setting timerange between incident created time and additional 30 mins
                timerange = restapi.get_timerange(ui_resp['created'], 'wrong')
                for host in ui_host_info:
                    data = {'hostId': host, 'createdAtRange': timerange}
                    log.info(f'check account details with UI api for the Host: {host}')
                    # get the account details with ui api
                    aiops_api = restapi.RestApi(test_config['var_endpoint_ui'], 'impacted-accounts/count', data)
                    resp_acct_api = aiops_api.post()
                    acct_results.update({host : resp_acct_api['status']})
            else:
                log.error(f'No Host info for the incident: {incident_list}')
        else:
            log.info(f"Incident:{incident_list} in state: {ui_resp['state']}, fetching real time data ")
        results['acct_results'] = acct_results or {'no host': 'No resources available'}
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        if (incident['host_results'] is False or True in incident['acct_results'].values() or
                False in incident['acct_results'].values()):
            assert False, f'Not all incidents:{incident} returned excepted values'
