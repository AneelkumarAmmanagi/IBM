import json
import logging as log
from pytest import mark
import aiops_restapi as restapi

fetch_log_response = {}
analyse_log_response = {}
fetch_log_count = {}
partial_fetch_log_count = {}
analyse_log_count = {}

def get_logs(ui_resp, snow_resp, ui_uuid, config, log_type=None):
    """ fetch/analyze the log for the provided resource id"""
    timestamp = ui_resp.get('outage_start', ui_resp.get('created'))
    region_list = snow_resp['location_identifier']['region'][0]
    start_time, end_time = restapi.get_custom_timerange(timestamp, interval='15mins')
    payload = {'ref_id': ','.join(ui_uuid), 'region': region_list,
               'start_date': start_time, 'end_date': end_time}
    payload['inc_summary'] = ui_resp['short_description']
    show_log_resp = restapi.get_fewer_incident_info(config['var_endpoint_ui'],
                                                    f'log_analyzer/{log_type}', payload)
    return show_log_resp

def validate_get_logs(show_log, ui_uuid, incident):
    """ validate the fetch/analyse log for the provided resource id"""
    if show_log.get('error') == 'Failed to fetch logs' or show_log.get('error') == 'Failed to analyse logs':
        log.error(f"incident:{incident}, log: {show_log.get('error')}")
        return False
    if show_log['code'] == 200 and len(show_log['results']) >= 1:
        log.info(f'found:{len(show_log["results"])} fetch_log/analyse_log entries for the incident:{incident}')
        uuid_result2 = []
        for log_entry in show_log['results']:
            for uuid in ui_uuid:
                if not uuid in log_entry['error_log']:
                    log.error(f'uuid:{uuid} not found in the fetch/analyse log entry')
                    uuid_result2.append(False)
                else:
                    log.info(f'uuid:{uuid} found in the fetch/analyse log entry')
                    uuid_result2.append(True)
        return all(uuid_result2)
    else:
        log.warning(f'fetch/analyse api returned no log for the incident:{incident}')
        return True

@mark.smoke
@mark.funct_incident_log
def test_aiops_incident_log1_validate_resources(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. check & extract uuid details of incident from UI using ui-api.
    3. fetch the uuid details from service now via snow-api.
    4. fetch the uuid details from slack war-room cos bucket/ slack archive cos bucket.
    5. compare the collected uuid details between ui and (snow + slack).
    """
    log.info(test_aiops_incident_log1_validate_resources.__doc__)
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        log.info(f'VALIDATE UUID INFORMATIONS')
        # 2. check & extract uuid details of incident from UI using ui-api.
        ui_resp = ui_resp_data[incident_list]['ui_data']
        ui_uuid_info = ui_resp_data[incident_list]['uuid_id']
        log.info(f"Incident:{incident_list} in state: {ui_resp['state']}, fetching historical data ")
        # 3. fetch the uuid details from service now via snow-api.
        snow_resp = snow_resp_data[incident_list]['snow_data']
        snow_uuid_info = snow_resp_data[incident_list]['uuid_id']
        # 4. fetch the uuid details from slack war-room cos bucket/ slack archive cos bucket.
        slack_uuid_info = restapi.load_slack_warroom_data(test_config, incident_list.lower(), 'uuid')
        # 5. compare the collected uuid details between ui and (snow + slack).
        result = restapi.compare_host_ui_snow(ui_uuid_info, snow_uuid_info, slack_uuid_info)
        results = {'incident' : incident_list, 'uuid_results': result}
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        if incident['uuid_results'] is False:
            assert False, f'Not all incidents:{incident} returned excepted values'


@mark.smoke
@mark.funct_incident_log
def test_aiops_incident_log2_fetch_log(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. check & extract uuid details of incident from UI using ui-api.
    3. fetch the uuid details from service now via snow-api.
    4. fetch the uuid details from slack war-room cos bucket/ slack archive cos bucket.
    5. compare the collected uuid details between ui and (snow + slack).
    6. get the fetch_log with the uuid.
    7. check uuid exists in all fetch_log entries
    """
    log.info(test_aiops_incident_log2_fetch_log.__doc__)
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        log.info(f'VALIDATE UUID')
        # 2. check & extract uuid details of incident from UI using ui-api.
        ui_resp = ui_resp_data[incident_list]['ui_data']
        ui_uuid_info = ui_resp_data[incident_list]['uuid_id']
        log.info(f"Incident:{incident_list} in state: {ui_resp['state']}, fetching historical data ")
        # 3. fetch the uuid details from service now via snow-api.
        snow_resp = snow_resp_data[incident_list]['snow_data']
        snow_uuid_info = snow_resp_data[incident_list]['uuid_id']
        # 4. fetch the uuid details from slack war-room cos bucket/ slack archive cos bucket.
        slack_uuid_info = restapi.load_slack_warroom_data(test_config, incident_list.lower(), 'uuid')
        # 5. compare the collected uuid details between ui and (snow + slack).
        result1 = restapi.compare_host_ui_snow(ui_uuid_info, snow_uuid_info, slack_uuid_info)
        result2 = "No resources available"
        show_log_resp = {'results': []}
        if ui_uuid_info:
            # 6. get the fetch_log with the uuid.
            show_log_resp = get_logs(ui_resp, snow_resp, ui_uuid_info, test_config, 'fetch_log')
            # 7. check uuid exists in all fetch_log entries
            result2 = validate_get_logs(show_log_resp, ui_uuid_info, incident_list)
            if show_log_resp.get('error', None) ==  'Failed to fetch logs':
                show_log_resp = {'results': []}
        results = {'incident' : incident_list, 'uuid_results': result1, 'fetch_log_results': result2}
        fetch_log_count.update({incident_list: len(show_log_resp['results'])})
        fetch_log_response.update({incident_list: show_log_resp['results']})
        all_results.append(results)
    log.info(f'Overall fetch log summary: {fetch_log_count}')
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        if incident['uuid_results'] is False or incident['fetch_log_results'] is False:
            assert False, f'Not all incidents:{incident} returned excepted values'


@mark.funct_incident_log
def test_aiops_incident_log3_analyse_log(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. check & extract uuid details of incident from UI using ui-api.
    3. fetch the uuid details from service now via snow-api.
    4. fetch the uuid details from slack war-room cos bucket/ slack archive cos bucket.
    5. compare the collected uuid details between ui and (snow + slack).
    6. get the analyse_log with the uuid.
    7. check uuid exists in all analyse_log entries
    """
    log.info(test_aiops_incident_log3_analyse_log.__doc__)
    log.info(f'fetch log summary from previous testcase: {fetch_log_count}')
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        log.info(f'VALIDATE UUID')
        # 2. check & extract uuid details of incident from UI using ui-api.
        ui_resp = ui_resp_data[incident_list]['ui_data']
        ui_uuid_info = ui_resp_data[incident_list]['uuid_id']
        log.info(f"Incident:{incident_list} in state: {ui_resp['state']}, fetching historical data ")
        # 3. fetch the uuid details from service now via snow-api.
        snow_resp = snow_resp_data[incident_list]['snow_data']
        snow_uuid_info = snow_resp_data[incident_list]['uuid_id']
        # 4. fetch the uuid details from slack war-room cos bucket/ slack archive cos bucket.
        slack_uuid_info = restapi.load_slack_warroom_data(test_config, incident_list.lower(), 'uuid')
        # 5. compare the collected uuid details between ui and (snow + slack).
        result1 = restapi.compare_host_ui_snow(ui_uuid_info, snow_uuid_info, slack_uuid_info)
        result2 = "No resources available"
        analyse_log_resp = {'results': []}
        if ui_uuid_info:
            # 6. get the analyse_log with the uuid.
            analyse_log_resp = get_logs(ui_resp, snow_resp, ui_uuid_info, test_config, 'analyse_log')
            # 7. check uuid exists in all analyse_log entries
            result2 = validate_get_logs(analyse_log_resp, ui_uuid_info, incident_list)
            if analyse_log_resp.get('error', None) == 'Failed to analyse logs':
                analyse_log_resp = {'results': []}
        results = {'incident' : incident_list, 'uuid_results': result1, 'analyse_log_results': result2}
        analyse_log_count.update({incident_list: len(analyse_log_resp['results'])})
        analyse_log_response.update({incident_list: analyse_log_resp['results']})
        all_results.append(results)
    log.info(f'Overall analyse log summary: {analyse_log_count}')
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        if incident['uuid_results'] is False or incident['analyse_log_results'] is False:
            assert False, f'Not all incidents:{incident} returned excepted values'


@mark.funct_incident_log
def test_aiops_incident_log4_compare_logs(test_config, ui_resp_data, snow_resp_data):
    """
    1. compare fetch and analyse logs results
    2. assert if analyse log entries greater than fetch log entries.
    3. error_log of analyse logs should be sub-set of fetch log.
    """
    log.info(test_aiops_incident_log4_compare_logs.__doc__)
    if not (fetch_log_count and analyse_log_count and fetch_log_response and analyse_log_response):
        log.error('variable details not available. Run test_aiops_incident_log2_fetch_log'
                  'and test_aiops_incident_log3_analyse_log before this testcase')
        assert False
    log.info(f'fetch log summary from previous testcase: {fetch_log_count}')
    log.info(f'analyse log summary from previous testcase: {analyse_log_count}')
    all_results = []
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        result2 = []
        if analyse_log_count[incident_list] <= fetch_log_count[incident_list]:
            log.info(f'analyse log count is lesser or equal than fetch log for: {incident_list} as expected')
            result1 =  True
        else:
            log.error(f'analyse log count is not lesser or equal than fetch log for: {incident_list}')
            result1 = False
        for analyse_log in analyse_log_response[incident_list]:
            analyse_log = json.loads(analyse_log.get('error_log', '{}'))
            for fetch_log in fetch_log_response[incident_list]:
                fetch_log = json.loads(fetch_log.get('error_log', '{}'))
                if (analyse_log.get('MESSAGE', '').replace('\\n', '\n') == fetch_log.get('MESSAGE', '') and
                    analyse_log.get('error', '').replace('\\n', '\n') == fetch_log.get('error', '') and
                    analyse_log.get('caller', '').replace('\\n', '\n') == fetch_log.get('caller', '') and
                    analyse_log.get('CORRID', '').replace('\\n', '\n') == fetch_log.get('CORRID', '') and
                    analyse_log.get('ACCOUNTID', '').replace('\\n', '\n') == fetch_log.get('ACCOUNTID', '')):
                    result2.append(True)
                    break
            else:
                result2.append(False)
        if not all(result2):
            log.error(f'fetch/analyse log details are not matching for incident: {incident_list}, results: {result2}')
        else:
            log.info(f'fetch/analyse log details are matching for incident: {incident_list}, results: {result2}')
        results = {'incident': incident_list, 'cnt_results': result1, 'subset_results': all(result2)}
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        if incident['cnt_results'] is False or incident['subset_results'] is False:
            assert False, f'Not all incidents:{incident} returned excepted values'


@mark.funct_incident_log
def test_aiops_incident_log5_fetch_partial_uuid(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. check & extract uuid details of incident from UI using ui-api.
    3. fetch the uuid details from service now via snow-api.
    4. fetch the uuid details from slack war-room cos bucket/ slack archive cos bucket.
    5. compare the collected uuid details between ui and (snow + slack).
    6. get the fetch_log with partial uuid.
    7. check the partial uuid exists in all analyse_log entries
    Need improvement
    """
    log.info(test_aiops_incident_log5_fetch_partial_uuid.__doc__)
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        log.info(f'VALIDATE UUID')
        # 2. check & extract uuid details of incident from UI using ui-api.
        ui_resp = ui_resp_data[incident_list]['ui_data']
        ui_uuid_info = ui_resp_data[incident_list]['uuid_id']
        log.info(f"Incident:{incident_list} in state: {ui_resp['state']}, fetching historical data ")
        # 3. fetch the uuid details from service now via snow-api.
        snow_resp = snow_resp_data[incident_list]['snow_data']
        snow_uuid_info = snow_resp_data[incident_list]['uuid_id']
        # 4. fetch the uuid details from slack war-room cos bucket/ slack archive cos bucket.
        slack_uuid_info = restapi.load_slack_warroom_data(test_config, incident_list.lower(), 'uuid')
        # 5. compare the collected uuid details between ui and (snow + slack).
        result1 = restapi.compare_host_ui_snow(ui_uuid_info, snow_uuid_info, slack_uuid_info)
        result2 = "No resources available"
        show_log_resp = {'results': []}
        if ui_uuid_info:
            ui_uuid_info = [ui_uuid.split('-')[0] for ui_uuid in ui_uuid_info]
            log.info(f'validate fetch_log with partial uuid:{ui_uuid_info}')
            # 6. get the analyse_log with partial uuid.
            show_log_resp = get_logs(ui_resp, snow_resp, ui_uuid_info, test_config, 'fetch_log')
            # 7. check the partial uuid exists in all analyse_log entries
            result2 = validate_get_logs(show_log_resp, ui_uuid_info, incident_list)
            if show_log_resp.get('error', None) ==  'Failed to fetch logs':
                show_log_resp = {'results': []}
        results = {'incident' : incident_list, 'uuid_results': result1, 'fetch_log_results': result2}
        partial_fetch_log_count.update({incident_list: len(show_log_resp['results'])})
        fetch_log_response.update({incident_list: show_log_resp['results']})
        all_results.append(results)
    log.info(f'Overall fetch log summary: {partial_fetch_log_count}')
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        if incident['uuid_results'] is False or incident['fetch_log_results'] is False:
            assert False, f'Not all incidents:{incident} returned excepted values'


@mark.skip(reason="As there is dependency with runbooks module, skipping the testcase")
@mark.funct_incident_log
def test_aiops_incident_log6_analyse_log_partial(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. check & extract uuid details of incident from UI using ui-api.
    3. fetch the uuid details from service now via snow-api.
    4. fetch the uuid details from slack war-room cos bucket/ slack archive cos bucket.
    5. compare the collected uuid details between ui and (snow + slack).
    6. get the analyse_log with partial uuid.
    7. check the partial uuid exists in all analyse_log entries
    Need improvement
    """
    log.info(test_aiops_incident_log6_analyse_log_partial.__doc__)
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        log.info(f'VALIDATE UUID')
        # 2. check & extract uuid details of incident from UI using ui-api.
        ui_resp = ui_resp_data[incident_list]['ui_data']
        ui_uuid_info = ui_resp_data[incident_list]['uuid_id']
        log.info(f"Incident:{incident_list} in state: {ui_resp['state']}, fetching historical data ")
        # 3. fetch the uuid details from service now via snow-api.
        snow_resp = snow_resp_data[incident_list]['snow_data']
        snow_uuid_info = snow_resp_data[incident_list]['uuid_id']
        # 4. fetch the uuid details from slack war-room cos bucket/ slack archive cos bucket.
        slack_uuid_info = restapi.load_slack_warroom_data(test_config, incident_list.lower(), 'uuid')
        # 5. compare the collected uuid details between ui and (snow + slack).
        result1 = restapi.compare_host_ui_snow(ui_uuid_info, snow_uuid_info, slack_uuid_info)
        result2 = "No resources available"
        analyse_log_resp = {'results': []}
        if ui_uuid_info:
            ui_uuid_info = [ui_uuid.split('-')[0] for ui_uuid in ui_uuid_info]
            # 6. get the analyse_log with partial uuid.
            analyse_log_resp = get_logs(ui_resp, snow_resp, ui_uuid_info, test_config, 'analyse_log')
            # 7. check uuid exists in all analyse_log entries
            result2 = validate_get_logs(analyse_log_resp, ui_uuid_info, incident_list)
            if analyse_log_resp.get('error', None) == 'Failed to analyse logs':
                analyse_log_resp = {'results': []}
        results = {'incident' : incident_list, 'uuid_results': result1, 'analyse_log_results': result2}
        analyse_log_count.update({incident_list: len(analyse_log_resp['results'])})
        analyse_log_response.update({incident_list: analyse_log_resp['results']})
        all_results.append(results)
    log.info(f'Overall analyse log summary: {analyse_log_count}')
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        if incident['uuid_results'] is False or incident['analyse_log_results'] is False:
            assert False, f'Not all incidents:{incident} returned excepted values'
