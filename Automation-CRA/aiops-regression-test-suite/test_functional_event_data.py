import random
import math
import logging as log
from pytest import mark
import aiops_restapi as restapi

def get_logs(config, ui_resp, snow_resp, ui_uuid, log_type=None):
    """ fetch the failure event"""
    full_list = {}
    start_time, end_time = restapi.get_custom_timerange(ui_resp['created'], interval='48hours')
    region_list = snow_resp['location_identifier']['region']
    mzone_list = snow_resp['location_identifier']['mzone']
    for uuid in ui_uuid:
        event_uuid_list = []
        payload = {'region': region_list,
                   'start_time': start_time, 'end_time': end_time,
                   'env': mzone_list, 'resource_ids': [uuid],
                   'options': {'page':1,'limit':20}}
        show_log_resp = restapi.get_fewer_incident_info(config['var_event_data_ui'],
                                                        f'{log_type}', payload)
        if show_log_resp['pagination']['totalPages'] > 0:
            event_uuid_list.extend(event['resource_id'] for event in show_log_resp['data'])
            total_pages = math.ceil(show_log_resp['pagination']['totalPages'] / 20)
            for page in range(2, total_pages + 1):
                payload['options']['page'] = page
                show_log_resp = restapi.get_fewer_incident_info(config['var_event_data_ui'],
                                                                f'{log_type}', payload)
                for event in show_log_resp['data']:
                    event_uuid_list.append(event['resource_id'])
        else:
            event_uuid_list = "no log entries"
        full_list[uuid] = event_uuid_list
    return full_list

def get_logs_withoutincident(config, region, ui_uuid=None, log_type=None):
    """ fetch the events without incident"""
    start_time, end_time = restapi.get_custom_timerange(interval='48hours')
    mzone_list = restapi.create_incident_path(region.split())['mzone']
    payload = {'region': region.split(),
               'start_time': start_time, 'end_time': end_time,
               'env': mzone_list, 'resource_ids': ui_uuid.split() if ui_uuid else [],
               'options': {'page':1,'limit':20}}
    event_uuid_list = []
    event_uuid_status = {}
    show_log_resp = restapi.get_fewer_incident_info(config['var_event_data_ui'],
                                                    f'{log_type}', payload)
    api_status = True if show_log_resp.get('status', None) == 'success' else False
    if show_log_resp['pagination']['totalPages'] > 0:
        event_uuid_list.extend(event['resource_id'] for event in show_log_resp['data'])
        if ui_uuid:
            for event in show_log_resp['data']:
                event_uuid_status[event['resource_id']] = (ui_uuid == event['resource_id'])
        total_pages = math.ceil(show_log_resp['pagination']['totalPages'] / 20)
        for page in range(2, total_pages + 1):
            payload['options']['page'] = page
            show_log_resp = restapi.get_fewer_incident_info(config['var_event_data_ui'],
                                                            f'{log_type}', payload)
            event_uuid_list.extend(event['resource_id'] for event in show_log_resp['data'])
            if ui_uuid:
                for event in show_log_resp['data']:
                    event_uuid_status[event['resource_id']] = (ui_uuid == event['resource_id'])
    return api_status, event_uuid_list, event_uuid_status

def get_lifecycle_event(config, uuid, log_type=None):
    """ fetch the lifecycle events"""
    payload = {'resource_id': uuid}
    show_log_resp = restapi.get_fewer_incident_info(config['var_event_data_ui'],
                                                f'{log_type}', payload)
    api_status = True if show_log_resp.get('resource_id', None) == uuid else False
    return api_status, show_log_resp

def analyze_lifecycle_event(data, uuid):
    """ analyze the lifecycle events"""
    result = {'uuid_status': False, 'failed_status': {'event_id': [], 'event_cnt':0},
              'success_status': {'event_id':[], 'event_cnt':0}}
    if data['resource_id'] == uuid:
        result['uuid'] = True
    if len(data['failed_events']) > 0:
        result['failed_status']['event_cnt'] = len(data['failed_events'])
        fail_event_log = []
        for fail_event in data['failed_events']:
            fail_event_log.append({fail_event['context_eventid']: fail_event['context_eventtype']})
        result['failed_status']['event_id'] = fail_event_log
    if len(data['lifecycle_events']) > 0:
        result['success_status']['event_cnt'] = len(data['lifecycle_events'])
        success_event_log = []
        for fail_event in data['lifecycle_events']:
            success_event_log.append({fail_event['context_eventid']: fail_event['context_eventtype']})
        result['success_status']['event_id'] = success_event_log
    return result


@mark.smoke
@mark.funct_event_data
def test_aiops_event_data1_failure_event(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. get incident details from UI API.
    3. get incident details from SNOW API.
    4. compare ui & snow details.
    5. get log entries via failure-events api.
    6. assert if no uuid details or failure log entries.
    """
    log.info(test_aiops_event_data1_failure_event.__doc__)
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # 2. get incident details from UI API.
        ui_resp = ui_resp_data[incident_list]['ui_data']
        ui_uuid_info = ui_resp_data[incident_list]['uuid_id']
        # 3. get incident details from SNOW API.
        snow_resp = snow_resp_data[incident_list]['snow_data']
        snow_uuid_info = snow_resp_data[incident_list]['uuid_id']
        slack_uuid_info = restapi.load_slack_warroom_data(test_config, incident_list.lower(), 'uuid')
        # 4. compare ui & snow details.
        result = restapi.compare_host_ui_snow(ui_uuid_info, snow_uuid_info, slack_uuid_info)
        if ui_uuid_info:
            failure_event_resp = get_logs(test_config, ui_resp, snow_resp, ui_uuid_info, 'failure-events')
        else:
            failure_event_resp = "no uuid found"
        results = {'incident' : incident_list, 'uuid_results':result, 'failure-events': failure_event_resp}
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        if incident['uuid_results'] is False:
            # 6. assert if no uuid details or failure log entries.
            assert False, f'Not all incidents:{incident} returned excepted values'


@mark.funct_event_data
def test_aiops_event_data2_stuck_provisioning(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. get incident details from UI API.
    3. get incident details from SNOW API.
    4. compare ui & snow details.
    5. get log entries via stuck-provisioning api.
    6. assert if no uuid details or failure log entries.
    """
    log.info(test_aiops_event_data2_stuck_provisioning.__doc__)
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # 2. get incident details from UI API.
        ui_resp = ui_resp_data[incident_list]['ui_data']
        ui_uuid_info = ui_resp_data[incident_list]['uuid_id']
        # 3. get incident details from SNOW API.
        snow_resp = snow_resp_data[incident_list]['snow_data']
        snow_uuid_info = snow_resp_data[incident_list]['uuid_id']
        slack_uuid_info = restapi.load_slack_warroom_data(test_config, incident_list.lower(), 'uuid')
        # 4. compare ui & snow details.
        result = restapi.compare_host_ui_snow(ui_uuid_info, snow_uuid_info, slack_uuid_info)
        if ui_uuid_info:
            # 5. get log entries via stuck-provisioning api.
            failure_event_resp = get_logs(test_config, ui_resp, snow_resp, ui_uuid_info, 'stuck-provisioning')
        else:
            failure_event_resp = "no uuid found"
        results = {'incident' : incident_list, 'uuid_results':result, 'failure-events': failure_event_resp}
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        if incident['uuid_results'] is False:
            # 6. assert if no uuid details or failure log entries.
            assert False, f'Not all incidents:{incident} returned excepted values'


@mark.funct_event_data
def test_aiops_event_data3_failure_event_withoutincident(test_config):
    """
    1. iterate through the region.
    2. get log entries via failure-event api.
    3. assert if any region not returned any response.
    """
    log.info(test_aiops_event_data3_failure_event_withoutincident.__doc__)
    all_results = []
    # 1. iterate through the region.
    for region in random.sample(restapi.region_list, 3):
        log.info(f'checking: #####  {region}  #####')
        # 2. get log entries via failure-event api.
        fe_status, uuid_list, uuid_status = get_logs_withoutincident(test_config, region, log_type='failure-events')
        log.info(f'region:{region}, fe_api_status:{fe_status}, fe_event_cnt:{len(uuid_list)}')
        results = {'region': region, 'fe_api_status': fe_status}
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    for event in all_results:
        if event['fe_api_status'] is False:
            # 3. assert if any region not returned any response.
            assert False, f"failure-events api failed for region:{event['region']}"


@mark.funct_event_data
def test_aiops_event_data4_stuck_provisioning_withoutincident(test_config):
    """
    1. iterate through the region.
    2. get log entries via stuck-provisioning api.
    3. assert if any region not returned any response.
    """
    log.info(test_aiops_event_data4_stuck_provisioning_withoutincident.__doc__)
    all_results = []
    # 1. iterate through the region.
    for region in random.sample(restapi.region_list, 3):
        log.info(f'checking: #####  {region}  #####')
        # 2. get log entries via stuck-provisioning api.
        sp_status, uuid_list, uuid_status = get_logs_withoutincident(test_config, region, log_type='stuck-provisioning')
        log.info(f'region:{region}, sp_api_status:{sp_status}, sp_event_cnt:{len(uuid_list)}')
        results = {'region': region, 'sp_api_status': sp_status}
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    for event in all_results:
        if event['sp_api_status'] is False:
            # 3. assert if any region not returned any response.
            assert False, f"stuck prov api failed for region:{event['region']}"


@mark.funct_event_data
def test_aiops_event_data5_failure_event_apply_filter_withoutincident(test_config):
    """
    1. iterate through the region.
    2. get log entries via failure-event api.
    3. apply resource id filter and get log entries via failure-event api.
    4. check filter functionality and assert if didn't find any match.
    """
    log.info(test_aiops_event_data5_failure_event_apply_filter_withoutincident.__doc__)
    all_results = []
    # 1. iterate through the region.
    for region in random.sample(restapi.region_list, 3):
        uuid_status_all_region = []
        log.info(f'checking: #####  {region}  #####')
        # 2. get log entries via failure-event api.
        fe_status1, uuid_list1, uuid_status1 = get_logs_withoutincident(test_config, region, log_type='failure-events')
        if uuid_list1:
            for uuid in random.sample(uuid_list1, 1 if len(uuid_list1) < 2 else 2):
                # 3. apply resource id filter and get log entries via failure-event api.
                fe_status2, uuid_list2, uuid_status2 = get_logs_withoutincident(test_config, region, ui_uuid=uuid, log_type='failure-events')
                uuid_status_all_region.append(uuid_status2)
        log.info(f'region:{region}, failure events:{len(uuid_status_all_region)}')
        results = {'region': region, 'failure_event_filter_status': all(uuid_status_all_region)}
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    for event in all_results:
        if event['failure_event_filter_status'] is False:
            # 4. check filter functionality and assert if didn't find any match.
            assert False, f"Not received any log entries for region:{event['region']}"


@mark.funct_event_data
def test_aiops_event_data6_stuck_provisioning_apply_filter_withoutincident(test_config):
    """
    1. iterate through the region.
    2. get log entries via stuck-provisioning api.
    3. apply resource id filter and get log entries via stuck-provisioning api.
    4. check filter functionality and assert if didn't find any match.
    """
    log.info(test_aiops_event_data6_stuck_provisioning_apply_filter_withoutincident.__doc__)
    all_results = []
    # 1. iterate through the region.
    for region in random.sample(restapi.region_list, 3):
        uuid_status_all_region = []
        log.info(f'checking: #####  {region}  #####')
        # 2. get log entries via stuck-provisioning api.
        sp_status1, uuid_list1, uuid_status1 = get_logs_withoutincident(test_config, region, log_type='stuck-provisioning')
        if uuid_list1:
            for uuid in uuid_list1:
                # 3. apply resource id filter and get log entries via stuck-provisioning api.
                sp_status2, uuid_list2, uuid_status2 = get_logs_withoutincident(test_config, region,
                                                                    ui_uuid=uuid, log_type='stuck-provisioning')
                uuid_status_all_region.append(uuid_status2)
        log.info(f'region:{region}, stuck prov events:{len(uuid_status_all_region)}')
        results = {'region': region, 'stuck_prov_filter_status': all(uuid_status_all_region)}
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    # 4. check filter functionality and assert if didn't find any match.
    assert any(result['stuck_prov_filter_status'] for result in
               all_results), f"Atleast one region should be True {all_results}"


@mark.funct_event_data
def test_aiops_event_data7_lifecycle_event_withoutincident(test_config):
    """
    1. iterate through the region.
    2. get log entries via failure-event api.
    3. get lifecycle log entries with resource uuid.
    4. analyse lifecycle log entries.
    5. assert if returned no response.
    """
    log.info(test_aiops_event_data7_lifecycle_event_withoutincident.__doc__)
    all_results = []
    # 1. iterate through the region.
    for region in random.sample(restapi.region_list, 3):
        fe_resp = []
        lc_status_resp = []
        lc_analysis_resp = []
        log.info(f'checking: #####  {region}  #####')
        # 2. get log entries via failure-event api.
        fe_status, uuid_list, uuid_status = get_logs_withoutincident(test_config, region, log_type='failure-events')
        log.info(f'region:{region}, failure events:{len(uuid_list)}')
        if not uuid_list:
            fe_resp.append("no log")
            lc_analysis_resp.append("no log")
        else:
            for uuid in random.sample(uuid_list, 1 if len(uuid_list) < 2 else 2):
                # 3. get lifecycle log entries with resource uuid.
                le_status, lifecycle_resp = get_lifecycle_event(test_config, uuid, log_type='lifecycle-events')
                lc_status_resp.append(le_status)
                log.info(f'region:{region},'
                    f"Failed_events:{len(lifecycle_resp['failed_events'])},"
                    f"Success_events:{len(lifecycle_resp['lifecycle_events'])}")
                if len(lifecycle_resp['failed_events']) > 0 or len(lifecycle_resp['lifecycle_events']) > 0:
                    fe_resp.append(True)
                    # 4. analyse lifecycle log entries.
                    lifecycle_analysis_resp =  analyze_lifecycle_event(lifecycle_resp, uuid)
                    lc_analysis_resp.append(lifecycle_analysis_resp['uuid'])
                    log.info(f'region:{region}, lifecycle resp analysis:{lifecycle_analysis_resp}')
                else:
                    lc_analysis_resp.append(False)
        results = {'region': region, 'le_resp': all(lc_status_resp), 'lc_analysis_resp': all(lc_analysis_resp)}
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    for event in all_results:
        if event['le_resp'] is False:
            # 5. assert if returned no response.
            assert False, f"lifecycle-event api failed for region:{event['region']}"
