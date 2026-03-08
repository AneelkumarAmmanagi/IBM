import random
import glob
import logging as log
from pytest import mark
import aiops_restapi as restapi

def get_logs(config, ui_resp, st_time, ed_time,
             impact=None, state=None, dc=None, search=None, service=None):
    """ fetch the log from show change request api"""
    search_field = search if search else 'actual_end'
    region = ui_resp['location_identifier']['region'][0]
    dc_list = ui_resp['location_identifier']['dc']
    params = {'regions': region, 'tribe': ui_resp['tribe'],
              f'{search_field}': f'>={st_time}<={ed_time}',
               'customer_impact': impact, 'page':1,'page_size':50}
    if state:
        params['state'] = state
    if service:
        params['service_names'] = ui_resp['service_names'][0]
    if dc:
        params['dc'] = ','.join(dc_list)
    show_chang_req_resp = restapi.get_api_details(config['var_endpoint_ui'],'datasync', params)
    return show_chang_req_resp, [change['number'] for change in show_chang_req_resp]

def get_logs_without_incident(config, st_time=None, ed_time=None,
             impact=None, dc=None, search=None, service=None,
             region=None, tribe=None, state=None):
    """ fetch the log from show change request api"""
    search_field = search if search else 'actual_end'
    dc_list = restapi.create_incident_path([region])['dc']
    params = {'regions': region, 'tribe': tribe, 'service_names': service,
              f'{search_field}': f'>={st_time}<={ed_time}',
               'customer_impact': impact, 'page':1,'page_size':50}
    if dc:
        params['dc'] = ','.join(dc_list)
    if state:
        params['state'] = state
    show_chang_req_resp = restapi.get_api_details(config['var_endpoint_ui'],'datasync', params)
    return show_chang_req_resp, [change['number'] for change in show_chang_req_resp]


@mark.smoke
@mark.funct_change_request
@mark.parametrize('cr_state_flag', ["ON", "OFF"])
def test_aiops_change_request1_withfullimpact_withstate(test_config, ui_resp_data, cr_state_flag):
    """
    1. iterate through the provided incident list.
    2. get incident details from UI API.
    3. get cr entries with impact & state(on/off) via show change request api.
    4. get cr entries from cr parquet file.
    5. compare the received cr entries between api & parquet file.
    6. assert if no match request api.
    """
    log.info(test_aiops_change_request1_withfullimpact_withstate.__doc__)
    cr_state = ','.join(restapi.cr_state_list) if cr_state_flag == "ON" else None
    cr_impact = ','.join(restapi.cr_impact_list)
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        results = {'incident': incident_list}
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # 2. get incident details from UI API.
        ui_resp = ui_resp_data[incident_list]['ui_data']
        # 3. get cr entries with & without state via show change request api.
        start_time, end_time = restapi.get_custom_timerange(interval='48hours')
        change_resp, change_ids = get_logs(test_config, ui_resp, start_time, end_time,
                                           impact=cr_impact, state=cr_state)
        log.info(f"incident:{incident_list}, api_cr_id:{change_ids}")
        if change_ids:
            results['api_cr_id'] = True
            change_req_parquet = glob.glob('snow_changes_*.parquet')[0]
            # 4. get cr entries from cr parquet file.
            parquet_change_id = restapi.filter_cr_parquet_file(ui_resp, change_req_parquet,(start_time, end_time),
                                                               impact=cr_impact, state=cr_state)
            # 5. compare the received cr entries between api & parquet file.
            results['cr_validation'] = set(change_ids) == set(parquet_change_id)
        else:
            results['api_cr_id'] = 'no cr'
            results['cr_validation'] = 'no cr'
        log.info(f"incident:{incident_list}, cr_validation:{results['cr_validation']}")
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    # 6. assert if no match request api.
    result = all(incident['cr_validation'] for incident in all_results)
    assert result, f'change request validation with state_{cr_state_flag} flag is failed'


@mark.funct_change_request
@mark.parametrize('cr_dc_flag', ["ON", "OFF"])
def test_aiops_change_request2_withimpact_dc(test_config, ui_resp_data, cr_dc_flag):
    """
    1. iterate through the provided incident list.
    2. get incident details from UI API.
    3. get cr entries with impact & dc(on/off) via show change request api.
    4. get cr entries from cr parquet file.
    5. compare the received cr entries between api & parquet file.
    6. assert if no match request api.
    """
    log.info(test_aiops_change_request2_withimpact_dc.__doc__)
    dc = True if cr_dc_flag == "ON" else None
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        all_impact = []
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # 2. get incident details from UI API.
        ui_resp = ui_resp_data[incident_list]['ui_data']
        # 3. get cr entries with impact via show change request api.
        start_time, end_time = restapi.get_custom_timerange(interval='48hours')
        for impact in random.sample(restapi.cr_impact_list, 3):
            imp_results = {}
            change_resp, change_ids = get_logs(test_config, ui_resp, start_time, end_time, impact=impact, dc=dc)
            log.info(f"incident:{incident_list}, impact:{impact}, api_cr_id:{change_ids}")
            if change_ids:
                imp_results[f'api_cr_id_{impact}'] = True
                change_req_parquet = glob.glob('snow_changes_*.parquet')[0]
                # 4. get cr entries from cr parquet file.
                parquet_change_id = restapi.filter_cr_parquet_file(ui_resp, change_req_parquet,(start_time, end_time),
                                                               impact=impact, dc=dc)
                # 5. compare the received cr entries between api & parquet file.
                imp_results[f'cr_validation_{impact}'] = set(change_ids) == set(parquet_change_id)
            else:
                imp_results[f'api_cr_id_{impact}'] = 'no cr'
                imp_results[f'cr_validation_{impact}'] = 'no cr'
            all_impact.append(imp_results)
            log.info(f"incident:{incident_list}, impact_results:{all_impact}")
        all_results.append({'incident': incident_list, 'impact': all_impact})
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        # 6. assert if no match request api.
        result = all(all(imp_res.values()) for imp_res in incident['impact'])
        assert result, f'change request validation with impact and dc_{cr_dc_flag} flag is failed'


@mark.funct_change_request
@mark.parametrize('cr_dc_flag', ["ON", "OFF"])
def test_aiops_change_request3_withstate_dc(test_config, ui_resp_data, cr_dc_flag):
    """
    1. iterate through the provided incident list.
    2. get incident details from UI API.
    3. get cr entries with state & dc(on/off) via show change request api.
    4. get cr entries from cr parquet file.
    5. compare the received cr entries between api & parquet file.
    6. assert if no match request api.
    """
    log.info(test_aiops_change_request3_withstate_dc.__doc__)
    dc = True if cr_dc_flag == "ON" else None
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        all_state = []
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # 2. get incident details from UI API.
        ui_resp = ui_resp_data[incident_list]['ui_data']
        # get cr entries with state via show change request api.
        start_time, end_time = restapi.get_custom_timerange(interval='48hours')
        for state in random.sample(restapi.cr_state_list, 3):
            imp_results = {}
            change_resp, change_ids = get_logs(test_config, ui_resp, start_time, end_time,
                                               impact='no_impact', state=state, dc=dc)
            log.info(f"incident:{incident_list}, state:{state}, api_cr_id:{change_ids}")
            if change_ids:
                imp_results[f'api_cr_id_{state}'] = True
                change_req_parquet = glob.glob('snow_changes_*.parquet')[0]
                # 4. get cr entries from cr parquet file.
                parquet_change_id = restapi.filter_cr_parquet_file(ui_resp, change_req_parquet,(start_time, end_time),
                                                               impact='no_impact', state=state, dc=dc)
                # 5. compare the received cr entries between api & parquet file.
                imp_results[f'cr_validation_{state}'] = set(change_ids) == set(parquet_change_id)
            else:
                imp_results[f'api_cr_id_{state}'] = 'no cr'
                imp_results[f'cr_validation_{state}'] = 'no cr'
            all_state.append(imp_results)
            log.info(f"incident:{incident_list}, state_results:{all_state}")
        all_results.append({'incident': incident_list, 'state': all_state})
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        # 6. assert if no match request api.
        result = all(all(imp_res.values()) for imp_res in incident['state'])
        assert result, f'change request validation with state and dc_{cr_dc_flag} flag is failed'


@mark.funct_change_request
@mark.parametrize('cr_dc_flag', ["ON", "OFF"])
def test_aiops_change_request4_withsearch_dc(test_config, ui_resp_data, cr_dc_flag):
    """
    1. iterate through the provided incident list.
    2. get incident details from UI API.
    3. get cr entries with search & dc(on/off) via show change request api.
    4. get cr entries from cr parquet file.
    5. compare the received cr entries between api & parquet file.
    6. assert if no match request api.
    """
    log.info(test_aiops_change_request4_withsearch_dc.__doc__)
    dc = True if cr_dc_flag == "ON" else None
    cr_impact = ','.join(restapi.cr_impact_list)
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        all_search = []
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # 2. get incident details from UI API.
        ui_resp = ui_resp_data[incident_list]['ui_data']
        # 3. get cr entries with impact via show change request api.
        start_time, end_time = restapi.get_custom_timerange(interval='48hours')
        for search in random.sample(restapi.cr_search_list, 3):
            search_results = {}
            change_resp, change_ids = get_logs(test_config, ui_resp, start_time, end_time,
                                               impact=cr_impact, search=search, dc=dc)
            log.info(f"incident:{incident_list}, impact:{search}, api_cr_id:{change_ids}")
            if change_ids:
                search_results[f'api_cr_id_{search}'] = True
                change_req_parquet = glob.glob('snow_changes_*.parquet')[0]
                # 4. get cr entries from cr parquet file.
                parquet_change_id = restapi.filter_cr_parquet_file(ui_resp, change_req_parquet,(start_time, end_time),
                                                               impact=cr_impact, search=search, dc=dc)
                # 5. compare the received cr entries between api & parquet file.
                search_results[f'cr_validation_{search}'] = set(change_ids) == set(parquet_change_id)
            else:
                search_results[f'api_cr_id_{search}'] = 'no cr'
                search_results[f'cr_validation_{search}'] = 'no cr'
            all_search.append(search_results)
            log.info(f"incident:{incident_list}, search_results:{all_search}")
        all_results.append({'incident': incident_list, 'search': all_search})
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        # 6. assert if no match request api.
        result = all(all(imp_res.values()) for imp_res in incident['search'])
        assert result, f'change request validation with search and dc_{cr_dc_flag} flag is failed'


@mark.funct_change_request
@mark.parametrize('cr_dc_flag', ["ON", "OFF"])
def test_aiops_change_request5_withfullimpact_withservice_dc(test_config, ui_resp_data, cr_dc_flag):
    """
    1. iterate through the provided incident list.
    2. get incident details from UI API.
    3. get cr entries with impact, with service & dc(on/off) state via show change request api.
    4. get cr entries from cr parquet file.
    5. compare the received cr entries between api & parquet file.
    6. assert if no match request api.
    """
    log.info(test_aiops_change_request5_withfullimpact_withservice_dc.__doc__)
    cr_impact = ','.join(restapi.cr_impact_list)
    cr_dc = True if cr_dc_flag == "ON" else None
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        results = {'incident': incident_list}
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # 2. get incident details from UI API.
        ui_resp = ui_resp_data[incident_list]['ui_data']
        # 3. get cr entries with & without state via show change request api.
        start_time, end_time = restapi.get_custom_timerange(interval='48hours')
        change_resp, change_ids = get_logs(test_config, ui_resp, start_time, end_time,
                                           impact=cr_impact, dc=cr_dc, service=True)
        log.info(f"incident:{incident_list}, api_cr_id:{change_ids}")
        if change_ids:
            results['api_cr_id'] = True
            change_req_parquet = glob.glob('snow_changes_*.parquet')[0]
            # 4. get cr entries from cr parquet file.
            parquet_change_id = restapi.filter_cr_parquet_file(ui_resp, change_req_parquet,(start_time, end_time),
                                                               impact=cr_impact, dc=cr_dc, service=True)
            # 5. compare the received cr entries between api & parquet file.
            results['cr_validation'] = set(change_ids) == set(parquet_change_id)
        else:
            results['api_cr_id'] = 'no cr'
            results['cr_validation'] = 'no cr'
        log.info(f"incident:{incident_list}, cr_validation:{results['cr_validation']}")
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    # 6. assert if no match request api.
    result = all(incident['cr_validation'] for incident in all_results)
    assert result, f'change request validation with state_{cr_dc_flag} flag is failed'


@mark.funct_change_request
@mark.parametrize('cr_download_custom_flag', [False, True])
def test_aiops_change_request6_download_cr_custom(test_config, cr_download_custom_flag):
    """
    1. iterate through the provided incident list.
    2. download cr entries via change request api.
    3. assert if not downloaded the Excel
    """
    log.info(test_aiops_change_request6_download_cr_custom.__doc__)
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        results = {'incident': incident_list}
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # 2. download cr entries via change request api.
        for day in random.sample(range(7), 3):
            cr_response = False
            if cr_download_custom_flag:
                start_time, end_time = restapi.get_custom_timerange(interval='48hours')
                params = {'actual_start': f'>={start_time}<={end_time}'}
                restapi.get_api_details(test_config['var_endpoint_ui'],
                                                      f'changes/download', params)
                cr_response = True
            else:
                restapi.get_api_details(test_config['var_endpoint_ui'],
                                                      f'changes/download?days={day}')
                cr_response = True
            log.info(f"incident:{incident_list}, download_cr_status:{cr_response}")
            results['download_cr_status'] = cr_response
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    # 3. assert if not downloaded the Excel
    result = all(incident['download_cr_status'] for incident in all_results)
    assert result, f'download cr has failed'


@mark.funct_change_request
@mark.parametrize('cr_dc_flag', ["ON", "OFF"])
def test_aiops_change_request7_without_incident_withfullimpact_dc(test_config, cr_dc_flag):
    """
    1. iterate through the region.
    2. randomly choose one tribe & service from the given list.
    3. get cr entries with impact, tribe service & dc(on/off) state via show change request api.
    4. get cr entries from cr parquet file with the same payload.
    5. compare the received cr entries between api & parquet file.
    6. assert if no match request api.
    """
    log.info(test_aiops_change_request7_without_incident_withfullimpact_dc.__doc__)
    cr_impact = ','.join(restapi.cr_impact_list)
    cr_dc = True if cr_dc_flag == "ON" else None
    all_results = []
    # 1. iterate through the region.
    for region in random.sample(restapi.region_list, 5):
        results = {'region': region}
        log.info(f'REGION: #####  {region}  #####')
        # 2. randomly choose one tribe & service from the given list.
        tribe = random.sample(list(test_config['tribe_service_mapping'].keys()), 1)[0]
        service = test_config['tribe_service_mapping'][tribe][0]
        # 3. get cr entries with impact, tribe service & dc(on/off) state via show change request api.
        start_time, end_time = restapi.get_custom_timerange(interval='48hours')
        change_resp, change_ids = get_logs_without_incident(test_config, start_time, end_time, impact=cr_impact,
                                           region=region, tribe=tribe, service=service, dc=cr_dc)
        log.info(f"region:{region}, api_cr_id:{change_ids}")
        if change_ids:
            results['api_cr_id'] = True
            change_req_parquet = glob.glob('snow_changes_*.parquet')[0]
            # 4. get cr entries from cr parquet file with the same payload.
            parquet_change_id = restapi.filter_cr_parquet_file_without_incident(
                change_req_parquet,(start_time, end_time),
                region=region, impact=cr_impact, dc=cr_dc, tribe=tribe, service=service)
            # 5. compare the received cr entries between api & parquet file.
            results['cr_validation'] = set(change_ids) == set(parquet_change_id)
        else:
            results['api_cr_id'] = 'no cr'
            results['cr_validation'] = 'no cr'
        log.info(f"incident:{region}, cr_validation:{results['cr_validation']}")
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    # 6. assert if no match request api.
    result = all(incident['cr_validation'] for incident in all_results)
    assert result, f'change request validation with state_{cr_dc_flag} flag is failed'


@mark.funct_change_request
@mark.parametrize('cr_dc_flag', ["ON", "OFF"])
def test_aiops_change_request8_without_incident_withfullimpact_withfullstate_dc(test_config, cr_dc_flag):
    """
    1. iterate through the region.
    2. randomly choose one tribe & service from the given list.
    3. get cr entries with impact, tribe service & dc(on/off) state via show change request api.
    4. get cr entries from cr parquet file with the same payload.
    5. compare the received cr entries between api & parquet file.
    6. assert if no match request api.
    """
    log.info(test_aiops_change_request8_without_incident_withfullimpact_withfullstate_dc.__doc__)
    cr_impact = ','.join(restapi.cr_impact_list)
    cr_dc = True if cr_dc_flag == "ON" else None
    cr_state = ','.join(restapi.cr_state_list)
    all_results = []
    # 1. iterate through the region.
    for region in random.sample(restapi.region_list, 5):
        results = {'region': region}
        log.info(f'REGION: #####  {region}  #####')
        # 2. randomly choose one tribe & service from the given list.
        tribe = random.sample(list(test_config['tribe_service_mapping'].keys()), 1)[0]
        service = test_config['tribe_service_mapping'][tribe][0]
        # 3. get cr entries with impact, tribe service & dc(on/off) state via show change request api.
        start_time, end_time = restapi.get_custom_timerange(interval='48hours')
        change_resp, change_ids = get_logs_without_incident(test_config, start_time, end_time, impact=cr_impact,
                                           region=region, tribe=tribe, service=service, dc=cr_dc, state=cr_state)
        log.info(f"region:{region}, api_cr_id:{change_ids}")
        if change_ids:
            results['api_cr_id'] = True
            change_req_parquet = glob.glob('snow_changes_*.parquet')[0]
            # 4. get cr entries from cr parquet file with the same payload.
            parquet_change_id = restapi.filter_cr_parquet_file_without_incident(
                change_req_parquet,(start_time, end_time),
                region=region, impact=cr_impact, dc=cr_dc, tribe=tribe, service=service, state=cr_state)
            # 5. compare the received cr entries between api & parquet file.
            results['cr_validation'] = set(change_ids) == set(parquet_change_id)
        else:
            results['api_cr_id'] = 'no cr'
            results['cr_validation'] = 'no cr'
        log.info(f"incident:{region}, cr_validation:{results['cr_validation']}")
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    # 6. assert if no match request api.
    result = all(incident['cr_validation'] for incident in all_results)
    assert result, f'change request validation with state_{cr_dc_flag} flag is failed'
