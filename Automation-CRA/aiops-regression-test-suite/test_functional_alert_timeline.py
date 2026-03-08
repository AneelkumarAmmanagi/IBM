import random
import glob
import logging as log
from pytest import mark
import aiops_restapi as restapi

def build_alert_timeline_query(ui_resp=None, service=None, region=None, dc=None, timeline=None):
    """Build the query string and payload for alert timeline"""
    # Assume inputs are lists in this test module
    if ui_resp:
        service_names = ui_resp['service_names'][0] if service else None
        region_name = ui_resp['location_identifier']['region'][0] if region else None
        data_centers = ui_resp['location_identifier']['dc'] if dc else None
    else:
        service_names = service if service else None
        region_name = region if region else None
        data_centers = restapi.create_incident_path([region])['dc'] if dc else None
    log.info(f"Extracted: services={service_names}, regions={region_name}, data_centers={data_centers}")
    # Base query with time range and created_by
    query = f"created >= '{timeline[0]}' AND created <= '{timeline[1]}' AND created_by = 'TIP.SA'"
    # Collect optional filter clauses only when values are present
    filters = []
    if region_name:
        filters.append(f"(regions LIKE '%\"{region_name}\"%')")
    if service_names:
        filters.append(f"(service_names LIKE '%\"{service_names}\"%')")
    if data_centers:
        dc_conditions = [f"'{dc}'" for dc in data_centers]
        filters.append(f"metadata->'datacenters' ?| array[{', '.join(dc_conditions)}]")
    # Only append filters if we actually have any
    if filters:
        query += " AND " + " AND ".join(filters)
    payload = { "fields": [], "where": query, "page": 1, "page_size": 1500 }
    return payload

def post_data(config, payload):
    alert_timeline_url = f"{config['var_endpoint_ui']}incidents/alert-timeline"
    aiops_api_timeline = restapi.RestApi(alert_timeline_url, "", payload)
    return aiops_api_timeline.post()

def process_timeline_response(incident, timeline_resp):
    if not timeline_resp:
        log.warning(f"region/incident:{incident}, no response found for alert timeline api")
        return {'id': incident, 'api_status': True, 'tip_mapping': 'no data'}
    if (type(timeline_resp) == dict and timeline_resp.get('error', '') and
            timeline_resp.get('error')['status_code'] == 500):
        log.error(f"region/incident:{incident}, API Failed")
        return {'id': incident, 'api_status': False, 'tip_mapping': False}
    tip_mapping = restapi.is_tip_generated(incident, timeline_resp)
    log.info(f"region/incident:{incident}, "
             f"tip_mapping: {all(incident[list(incident.keys())[0]] for incident in tip_mapping)},"
             f"incidents:{[list(incident.keys())[0] for incident in tip_mapping]}")
    return {'id': incident, 'api_status': True,
            'tip_mapping': all(incident[list(incident.keys())[0]] for incident in tip_mapping),
            'full_data': tip_mapping}

def process_incident_timeline_response(config, results, st_time, en_time, ui_resp=None,
                                       region=None, service=None, dc=None):
    if results['api_status'] and results['tip_mapping'] != 'no data':
        incident_parquet = glob.glob('snow_inc_*.parquet')[0]
        if ui_resp:
            inp_region = ui_resp['location_identifier']['region'][0]
            inp_service = ui_resp['service_names'][0]
            dc = ui_resp['location_identifier']['dc'][0]
        else:
            inp_region = region
            inp_service = service
            dc = restapi.create_incident_path([inp_region])['dc'] if dc else None
        api_incident_ids = [list(incident.keys())[0] for incident in results['full_data']]
        parquet_dc_list, parquet_incident_ids = restapi.filter_incident_parquet_file(incident_parquet,
                                                                                    (st_time, en_time),
                                                                                    region=inp_region, service=inp_service)
        log.info(f"alert-timeline_incidents_cnt:{len(api_incident_ids)}, parquet_incidents, cnt:{len(parquet_incident_ids)}")
        if parquet_incident_ids and not dc:
            return True if sorted(api_incident_ids) == sorted(parquet_incident_ids) else False
        elif parquet_incident_ids and dc:
            dc_filtered_incident = []
            if sorted(api_incident_ids) == sorted(parquet_incident_ids):
                for api_incident in  results['full_data']:
                    if ((type(api_incident['datacenter']) == str and api_incident['datacenter'] in parquet_dc_list) or
                            (type(api_incident['datacenter']) == list and api_incident['datacenter'] == parquet_dc_list)):
                        dc_filtered_incident.append(api_incident)
            else:
                return False
    return "no data"


@mark.smoke
@mark.funct_alert_timeline
@mark.parametrize('at_dc_flag', ["ON", "OFF"])
def test_aiops_alert_timeline1_validate_incident_dc(test_config, ui_resp_data, at_dc_flag):
    """
    1. Get incident details using an incident ID from the var_incident_list
    2. Extract service_name from crn_masks
    3. Extract location identifiers (region and data centers)
    4. Create a payload with these filters plus a time range
    5. Send a request to the alert-timeline endpoint with this payload
    6. Validate that all incidents in the response are TIP-generated
    7. check that incidents are matching between alert-timeline & parquet.
    """
    log.info(test_aiops_alert_timeline1_validate_incident_dc.__doc__)
    dc = True if at_dc_flag == "ON" else False
    all_results = []
    # 1. Get incident details using an incident ID from the var_incident_list
    for incident_id in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_id}  #####')
        # Step 3: Create payload with time range
        ui_resp = ui_resp_data[incident_id]['ui_data']
        # 2. Extract service_name from crn_masks
        # 3. Extract location identifiers (region and data centers)
        # 4. Create a payload with these filters plus a time range
        start_time, end_time = restapi.get_custom_timerange(interval='24hours')
        payload = build_alert_timeline_query(ui_resp=ui_resp, service=True, region=True, dc=dc,
                                             timeline=(start_time, end_time))
        # 5. Send a request to the alert-timeline endpoint with this payload
        timeline_resp = post_data(test_config, payload)
        # 6. Validate that all incidents in the response are TIP-generated
        results = process_timeline_response(incident_id, timeline_resp)
        # 7. check that incidents are matching between alert-timeline & parquet.
        results['at_inc_status'] = process_incident_timeline_response(test_config, results,
                                                                      start_time, end_time, ui_resp=ui_resp, dc=dc)
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        if incident['api_status'] is False or incident['tip_mapping'] is False or incident['at_inc_status'] is False:
            assert False, f"API/TIP verification failed for incident:{incident}"


@mark.funct_alert_timeline
@mark.parametrize('at_dc_flag', ["ON", "OFF"])
def test_aiops_alert_timeline2_fetch_and_validate_withoutservice_dc(test_config, ui_resp_data, at_dc_flag):
    """
    1. get region details and iterate.
    2. create a payload with region and also with & without dc
    3. send a request to the alert-timeline endpoint with this payload
    4. validate that all incidents in the response are TIP-generated
    5. check that incidents are matching between alert-timeline & parquet.
    6. assert, api response or tip validation failed
    """
    log.info(test_aiops_alert_timeline2_fetch_and_validate_withoutservice_dc.__doc__)
    dc = True if at_dc_flag == "ON" else False
    all_results = []
    # 1. get region details and iterate.
    for region in random.sample(restapi.region_list, 3):
        log.info(f'REGION: #####  {region}  #####')
        # 2. create a payload with region and also with & without dc
        start_time, end_time = restapi.get_custom_timerange(interval='24hours')
        payload = build_alert_timeline_query(region=region, dc=dc, timeline=(start_time, end_time))
        # 3. Send a request to the alert-timeline endpoint with this payload
        timeline_resp = post_data(test_config, payload)
        # 4. validate that all incidents in the response are TIP-generated
        results = process_timeline_response(region, timeline_resp)
        # 5. check that incidents are matching between alert-timeline & parquet.
        results['at_inc_status'] = process_incident_timeline_response(test_config, results, start_time, end_time,
                                                                      ui_resp=None, region=region, dc=dc)
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        if incident['api_status'] is False or incident['tip_mapping'] is False or incident['at_inc_status'] is False:
            # 6. assert, api response or tip validation failed
            assert False, f"API/TIP verification failed for incident:{incident}"


@mark.funct_alert_timeline
@mark.parametrize('at_dc_flag', ["ON", "OFF"])
def test_aiops_alert_timeline3_fetch_and_validate_withservice_dc(test_config, ui_resp_data, at_dc_flag):
    """
    1. get region details and iterate.
    2. randomly choose one service name from network or compute or storage
    3. create a payload with fetched region, service_name and also with & without dc
    4. send a request to the alert-timeline endpoint with this payload
    5. validate that all incidents in the response are TIP-generated
    6. check that incidents are matching between alert-timeline & parquet.
    7. assert, api response or tip validation failed
    """
    log.info(test_aiops_alert_timeline3_fetch_and_validate_withservice_dc.__doc__)
    dc = True if at_dc_flag == "ON" else False
    all_results = []
    # 1. get region details and iterate.
    for region in random.sample(restapi.region_list, 3):
        log.info(f'REGION: #####  {region}  #####')
        # 2. randomly choose one service name from network or compute or storage
        service_names_list = ["compute_service_names", "network_service_names", "storage_service_names"]
        service = random.choice(test_config[random.choice(service_names_list)])
        # 3. create a payload with fetched region, service_name and also with & without dc
        start_time, end_time = restapi.get_custom_timerange(interval='24hours')
        payload = build_alert_timeline_query(region=region, service=service, dc=dc, timeline=(start_time, end_time))
        # 4. Send a request to the alert-timeline endpoint with this payload
        timeline_resp = post_data(test_config, payload)
        # 5. validate that all incidents in the response are TIP-generated
        results = process_timeline_response(region, timeline_resp)
        # 6. check that incidents are matching between alert-timeline & parquet.
        results['at_inc_status'] = process_incident_timeline_response(test_config, results, start_time, end_time,
                                                                      ui_resp=None, region=region, dc=dc, service=service)
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        if incident['api_status'] is False or incident['tip_mapping'] is False or incident['at_inc_status'] is False:
            # 7. assert, api response or tip validation failed
            assert False, f"API/TIP verification failed for incident:{incident}"
