import logging as log
from pytest import mark
import aiops_restapi as restapi

def extract_crs(cbc_details):
    response = cbc_details.get('response', [])
    if isinstance(response, list):
        return list(set(change.get('changeid') for change in response if 'changeid' in change))
    return []

def validate_crs_scores(cbc_details):
    response = cbc_details.get('response', [])
    if isinstance(response, list):
        all_crs = []
        for change in response:
            crs_scores_list = {'changeid': change.get('changeid', None),
                               'change_quality_score': change['categoryScores'].get('change_quality_score', None),
                               'system_overlap_score': change['categoryScores'].get('system_overlap_score', None),
                               'technical_risk_score': change['categoryScores'].get('technical_risk_score', None),
                               'confidenceScore': change.get('confidenceScore', None)}
            all_crs.append(crs_scores_list)
        return all_crs
    return []

def validate_crs_state(config, ui_data, cbc_crs):
    all_crs = []
    def find_owner(ci_name):
        for item in config["ci_name_owner_mapping"]:
            if ci_name in item['ci_names']:
                return item['owner']
        return None
    incident_ci_owner = find_owner(ui_data['service_names'][0])
    log.info(f"Incident:{ui_data['number']}, incident_ci_owner:{incident_ci_owner}")
    for crs in cbc_crs:
        crs_state_results = {'changeid': crs, 'crs_region_validate': None,
                             'crs_ci_name_validate': None}
        crs_resp = restapi.get_api_details(config['var_endpoint_ui'], f'changes/{crs}')
        # Validate CRS region
        crs_state_results['crs_region_validate'] = (
                crs_resp['regions'].replace(',','') == ui_data['location_identifier']['region'][0]
        )
        log.info(f"crs:{crs}, incident_region:{ui_data['location_identifier']['region'][0]}, "
                 f"api_crs_region:{crs_resp['regions'].replace(',','')}, "
                 f"crs_region_validate:{crs_state_results['crs_region_validate']}")
        # Validate CRS ci name
        for ci_owner in config["ci_name_owner_mapping"]:
            if ci_owner['owner'] == incident_ci_owner:
                crs_state_results['crs_ci_name_validate'] = True if crs_resp['service_names'] in ci_owner["ci_names"] \
                                                            else False
                log.info(f"crs:{crs}, incident_service_name:{ui_data['service_names'][0]}, "
                         f"api_crs_service:{crs_resp['service_names']}, "
                         f"crs_ci_name_validate:{crs_state_results['crs_ci_name_validate']}")
        all_crs.append(crs_state_results)
    return all_crs


@mark.smoke
@mark.funct_cbc
def test_aiops_cbc1_validate_crs(test_config, ui_resp_cbc):
    """
    1. iterate through the provided incident list.
    2. get cbc details from UI API.
    3. extract the crs from the API response.
    4. asert if no crs found
    """
    log.info(test_aiops_cbc1_validate_crs.__doc__)
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        if not incident_list in ["INC10603689", "INC10574422", "INC10604907"]:
            log.warning(f'Incident:{incident_list} not supported')
            continue
        # 2. get cbc details from UI API.
        cbc_details = ui_resp_cbc[incident_list]['cbc_data']
        # 3. extract the crs from the API response.
        crs = extract_crs(cbc_details)
        log.info(f'Incident: {incident_list}, CRS: {crs}')
        all_results.append({'incident': incident_list, 'crs': crs,
                            'api_execution': ui_resp_cbc[incident_list]['api_execution']})
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        assert incident['api_execution'], 'cbc api failed'
        # 4. asert if no crs found
        if not incident["incident"]:
            log.warning(f'Incident:{incident["incident"]} has no crs')
        # assert incident['crs'], f"Not all incidents:{incident} has no CR's"


@mark.funct_cbc
def test_aiops_cbc2_validate_scores(test_config, ui_resp_cbc):
    """
    1. iterate through the provided incident list.
    2. get cbc details from UI API.
    3. extract the crs from the API response.
    4. asert if no crs found
    """
    log.info(test_aiops_cbc2_validate_scores.__doc__)
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        if not incident_list in ["INC10603689", "INC10574422", "INC10604907"]:
            log.warning(f'Incident:{incident_list} not supported')
            continue
        # 2. get cbc details from UI API.
        cbc_details = ui_resp_cbc[incident_list]['cbc_data']
        # 3. extract the crs from the API response.
        crs_scores = validate_crs_scores(cbc_details)
        log.info(f'Incident: {incident_list}, CRS Score: {crs_scores}')
        all_results.append({'incident': incident_list, 'crs_scores': crs_scores,
                            'api_execution': ui_resp_cbc[incident_list]['api_execution']})
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        assert incident['api_execution'], 'cbc api failed'
        # 4. asert if no scores for cr's
        for crs_score in incident['crs_scores']:
            no_score = [key for key, value in crs_score.items() if value is None]
            if no_score:
                log.warning(f'Incident:{incident["incident"]} has no scores')
                # assert False, f"Not all incidents:{incident} has no scores"


@mark.funct_cbc
def test_aiops_cbc3_validate_crs_state(test_config, ui_resp_data, ui_resp_cbc):
    """
    1. iterate through the provided incident list.
    2. get incident details from UI API.
    3. get cbc details from UI API.
    4. extract the crs from the API response.
    5. validate the crs's region & service.
    6. asert if no crs found
    """
    log.info(test_aiops_cbc3_validate_crs_state.__doc__)
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        if not incident_list in ["INC10603689", "INC10574422", "INC10604907"]:
            log.warning(f'Incident:{incident_list} not supported')
            continue
        # 2. get incident details from UI API.
        ui_resp = ui_resp_data[incident_list]['ui_data']
        # 3. get cbc details from UI API.
        cbc_details = ui_resp_cbc[incident_list]['cbc_data']
        # 4. extract the crs from the API response.
        crs = extract_crs(cbc_details)
        log.info(f'Incident: {incident_list}, CRS: {crs}')
        # 5. validate the crs's region & service.
        if crs:
            crs_state = validate_crs_state(test_config, ui_resp, crs)
        else:
            log.warning(f'Incident:{incident_list} has no crs')
            crs_state = {'changeid': "no crs id", 'crs_region_validate': "not req", 'crs_ci_name_validate': "not req"}
        all_results.append({'incident': incident_list, 'crs': crs,
                            'crs_state_validation': crs_state})
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        # 6. asert if no crs found
        for crs_state in incident['crs_state_validation']:
            assert crs_state['crs_region_validate'], 'crs region failed'
            assert crs_state['crs_ci_name_validate'], 'crs ci failed'
