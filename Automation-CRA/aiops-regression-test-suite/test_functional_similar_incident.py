import logging as log
from pytest import mark, skip
import aiops_restapi as restapi

def convert_str_dict_resolution_steps(input):
    output = {}
    for line in input.strip().split('\n'):
        if ' **' in line:
            key_value = line.split('**:')
            if len(key_value) == 2:
                key = key_value[0].split('*')[-1]
                value = key_value[1].strip()
                output[key] = value
    return output

def compare_ui_static_similar_incident(conf, ui_data):
    if len(conf['INC9258603_similar_incidents']) == len(ui_data):
        for incident in conf['INC9258603_similar_incidents']:
            if incident not in ui_data:
                return False
    return True

def compare_ui_static_probable_rootcause(conf, incident, ui_probable):
    if conf['INC9258603_probable_rootcause'] == ui_probable['result']['content']:
        log.info(f"probable route cause for: {incident} in ui is: {ui_probable['result']['content']}")
        return True
    return False

def compare_ui_static_resolution_steps(conf, incident, ui_resolution):
    data = convert_str_dict_resolution_steps(ui_resolution['resolution_steps'])
    log.info(f'resolution steps for: {incident} in ui is {data}')
    for issue, reason in conf['INC9258603_resolution_steps'].items():
        if not reason == data[issue]:
            log.error(f'resolution steps not matched in ui with static information: {conf["INC9258603_resolution_steps"]}')
            return False
    log.info(f'resolution steps matched in ui with static information: {conf["INC9258603_resolution_steps"]}')
    return True


@mark.smoke
@mark.funct_similar_incident
def test_aiops_similar_inc1_similar_incident(test_config):
    """
    1. iterate through the provided incident list.
    2. check similar incident details of incident from UI using ui-api.
    3. compare the ui similar incident with static information.
    4. assert if comparison fails.
    """
    log.info(test_aiops_similar_inc1_similar_incident.__doc__)
    results = {}
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        if not incident_list == 'INC9258603':
            log.warning(f'Incident:{incident_list} not supported')
            continue
        # get incident details from UI API
        payload = {"incident_number": incident_list}
        ui_resp = restapi.get_fewer_incident_info(test_config['var_endpoint_ui'],
                                                             'incidents/similar', payload)
        log.info(f'Incident :{incident_list} response:{ui_resp}')
        similar_incidents = []
        if ui_resp['similar_ids']:
            similar_incidents = [incident['id'] for incident in ui_resp['similar_ids']]
            log.info(f'similar incidents: {",".join(similar_incidents)} found for {incident_list}')
        else:
            log.error(f'similar incident not found for the Incident :{incident_list}')
        result = compare_ui_static_similar_incident(test_config, similar_incidents)
        results[incident_list] = result
    log.info(f'Overall results: {results}')
    if False in results.values():
        assert False, 'Not all incidents returned excepted values'


@mark.funct_similar_incident
def test_aiops_similar_inc2_resolution_steps(test_config):
    """
    1. iterate through the provided incident list.
    2. check similar incident details of incident from UI using ui-api.
    3. compare the ui similar incident with static information.
    4. assert if comparison fails.
    """
    log.info(test_aiops_similar_inc2_resolution_steps.__doc__)
    results = {}
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        if not incident_list == 'INC9258603':
            log.warning(f'Incident:{incident_list} not supported')
            continue
        # get incident details from UI API
        payload = {"incident_number": incident_list}
        ui_similar = restapi.get_fewer_incident_info(test_config['var_endpoint_ui'],
                                                             'incidents/similar', payload)
        log.info(f'Incident :{incident_list} response:{ui_similar}')
        similar_incidents = []
        if ui_similar['similar_ids']:
            similar_incidents = [incident['id'] for incident in ui_similar['similar_ids']]
            log.info(f'similar incidents: {",".join(similar_incidents)} found for {incident_list}')
        else:
            log.error(f'similar incident not found for the Incident :{incident_list}')
        result1 = compare_ui_static_similar_incident(test_config, similar_incidents)
        payload_res = {'id': incident_list, 'similar_ids': similar_incidents}
        ui_resolution = restapi.get_fewer_incident_info(test_config['var_endpoint_ui'],
                                                             'incidents/resolution-steps', payload_res)
        result2 = compare_ui_static_resolution_steps(test_config, incident_list, ui_resolution)
        results[incident_list] = {'similar_incidents': result1, 'resolution_steps': result2}
    log.info(f'Overall results: {results}')
    if not all(value.values() for value in results.values()):
        assert False, 'Not all incidents returned excepted values'


@mark.funct_similar_incident
def test_aiops_similar_inc3_probable_rootcause(test_config):
    """
    1. iterate through the provided incident list.
    2. check probable root cause of incident list from UI using ui-api.
    3. compare the ui similar incident with static information.
    4. assert if comparison fails.
    """
    log.info(test_aiops_similar_inc3_probable_rootcause.__doc__)
    results = {}
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        if not incident_list == 'INC9258603':
            log.warning(f'Incident:{incident_list} not supported')
            continue
        # get incident details from UI API
        payload = {"incident_number": incident_list}
        ui_similar = restapi.get_fewer_incident_info(test_config['var_endpoint_ui'],
                                                             'incidents/similar', payload)
        log.info(f'Incident :{incident_list} response:{ui_similar}')
        similar_incidents = []
        if ui_similar['similar_ids']:
            similar_incidents = [incident['id'] for incident in ui_similar['similar_ids']]
            log.info(f'similar incidents: {",".join(similar_incidents)} found for {incident_list}')
        else:
            log.error(f'similar incident not found for the Incident :{incident_list}')
        result1 = compare_ui_static_similar_incident(test_config, similar_incidents)
        payload_res = {'incident_numbers': similar_incidents}
        ui_probable = restapi.get_fewer_incident_info(test_config['var_endpoint_ui'],
                                                             'incidents/probable-root-cause', payload_res)
        result2 = compare_ui_static_probable_rootcause(test_config, incident_list, ui_probable)
        results[incident_list] = {'similar_incidents': result1, 'probable_rootcause': result2}
    log.info(f'Overall results: {results}')
    if not all(value.values() for value in results.values()):
        assert False, 'Not all incidents returned excepted values'
