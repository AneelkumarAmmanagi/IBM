import logging as log
from pytest import mark
import aiops_restapi as restapi

def validate_close_notes_fields(ui_data, snow_data):
    fields = [ ('close_notes', 'Close Notes'), ('chronology', 'Timeline')]
    full_data = []
    for ui_snow_key, ui_name in fields:
        if ui_data.get(ui_snow_key, None) == snow_data.get(ui_snow_key, None):
            full_data.append({ui_data.get(ui_snow_key, None): True})
        else:
            full_data.append({ui_data.get(ui_snow_key, None): False})
    return full_data


@mark.smoke
@mark.funct_incident_information
def test_aiops_incident_info1_key_information(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. check & extract details of incident from UI using ui-api.
    3. fetch all the details from service now via snow-api.
    4. validate keys information fields by comparing snow & ui data.
    5. assert if fields not matches the information.
    """
    log.info(test_aiops_incident_info1_key_information.__doc__)
    results = {}
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # get incident details from UI API
        ui_resp = ui_resp_data[incident_list]['ui_data']
        # ui_host_info = ui_resp_data[incident_list]['host_id']
        log.info(f"validating incident:{incident_list} in state: {ui_resp['state']}")
        # get incident details from SNOW API
        snow_resp = snow_resp_data[incident_list]['snow_data']
        # snow_host_info = snow_resp_data[incident_list]['host_id']
        full_data = restapi.validate_key_information_fields(ui_resp, snow_resp)
        log.info(f'key information of {incident_list} is {full_data}')
        result = all(value for value in full_data.values())
        if result:
            log.info(f'All the key information of {incident_list} is correct')
        else:
            log.error(f'All the key information of {incident_list} is not correct')
        results[incident_list] = result
    log.info(f'Overall results: {results}')
    if False in results.values():
        assert False, 'Not all incidents returned excepted values'


@mark.funct_incident_information
def test_aiops_incident_info2_reference_information(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. check & extract details of incident from UI using ui-api.
    3. fetch all the details from service now via snow-api.
    4. validate reference information fields by comparing snow & ui data.
    5. assert if fields not matches the information.
    """
    log.info(test_aiops_incident_info2_reference_information.__doc__)
    results = {}
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # get incident details from UI API
        ui_resp = ui_resp_data[incident_list]['ui_data']
        # ui_host_info = ui_resp_data[incident_list]['host_id']
        log.info(f"validating incident:{incident_list} in state: {ui_resp['state']}")
        # get incident details from SNOW API
        snow_resp = snow_resp_data[incident_list]['snow_data']
        # snow_host_info = snow_resp_data[incident_list]['host_id']
        full_data = restapi.validate_reference_information_fields(ui_resp, snow_resp)
        log.info(f'reference information of {incident_list} is {full_data}')
        result = all(value for value in full_data.values())
        if result:
            log.info(f'All the reference information of {incident_list} is correct')
        else:
            log.error(f'All the reference information of {incident_list} is not correct')
        results[incident_list] = result
    log.info(f'Overall results: {results}')
    if False in results.values():
        assert False, 'Not all incidents returned excepted values'


@mark.funct_incident_information
def test_aiops_incident_info3_impact_description(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. check & extract details of incident from UI using ui-api.
    3. fetch all the details from service now via snow-api.
    4. validate impact & description information fields by comparing snow & ui data.
    5. assert if fields not matches the information.
    """
    log.info(test_aiops_incident_info3_impact_description.__doc__)
    results = {}
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # get incident details from UI API
        ui_resp = ui_resp_data[incident_list]['ui_data']
        # ui_host_info = ui_resp_data[incident_list]['host_id']
        log.info(f"validating incident:{incident_list} in state: {ui_resp['state']}")
        # get incident details from SNOW API
        snow_resp = snow_resp_data[incident_list]['snow_data']
        # snow_host_info = snow_resp_data[incident_list]['host_id']
        full_data = restapi.validate_impact_description_fields(ui_resp, snow_resp)
        log.info(f'impact & description information of {incident_list} is {full_data}')
        result = all(value for value in full_data.values())
        if result:
            log.info(f'All the impact & description information of {incident_list} is correct')
        else:
            log.error(f'All the impact & description information of {incident_list} is not correct')
        results[incident_list] = result
    log.info(f'Overall results: {results}')
    if False in results.values():
        assert False, 'Not all incidents returned excepted values'


@mark.funct_incident_information
def test_aiops_incident_info4_comment_list(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. check & extract details of incident from UI using ui-api.
    3. fetch all the details from service now via snow-api.
    4. validate comments by comparing snow & ui data.
    5. assert if fields not matches the information.
    """
    log.info(test_aiops_incident_info4_comment_list.__doc__)
    results = {}
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # get incident details from UI API
        ui_resp = ui_resp_data[incident_list]['ui_data']
        # ui_host_info = ui_resp_data[incident_list]['host_id']
        log.info(f"validating incident:{incident_list} in state: {ui_resp['state']}")
        # get incident details from SNOW API
        snow_resp = snow_resp_data[incident_list]['snow_data']
        # snow_host_info = snow_resp_data[incident_list]['host_id']
        full_status, full_data = restapi.validate_comments_list_fields(ui_resp, snow_resp)
        log.info(f'comment information of {incident_list} is {full_data}')
        if full_status:
            log.info(f'All the comment information of {incident_list} is correct')
        else:
            log.error(f'All the comment information of {incident_list} is not correct')
        results[incident_list] = full_status
    log.info(f'Overall results: {results}')
    if False in results.values():
        assert False, 'Not all incidents returned excepted values'


@mark.funct_incident_information
def test_aiops_incident_info5_close_notes(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. check & extract details of incident from UI using ui-api.
    3. fetch all the details from service now via snow-api.
    4. validate close notes by comparing snow & ui data.
    5. assert if fields not matches the information.
    """
    log.info(test_aiops_incident_info5_close_notes.__doc__)
    results = {}
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # get incident details from UI API
        ui_resp = ui_resp_data[incident_list]['ui_data']
        # ui_host_info = ui_resp_data[incident_list]['host_id']
        log.info(f"validating incident:{incident_list} in state: {ui_resp['state']}")
        # get incident details from SNOW API
        snow_resp = snow_resp_data[incident_list]['snow_data']
        # snow_host_info = snow_resp_data[incident_list]['host_id']
        full_data = validate_close_notes_fields(ui_resp, snow_resp)
        log.info(f'close notes information of {incident_list} is {full_data}')
        result = all(value.values() for value in full_data)
        if result:
            log.info(f'All the close notes information of {incident_list} is correct')
        else:
            log.error(f'All the close notes information of {incident_list} is not correct')
        results[incident_list] = result
    log.info(f'Overall results: {results}')
    if False in results.values():
        assert False, 'Not all incidents returned excepted values'
