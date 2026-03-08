import logging as log
from pytest import mark, skip
import aiops_restapi as restapi

def compare_database_staticinfo(conf, db_data):
    fields = [('Executive Summary', 'INC9258603_executive_summary'),
              ('Incident Details', 'INC9258603_incident_details'),
              ('Impact Analysis', 'INC9258603_impact_analysis'),
              ('Actions and Resolution', 'INC9258603_action_resolution'),
              ('Comment Summary', 'INC9258603_comment_summary'),
              ('Recommendations', 'INC9258603_recommendation'),
              ('Timeline', 'INC9258603_timeline'),
              ('Impact Details', 'INC9258603_impact_details'),
              ('General Information', 'INC9258603_general_information'),]
    full_data = {}
    for ui_name, com_var in fields:
        if ui_name in ['General Information', 'Impact Details']:
            field_name = ui_name + '\n- '
            output = [value for value in db_data if field_name in value][0]
            resp = output.split(field_name)[-1].replace('\n-', '').replace('\n', '').replace('**', '').replace('  ', ' ')
            full_data[ui_name] = resp
        else:
            field_name = ui_name + '\n* '
            output = [value for value in db_data if field_name in value][0]
            resp = output.split(field_name)[-1].replace('\n*', '').replace('\n', '')
        if conf[com_var] == resp:
            full_data[ui_name] = True
        else:
            full_data[ui_name] = False
    return full_data

def validate_impact_details_fields(ui_data, snow_data):
    fields = [('unit_type', 'Unit Type'), ('units_affected', 'Units Affected'),
              ('total_units', 'Total Units'), ('impact_adjustment_factor', 'Impact Adjustment Factor'),
              ('disruption_time','Disruption Time')]
    full_data = {}
    for ui_snow_key, ui_name in fields:
        if ui_data.get(ui_snow_key, None) == snow_data.get(ui_snow_key, None):
            full_data[ui_name] = {str(ui_data.get(ui_snow_key, None)): True}
        else:
            full_data[ui_name] = {str(ui_data.get(ui_snow_key, None)): False}
    return full_data

def validate_general_information_fields(ui_data, snow_data):
    fields = [('number', 'Incident Number'), ('severity', 'Severity'),
              ('created_by', 'Created By'), ('tribe', 'Tribe'),
              ('was_customer_impacted','Was Customer Impacted?'),
              ('caused_by_change','Caused by Change?')]
    full_data = {}
    for ui_snow_key, ui_name in fields:
        if ui_data.get(ui_snow_key, None) == snow_data.get(ui_snow_key, None):
            full_data[ui_name] = {str(ui_data.get(ui_snow_key, None)): True}
        else:
            full_data[ui_name] = {str(ui_data.get(ui_snow_key, None)): False}
    return full_data


@mark.smoke
@mark.funct_incident_summary
def test_aiops_incident_sum1_general_information(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. check & extract details of incident from UI using ui-api.
    3. fetch all the details from service now via snow-api.
    4. validate general information fields by comparing snow & ui data.
    5. assert if fields not matches the information.
    """
    log.info(test_aiops_incident_sum1_general_information.__doc__)
    results = {}
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # get incident details from UI API
        ui_resp = ui_resp_data[incident_list]['ui_data']
        # ui_host_info = ui_resp_data[incident_list]['host_uuid']
        log.info(f"validating incident:{incident_list} in state: {ui_resp['state']}")
        # get incident details from SNOW API
        snow_resp = snow_resp_data[incident_list]['snow_data']
        # snow_host_info = snow_resp_data[incident_list]['host_uuid']
        # get incident details from database API
        data = restapi.get_database_response(test_config, incident_list)
        # compare ui & snow data
        full_data1 = validate_general_information_fields(ui_resp, snow_resp)
        log.info(f'general information on ui for {incident_list} is {full_data1}')
        # compare database & snow data
        full_data2 = restapi.compare_database_and_snow_general_info(data.split('##')[1], snow_resp)
        log.info(f'general information in db for {incident_list} is {full_data2}')
        full_data_res1 = all(value.values() for value in full_data1.values())
        full_data_res2 = all(value.values() for value in full_data2.values())
        if full_data1 == full_data2 and full_data_res1 == full_data_res2:
            log.info(f'All the general information of {incident_list} is correct')
            result = True
        else:
            log.error(f'All the general information of {incident_list} is not correct')
            result = False
        results[incident_list] = result
    log.info(f'Overall results: {results}')
    if False in results.values():
        assert False, 'Not all incidents returned excepted values'


@mark.funct_incident_summary
def test_aiops_incident_sum2_impact_details(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. check & extract details of incident from UI using ui-api.
    3. fetch all the details from service now via snow-api.
    4. validate impact detail fields by comparing snow & ui data.
    5. assert if fields not matches the information.
    """
    log.info(test_aiops_incident_sum2_impact_details.__doc__)
    results = {}
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # get incident details from UI API
        ui_resp = ui_resp_data[incident_list]['ui_data']
        # ui_host_info = ui_resp_data[incident_list]['host_uuid']
        log.info(f"validating incident:{incident_list} in state: {ui_resp['state']}")
        # get incident details from SNOW API
        snow_resp = snow_resp_data[incident_list]['snow_data']
        # snow_host_info = snow_resp_data[incident_list]['host_uuid']
        full_data = validate_impact_details_fields(ui_resp, snow_resp)
        log.info(f'impact detail information of {incident_list} is {full_data}')
        result = all(value.values() for value in full_data.values())
        if result:
            log.info(f'All the impact detail information of {incident_list} is correct')
        else:
            log.error(f'All the impact detail information of {incident_list} is not correct')
        results[incident_list] = result
    log.info(f'Overall results: {results}')
    if False in results.values():
        assert False, 'Not all incidents returned excepted values'


@mark.funct_incident_summary
def test_aiops_incident_sum3_fewer_fields(test_config, ui_resp_data, snow_resp_data):
    """
    1. iterate through the provided incident list.
    2. check & extract details of incident from db using ui-api.
    3. validate below fields by comparing db & static data.
        --> Executive Summary
        --> Incident Details
        --> Timeline
        --> Impact Analysis
        --> Actions and Resolution
        --> Comment Summary
        --> Recommendations
        --> General Information
        --> Impact Details
    4. assert if fields not matches the information.
    """
    results = {}
    log.info(test_aiops_incident_sum3_fewer_fields.__doc__)
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        if not incident_list == 'INC9258603':
            log.warning(f'Incident:{incident_list} not supported')
            continue
        # get incident details from database API
        data = restapi.get_database_response(test_config, incident_list)
        full_data = compare_database_staticinfo(test_config, data.split('##'))
        log.info(f'Information of {incident_list} is {full_data}')
        result = all(full_data.values())
        if result:
            log.info(f'All the informations of {incident_list} is correct')
        else:
            log.error(f'All informations of {incident_list} is not correct')
        results[incident_list] = result
    log.info(f'Overall results: {results}')
    if False in results.values():
        assert False, 'Not all informations are correct'
