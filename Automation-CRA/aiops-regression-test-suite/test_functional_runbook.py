import logging as log
from pytest import mark
import aiops_restapi as restapi


@mark.smoke
@mark.funct_runbook
def test_aiops_runbook1_api_validation(test_config, ui_resp_data):
    """
    1. iterate through the provided incident list.
    2. prepare payload with description & query
    3. Submit the runbook-API and get the response
    4. Analyse the response and assert if api is not successful
    """
    log.info(test_aiops_runbook1_api_validation.__doc__)
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # 2. prepare payload with description & query
        ui_resp = ui_resp_data[incident_list]['ui_data']
        payload = {'number': incident_list, 'limit': 10, "skip": 0,
                   'short_description': ui_resp['short_description'], 'long_description': ui_resp['long_description']}
        payload['query'] = f"{payload['number']} {ui_resp['short_description']} {ui_resp['long_description']}"
        # 3. Submit the runbook-API and get the response
        runbook_response = restapi.get_fewer_incident_info(test_config['var_endpoint_ui'],
                                                            'runbooks', payload)
        results = {"incident": incident_list, "runbook_resp_cnt" :len(runbook_response)}
        log.info(f"status: {results}")
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        # 4. Analyse the response and assert if api is not successful
        assert incident['runbook_resp_cnt'] != 0 , f'runbook submission failed for the incident:{incident}'
