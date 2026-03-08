import logging as log
from pytest import mark
import aiops_restapi as restapi


@mark.smoke
@mark.funct_alert_correlation
def test_aiops_alert_correlation1_api_validation(test_config):
    """
    1. iterate through the provided incident list.
    2. Submit the alert_correlation API and get the response
    3. Analyse the response and assert if api is not successful
    """
    log.info(test_aiops_alert_correlation1_api_validation.__doc__)
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # 2. Submit the alert_correlation API and get the response
        alert_corr_response = restapi.get_fewer_incident_info(test_config['var_endpoint_ui'],
                                                            f'alert-correlation/{incident_list}', {})
        results = {"incident" : incident_list, "alert_corr_api_status" : f"{alert_corr_response['success']}"}
        log.info(f"status: {results}")
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        # 3. Analyse the response and assert if api is not successful
        assert incident['alert_corr_api_status'], f'alert correlation API failed for the incident:{incident}'
