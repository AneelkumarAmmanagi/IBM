import logging as log
from pytest import mark
import aiops_restapi as restapi


@mark.skip(reason="As this testcase submitting an issue on github, skipping it")
@mark.funct_feedback
def test_aiops_feedback1_submit_feedback(test_config):
    """
    1. iterate through the provided incident list.
    2. prepare payload with Subject & comment
    3. Submit the Feedback-API and get the response
    4. Analyse the response and assert if api is not successful
    """
    log.info(test_aiops_feedback1_submit_feedback.__doc__)
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        results = {'incident': incident_list}
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # 2. prepare payload with Subject & comment
        payload = {'incidentNumber': incident_list, 'submittedBy': 'Aiops Automation Framework',
                   'subject': 'Aiops Automation', 'comment': 'created by test script, kindly ignore'}
        # 3. Submit the Feedback-API and get the response
        feedback_response = restapi.get_fewer_incident_info(test_config['var_endpoint_ui'],
                                                            'github-feedback', payload)
        log.info(f"incident:{incident_list}, feedback_response:{feedback_response}")
        all_results.append(feedback_response)
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        # 4. Analyse the response and assert if api is not successful
        assert incident.get('success', False), f'Feedback submission failed for the incident:{incident}'
