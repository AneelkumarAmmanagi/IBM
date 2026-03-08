import logging as log
from pytest import mark
import aiops_restapi as restapi

def process_sw_response(incident, ui_resp):
    """ process slack warroom response """
    if 'total_results' in ui_resp:
        all_wr_incident = []
        for wr_incident in ui_resp['documents']:
            response = {'api_execution': True,
                        'wr_incident': wr_incident['incident_id'],
                        'score': wr_incident['score']}
            all_wr_incident.append(response)
            log.info(f"incident:{incident}, processed_data:{all_wr_incident}")
        else:
            response = {'api_execution': True,
                        'wr_incident': None,
                        'score': None}
            log.info(f"incident:{incident}, processed_data:{response}")
        return all_wr_incident if all_wr_incident else [response]
    else:
        response = {'api_execution': False, 'wr_incident': None, 'score': None}
        log.info(f"incident:{incident}, processed_data:{response}")
        return [response]


@mark.smoke
@mark.funct_slack_warroom
def test_aiops_slack_warroom1_fetch(test_config):
    """
    1. iterate through the provided incident list.
    2. Submit the slack warroom API and get the response
    3. analyse the response and extract api status, parent incident and children count
    4. assert if api execution failed
    """
    log.info(test_aiops_slack_warroom1_fetch.__doc__)
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        log.info(f"INCIDENT: #####  {incident_list}  #####")
        # 2. Submit the slack warroom API and get the response
        payload = {"incident_number": f"{incident_list}",
                   "top_k":10, "min_score":0.5}
        sw_resp = restapi.get_fewer_incident_info(test_config['var_endpoint_ui'],
                                                  'slack-warroom', payload)
        # 3. analyse the response and extract api status, parent incident and children count
        processed_data = process_sw_response(incident_list, sw_resp)
        all_results.append({'incident': incident_list, 'processed_data': processed_data})
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        # 4. assert if api execution failed
        for data in incident['processed_data']:
            assert data['api_execution'], 'slack warroom api failed'
            if not (data['wr_incident'] and data['score']):
                log.warning(f"incident:{incident['incident']}, warroom info:{data} not found")
            else:
                log.info(f"incident:{incident['incident']}, warroom info:{data} found")
