import logging as log
from pytest import mark
import aiops_restapi as restapi

def process_ri_response(incident, ui_resp):
    """ process related incident response """
    if 'parent' in ui_resp and 'children' in ui_resp:
        parent = ui_resp['parent']
        children = ui_resp['children']
        response = {'incident': incident, 'api_execution': True,
                    'parent': parent.get('number') if parent else None,
                    'child_cnt': len(children)}
        log.info(f"incident:{incident}, processed_data:{response}")
        return response
    else:
        response = {'incident':incident, 'api_execution':False,
                    'parent': None, 'child_cnt': None}
        log.info(f"incident:{incident}, processed_data:{response}")
        return response


@mark.funct_related_incident
def test_aiops_related_incident1_fetch(test_config):
    """
    1. iterate through the provided incident list.
    2. Submit the related incident API and get the response
    3. analyse the response and extract api status, parent incident and children count
    4. assert if api execution failed
    """
    log.info(test_aiops_related_incident1_fetch.__doc__)
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config['var_incident_list']:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # 2. Submit the related incident API and get the response
        ri_resp = restapi.get_api_details(test_config['var_endpoint_ui'],
                                                            f'incidents/related/{incident_list}')
        # 3. analyse the response and extract api status, parent incident and children count
        results = process_ri_response(incident_list, ri_resp)
        all_results.append(results)
    log.info(f'Overall results: {all_results}')
    for incident in all_results:
        # 4. assert if api execution failed
        assert incident['api_execution'], 'related incident api failed'
        if  incident['parent'] == 0 or incident['child_cnt'] == 0:
            log.warning(f"incident:{incident['incident']}, either parent/children not found")

