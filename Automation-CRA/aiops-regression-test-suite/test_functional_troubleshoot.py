import logging as log
from pytest import mark

import aiops_restapi
import aiops_restapi as restapi


@mark.smoke
@mark.funct_troubleshoot
def test_aiops_troubleshoot1_api_validation(test_config, ui_resp_data):
    """
    1. iterate through the provided incident list.
    2. Get ui response of the incident
    3. Generate chatId using tshoot-API
    4. Submit the tshoot-API with chatId and get the response
    5. Analyse the response and assert if api is not successful
    """
    log.info(test_aiops_troubleshoot1_api_validation.__doc__)
    all_results = []
    # 1. iterate through the provided incident list.
    for incident_list in test_config["var_incident_list"]:
        log.info(f'INCIDENT: #####  {incident_list}  #####')
        # 2. Get ui response of the incident
        ui_resp = ui_resp_data[incident_list]["ui_data"]
        # 3. Generate chatId using tshoot-API
        chatid_payload = {"action": "create-chat",
                          "initialQuery":f"Analyze and troubleshoot incident {incident_list}: "
                                         f"{ui_resp['short_description']}. "
                                         f"Additional details: {ui_resp['long_description']}"}
        chatid_response = restapi.get_fewer_incident_info(test_config["var_endpoint_ui"],
                                                            "troubleshooting", chatid_payload)
        log.info(f"incident:{incident_list}, chatId: {chatid_response.get('id', "")}")
        tshoot_payload = {"action": "get-response",
                          "chatId": chatid_response['id']}
        # 4. Submit the tshoot-API with chatId and get the response
        tshoot_response = restapi.get_fewer_incident_info(test_config["var_endpoint_ui"],
                                                            "troubleshooting", tshoot_payload)
        results = {"incident": incident_list, "chatid_api_status": True if chatid_response['id'] else False,
                   "tshoot_api_status" : "finish_reason" in tshoot_response and
                                         tshoot_response["finish_reason"] == "stop"}
        log.info(f"status: {results}")
        all_results.append(results)
    log.info(f"Overall results: {all_results}")
    for incident in all_results:
        # 5. Analyse the response and assert if api is not successful
        assert incident['chatid_api_status'] == True, (f"chatId generation failed for the "
                                                       f"incident:{incident['incident']}")
        assert incident['tshoot_api_status'] == True , (f"tshoot-Api submission failed for the "
                                                        f"incident:{incident['incident']}")
