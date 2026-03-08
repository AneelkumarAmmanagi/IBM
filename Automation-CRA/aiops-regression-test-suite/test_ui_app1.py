import time
import logging as log
from pytest import mark
import aiops_restapi as restapi

def test_aiops_ima1_ui_check_toprow_impacted_account(ui_obj, test_config, var_incident_list):
    for incident_list in var_incident_list:
        ui_obj.goto(f"incidents/{incident_list}")
        ui_obj.click_button('Impacted Accounts')
        resp, host_info = restapi.get_incident_details(test_config['var_endpoint_ui'], 'incidents/', incident_list)
        if host_info:
            log.info(f'Found Host count: {len(host_info)}, Host info: {host_info}')
            ui_obj.click_dropdown()
            for host in host_info:
                ui_obj.click_dropdown(host)
                #ui_obj.click_dropdown()
                #ui_obj.click_button('Select a Host')
                ui_obj.click_button('Fetch Impacted Accounts')
        else:
            log.warning('No Host info')
        time.sleep(5)
        ui_obj.close_window()

def test_aiops_ima2_ui_check_middlerow_impacted_account(ui_obj, test_config, var_incident_list):
    for incident_list in var_incident_list:
        ui_obj.goto(f"incidents/{incident_list}")
        # Add assertions for middlerow impacted account
