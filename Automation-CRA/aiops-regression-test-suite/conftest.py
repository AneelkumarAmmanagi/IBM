import os
import yaml
import time
import pytest
import logging as log
import aiops_restapi as restapi
from datetime import datetime, timedelta, timezone
from aiops_ui import AiopsUI
from dotenv import load_dotenv
from ibm_cloud_sdk_core.authenticators.iam_authenticator import IAMAuthenticator
from ibm_secrets_manager_sdk.secrets_manager_v2 import *

load_dotenv()

@pytest.fixture(scope="session")
def test_config():
    log.info('loading test constants')
    with open("aiops_constants.yaml") as f:
        config = yaml.safe_load(f)
    return get_secret_keys(config)

def get_secret_keys(test_config):
    log.info('reading api keys from secret manager')
    authenticator = IAMAuthenticator(apikey=os.getenv('IAM_TOKEN'))
    secrets_manager_service = SecretsManagerV2(authenticator=authenticator)
    secrets_manager_service.set_service_url(os.getenv('SECRET_MANAGER_URL'))
    
    # Fetch first secret (CRA_SECRET_GROUP_ID)
    try:
        cra_response = secrets_manager_service.get_secret(os.getenv('CRA_SECRET_ID'))
        cra_data = cra_response.result['data']
        test_config['auth_username'] = cra_data['AUTH_USERNAME']
        test_config['auth_password'] = cra_data['AUTH_PASSWORD']
        test_config['assistant_elastic_user'] = cra_data['ASSISTANT_ELASTIC_USER']
        test_config['assistant_elastic_certificate'] = cra_data['ASSISTANT_ELASTIC_CERTIFICATE']
        test_config['assistant_elastic_password'] = cra_data['ASSISTANT_ELASTIC_PASSWORD']
        test_config['assistant_elastic_host'] = cra_data['ASSISTANT_ELASTIC_SERVER_HOST']
        test_config['assistant_elastic_index'] = cra_data['ASSISTANT_ELASTIC_INDEX']
        test_config["data_sync_endpoint"] = cra_data["data_sync_endpoint"]
        test_config["data_sync_api_key"] = cra_data["data_sync_api_key"]
        log.info("Successfully fetched CRA secrets")
    except Exception as e:
        log.error(f"Failed to fetch CRA secrets: {e}")
    
    # Fetch second secret (SECRET_MANAGER_API_KEY)
    try:
        ira_response = secrets_manager_service.get_secret(os.getenv('IRA_SECRET_ID'))
        ira_data = ira_response.result['data']
        test_config['cos_api_key'] = ira_data['slack_api_key']
        test_config['parquet_cos_api_key'] = ira_data['parquet_cos_api_key']
        test_config['snow_api_key'] = ira_data['snow_api_key']
        test_config['data_sync_api_key'] = ira_data['data_sync_key']
        test_config['elastic_user'] = ira_data['elastic_user']
        test_config['elastic_password'] = ira_data['elastic_password']
        test_config['elastic_index'] = ira_data['elastic_index']
        test_config['elastic_host'] = ira_data['elastic_host']
        test_config['cr_cos_api_key'] = ira_data['cr_cos_api_key']
        log.info("Successfully fetched IRA secrets")
    except Exception as e:
        log.error(f"Failed to fetch IRA secrets: {e}")
    
    log.info(f"Final test_config: {test_config}")
    return test_config

@pytest.fixture(scope="session", autouse=True)
def validate_incident(test_config):
    removed_incidents = []
    for incident_list in test_config['var_incident_list'][:]:
        aiops_api = restapi.RestApi(test_config['var_endpoint_ui'], 'incidents/' + incident_list)
        resp = aiops_api.get()
        if 'Failed to fetch incident details' in resp.get('error', ""):
            removed_incidents.append(incident_list)
            test_config['var_incident_list'].remove(incident_list)
    if removed_incidents:
        log.info("####################################################################################################")
        log.warning(f"Removed {removed_incidents} as its invalid "
                    f"and proceeding regression with {test_config['var_incident_list']}")
        log.info("####################################################################################################")
    else:
        log.info("All are valid incidents")
    assert test_config['var_incident_list'], "no incidents available for testing"

@pytest.fixture(scope="session", autouse=True)
def download_parquet(test_config):
    log.warning("deleting the exiting parquet files in repo if available")
    os.system('rm -rf *.parquet')
    parquet_timerange = []
    if test_config['start_parquet_timestamp'] and test_config['end_parquet_timestamp']:
        parquet_timerange = restapi.get_parquet_timerange(test_config['start_parquet_timestamp'],
                                                          test_config['end_parquet_timestamp'])
        for timestamp in parquet_timerange:
            test_config['parquet_timestamp'] = timestamp
            restapi.download_parquet_file(test_config)
        log.info("Downloaded parquet files for the given start and end timestamps")
    if test_config['static_parquet_tf'] and not (test_config['static_parquet_tf'] in parquet_timerange):
        test_config['parquet_timestamp'] = test_config['static_parquet_tf']
        restapi.download_parquet_file(test_config)
    if test_config.get('cr_bucket_name', None) and test_config.get('inc_bucket_name', None):
        module_list = [{"bucket": "obs-snow-change-requests-feed-raw", "prefix": "latest_60dayssnapshot_file",
                        "filename": "obs-snow-changes-60dayssnapshot-latest.json"},
                       {"bucket": "obs-snow-incidents-feed-raw", "prefix": "latest_30dsnapshot_file",
                        "filename": "obs-snow-incidents-latest.json"}]
        for module in module_list:
            restapi.download_cr_cos_parquet_file(test_config, module)
            pass

@pytest.fixture(scope="session")
def snow_resp_data(test_config):
    snow_response = {}
    for incident_list in test_config['var_incident_list']:
        resp, host_info = restapi.servicenow_data(test_config['var_endpoint_snow'],
                                                            incident_list, test_config['snow_api_key'])
        resp, uuid_info = restapi.servicenow_data(test_config['var_endpoint_snow'],
                                                            incident_list, test_config['snow_api_key'], 'uuid')
        if resp['crn_masks'][0].split(':')[5]:
            available_path = resp['crn_masks'][0].split(':')[5]
        elif resp['crn_masks'][0].split(':')[4]:
            available_path = resp['crn_masks'][0].split(':')[4]
        else:
            available_path = ""
        resp['location_identifier'] = restapi.create_incident_path([available_path])
        resp['service_names'] = restapi.extract_service_names(resp)
        full_data = {'snow_data' : resp, 'host_id': host_info, 'uuid_id': uuid_info}
        snow_response.update({incident_list: full_data})
    return snow_response

@pytest.fixture(scope="session")
def ui_resp_data(test_config):
    ui_response = {}
    for incident_list in test_config['var_incident_list']:
        resp, host_info = restapi.get_incident_details(test_config['var_endpoint_ui'],
                                                            'incidents/', incident_list)
        resp, uuid_info = restapi.get_incident_details(test_config['var_endpoint_ui'],
                                                            'incidents/', incident_list, 'uuid')
        if resp['crn_masks'][0].split(':')[5]:
            available_path = resp['crn_masks'][0].split(':')[5]
        elif resp['crn_masks'][0].split(':')[4]:
            available_path = resp['crn_masks'][0].split(':')[4]
        else:
            available_path = ""
        resp['location_identifier'] = restapi.create_incident_path([available_path])
        resp['service_names'] = restapi.extract_service_names(resp)
        full_data = {'ui_data' : resp, 'host_id': host_info, 'uuid_id': uuid_info}
        ui_response.update({incident_list: full_data})
    return ui_response

@pytest.fixture(scope="session")
def ui_resp_cbc(test_config):
    cbc_response = {}
    for incident_list in test_config['var_incident_list']:
        resp = restapi.get_api_details(test_config['var_endpoint_ui'],
                                       f'ivh/incident/topscore/{incident_list}')
        full_data = {'api_execution': True if resp['success'] else False, 'cbc_data' : resp}
        cbc_response.update({incident_list: full_data})
    return cbc_response

def ui_obj(test_config):
    ui = AiopsUI(test_config['var_aiops_ui'])
    ui.goto()
    time.sleep(5)
    ui.login(os.getenv("IBM_USER"), os.getenv("IBM_PASS"))
    try:
        yield ui
    finally:
        ui.close()


# ================= summay and detailed view  =================
@pytest.fixture(scope="function")
def summary_elastic_data(test_config, request):

    marker = request.node.get_closest_marker("filters")
    test_filters = marker.kwargs if marker else {}
    log.info(f"Elastic Filters → {test_filters}")

    base_url = test_config['assistant_elastic_host']
    username = test_config['assistant_elastic_user']
    password = test_config['assistant_elastic_password']
    index = test_config['assistant_elastic_index']

    ca_path = restapi.write_ca_cert(
        test_config['assistant_elastic_certificate']
    )

    endpoint = f"{index}/_search"
    log.info(f"Fetching Elastic analyzed-changes from: {base_url}{endpoint}")

    # --------------------------
    # PLANNED DATE OVERRIDE LOGIC
    # --------------------------
    planned_start_date = test_filters.get("planned_start_date")
    planned_end_date = test_filters.get("planned_end_date")

    filter_clauses = []

    if planned_start_date or planned_end_date:

        start_dt = datetime.strptime(
            planned_start_date, "%m/%d/%Y"
        ).replace(
            tzinfo=timezone.utc,
            hour=0, minute=0, second=0, microsecond=0
        )

        end_dt = datetime.strptime(
            planned_end_date or planned_start_date, "%m/%d/%Y"
        ).replace(
            tzinfo=timezone.utc,
            hour=23, minute=59, second=59, microsecond=999000
        )

        filter_clauses.append({
            "range": {
                "planned_start": {
                    "gte": start_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "lte": end_dt.strftime("%Y-%m-%dT%H:%M:%S.999Z")
                }
            }
        })

        log.info("Elastic using ONLY planned_start_date / planned_end_date filter")

    else:
        # --------------------------
        # EXISTING TIME RANGE LOGIC
        # --------------------------
        hours = test_filters.get("hours")
        start_of_day, end_of_day = restapi.calculate_time_range(hours)

        start_date = start_of_day.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_date = end_of_day.strftime("%Y-%m-%dT%H:%M:%S.999Z")

        filter_clauses.append({
            "range": {
                "planned_start": {
                    "gte": start_date,
                    "lte": end_date
                }
            }
        })

        FIELD_MAPPING = {
            "regions": "regions.keyword",
            "state": "state.keyword",
            "tribe": "tribe.keyword",
            "service_names": "service_names.keyword",
            "deployment_method": "deployment_method.keyword",
            "dc": "dc.keyword"
        }

        for field, value in test_filters.items():
            if field in ["timeRange", "hours"]:
                continue

            if value:
                es_field = FIELD_MAPPING.get(field, field)
                filter_clauses.append({
                    "terms": {
                        es_field: value
                    }
                })

        log.info("Elastic using standard timeRange + optional filters")

    # --------------------------
    # FINAL QUERY
    # --------------------------
    query_body = {
        "query": {
            "bool": {
                "filter": filter_clauses
            }
        },
        "size": 2500
    }

    log.info(f"Elastic POST body → {query_body}")

    # --------------------------
    # API CALL
    # --------------------------
    api = restapi.RestApi(
        test_config=base_url,
        url_suffix=endpoint,
        username=username,
        password=password,
        cert_path=ca_path,
        payload=query_body
    )

    raw = api.post()

    # --------------------------
    # PARSE RESULTS
    # --------------------------
    try:
        docs = [
            {"id": hit["_id"], **hit["_source"]}
            for hit in raw.get("hits", {}).get("hits", [])
        ]
    except Exception as e:
        pytest.fail(f"Error parsing elastic hits: {e}")

    doc_map = {
        d["id"]: {"elastic_data": d, "cr_id": d.get("id")}
        for d in docs if "id" in d
    }

    return {
        "raw": raw,
        "documents": doc_map
    }

@pytest.fixture(scope="function")
def summary_rest_search_data(test_config, request):
    marker = request.node.get_closest_marker("filters")
    test_filters = marker.kwargs if marker else {}

    log.info(f"REST Filters → {test_filters}")

    base_url = test_config["assistant_endpoint_rest"]
    username = test_config["auth_username"]
    password = test_config["auth_password"]

    endpoint = "/analyzed-changes/search"
    log.info(f"Fetching UI analyzed-changes from: {base_url}{endpoint}")

    # --------------------------
    # CONSTANTS
    # --------------------------
    default_state = [
        "New",
        "Implement",
        "Scheduled",
        "Review",
        "Closed"
    ]

    query_body = {}

    # --------------------------
    # DATE FILTER LOGIC
    # --------------------------
    planned_start_date = test_filters.get("planned_start_date")
    planned_end_date = test_filters.get("planned_end_date")

    if planned_start_date or planned_end_date:
        
        query_body["planned_start_date"] = planned_start_date
        query_body["planned_end_date"] = planned_end_date

    else:
        
        hours = test_filters.get("hours")
        start_dt, end_dt = restapi.calculate_time_range(hours)

        query_body["timeRange"] = {
            "start": start_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "end": end_dt.strftime("%Y-%m-%dT%H:%M:%S.999Z"),
            "useUTC": True,
            "date_field": "planned_start"
        }
        query_body["state"] = test_filters.get("state", default_state)
  
    optional_keys = [
        "number",
        "tribe",
        "service_names",
        "deployment_method",
        "dc",
        "regions"
    ]

    for key in optional_keys:
        val = test_filters.get(key)
        if val:
            query_body[key] = val

    log.info(f"Final POST Body → {query_body}")

    # --------------------------
    # API CALL
    # --------------------------
    api = restapi.RestApi(
        test_config=base_url,
        url_suffix=endpoint,
        username=username,
        password=password,
        payload=query_body
    )

    raw = api.post()

    docs = raw.get("documents", [])
    if not isinstance(docs, list):
        pytest.fail("REST response missing 'documents' list")

    doc_map = {d["id"]: d for d in docs if "id" in d}

    return {
        "raw": raw,
        "documents": doc_map
    }

# ================= Analytics module =========================
@pytest.fixture(scope="function")
def summary_analytics_elastic_data(test_config, request):

    marker = request.node.get_closest_marker("filters")
    test_filters = marker.kwargs if marker else {}
    log.info(f"Elastic Filters → {test_filters}")

    base_url = test_config["assistant_elastic_host"]
    username = test_config["assistant_elastic_user"]
    password = test_config["assistant_elastic_password"]
    index = test_config["assistant_elastic_index"]

    ca_path = restapi.write_ca_cert(
        test_config["assistant_elastic_certificate"]
    )

    endpoint = f"{index}/_search"
    log.info(f"Fetching Elastic analytics from: {base_url}{endpoint}")

    # -------- Time Range --------
    time_range = test_filters.get("timeRange", "7d")
    if not time_range.endswith("d"):
        pytest.fail(f"Invalid timeRange format: {time_range}")

    # -------- Base Query --------
    query_body = {
        "size": 5000,
        "sort": [
            {"created": "asc"}
        ],
        "_source": ["tribe", "created", "state", "analysis_result.final_score"],
        "query": {
            "range": {
                "created": {
                    "gte": f"now-{time_range}/d",
                    "lt": "now/d"
                }
            }
        }
    }

    log.info(f"Elastic POST body → {query_body}")

    # -------- search_after pagination --------
    all_hits = []
    search_after = None

    while True:
        if search_after:
            query_body["search_after"] = search_after
        else:
            query_body.pop("search_after", None)

        api = restapi.RestApi(
            test_config=base_url,
            url_suffix=endpoint,
            username=username,
            password=password,
            cert_path=ca_path,
            payload=query_body
        )

        res = api.post()
        hits = res.get("hits", {}).get("hits", [])

        if not hits:
            break

        all_hits.extend(hits)

        # Get last document's sort values
        search_after = hits[-1]["sort"]

    log.info(f"Elastic total fetched → {len(all_hits)}")

    # -------- Build Aggregations --------
    grouped = {}
    grouped_by_risk = {}

    for hit in all_hits:
        src = hit.get("_source", {})
        tribe = src.get("tribe", "UNKNOWN")
        state = src.get("state", "UNKNOWN")
        score = src.get("analysis_result", {}).get("final_score")

        # ---- grouped (status counts) ----
        grouped.setdefault(tribe, {})
        grouped[tribe][state] = grouped[tribe].get(state, 0) + 1

        # ---- groupedByRisk ----
        if isinstance(score, (int, float)):
            bucket = f"{int(score)}-{int(score)+1}"
            grouped_by_risk.setdefault(tribe, {})
            grouped_by_risk[tribe][bucket] = (
                grouped_by_risk[tribe].get(bucket, 0) + 1
            )

    return {
        "raw": res,
        "total": len(all_hits),
        "grouped": grouped,
        "groupedByRisk": grouped_by_risk
    }

@pytest.fixture(scope="function")
def summary_rest_analytics_data(test_config, request):
    marker = request.node.get_closest_marker("filters")
    test_filters = marker.kwargs if marker else {}

    log.info(f"REST Filters → {test_filters}")

    base_url = test_config["assistant_endpoint_rest"]
    username = test_config["auth_username"]
    password = test_config["auth_password"]

    endpoint = "/all-elastic-documents"
    log.info(f"Fetching backend analytics from: {base_url}{endpoint}")

    # -------- Build payload --------
    time_range = test_filters.get("timeRange", "7d")

    query_body = {
        "timeRange": time_range
    }

    log.info(f"Final POST Body → {query_body}")

    # -------- API Call --------
    api = restapi.RestApi(
        test_config=base_url,
        url_suffix=endpoint,
        username=username,
        password=password,
        payload=query_body
    )

    raw = api.post()

    # -------- Validations --------
    if not isinstance(raw, dict):
        pytest.fail("REST response is not a JSON object")

    for key in ["grouped", "groupedByRisk", "total"]:
        if key not in raw:
            pytest.fail(f"REST response missing '{key}' field")

    if not isinstance(raw["grouped"], dict):
        pytest.fail("'grouped' is not a dictionary")

    if not isinstance(raw["groupedByRisk"], dict):
        pytest.fail("'groupedByRisk' is not a dictionary")

    return {
        "raw": raw,
        "total": raw["total"],
        "grouped": raw["grouped"],
        "groupedByRisk": raw["groupedByRisk"]
    }

# ==================== Heat map module =========================
@pytest.fixture(scope="function")
def cie_rest_data(test_config):

    base_url = test_config["assistant_endpoint_rest"]
    username = test_config["auth_username"]
    password = test_config["auth_password"]

    endpoint = "/cie-check"
    log.info(f"Fetching CIE incidents from {base_url}{endpoint}")

    api = restapi.RestApi(
        test_config=base_url,
        url_suffix=endpoint,
        username=username,
        password=password,
        payload={}
    )

    raw = api.post()

    incidents = raw.get("cieIncidents")
    if not isinstance(incidents, list):
        pytest.fail("Invalid response: cieIncidents missing or not list")

    return {
        "raw": raw,
        "incidents": incidents
    }

@pytest.fixture(scope="function")
def cie_datasync_data(test_config):
    """
    Fetch CIE incidents directly from DataSync using RestApi wrapper
    """

    base_url = test_config["data_sync_endpoint"]
    api_key = test_config["data_sync_api_key"]
   
    endpoint = "/incidents/customQuery"

    body = {
        "fields": [
            "number",
            "severity",
            "service_names",
            "created",
            "tribe",
            "regions",
            "locations",
            "affected_ci_list"
        ],
        "where": (
            "(status='confirmed_cie' OR status='potential_cie') "
            "AND state!='closed' AND state!='resolved' "
            "AND (severity=1 OR severity=2)"
        )
    }

    log.info("Fetching CIE incidents directly from DataSync")
    log.info(f"POST {base_url}{endpoint}")
    log.info(f"Payload → {body}")

    # --------------------------
    # API CALL (USING YOUR WRAPPER)
    # --------------------------
    api = restapi.RestApi(
        test_config=base_url,
        url_suffix=endpoint,
        payload=body
    )

    raw = api.post_db_status(api_key=api_key)

    if not isinstance(raw, list):
        pytest.fail(f"Unexpected DataSync response format: {raw}")

    log.info(f"DataSync incidents count → {len(raw)}")

    doc_map = {
        inc["number"]: inc
        for inc in raw
        if "number" in inc
    }

    return {
        "raw": raw,
        "incidents": raw,
        "documents": doc_map
    }
