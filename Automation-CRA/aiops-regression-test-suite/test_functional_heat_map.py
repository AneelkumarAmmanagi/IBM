import logging as log
import json
from pytest import mark
import aiops_restapi as restapi

@mark.funct_heat_map_cie
def test_aiops_cie1_response_structure(cie_rest_data):
    """
    1. Validate backend CIE API response contains 'cieIncidents'.
    2. Ensure 'cieIncidents' is a list.
    3. Log total incident count returned by backend.
    """
    log.info(test_aiops_cie1_response_structure.__doc__)

    raw = cie_rest_data["raw"]

    assert "cieIncidents" in raw, "'cieIncidents' key missing in response"
    assert isinstance(raw["cieIncidents"], list), "'cieIncidents' is not a list"

    count = len(raw["cieIncidents"])
    log.info(f"✓ Backend returned {count} CIE incidents")
    print(f"Backend incident count → {count}")

@mark.funct_heat_map_cie
def test_aiops_cie2_mandatory_fields(cie_rest_data):
    """
    1. Validate every CIE incident contains all mandatory fields.
    2. Fail immediately if any incident is missing required attributes.
    """
    log.info(test_aiops_cie2_mandatory_fields.__doc__)

    required_fields = {
        "number", "severity", "service_names",
        "created", "tribe", "regions",
        "locations", "affected_ci_list"
    }

    for inc in cie_rest_data["incidents"]:
        log.info(f"Validating fields for incident → {inc.get('number')}")
        missing = required_fields - inc.keys()

        if missing:
            restapi.pretty_print("INCIDENT WITH MISSING FIELDS", inc)
            pytest.fail(f"Missing fields {missing} in incident {inc.get('number')}")

    log.info("✓ All incidents contain mandatory fields")


@mark.funct_heat_map_cie
def test_aiops_cie3_severity_validation(cie_rest_data):
    """
    1. Validate severity field for each CIE incident.
    2. Allowed values are only Severity 1 or 2.
    """
    log.info(test_aiops_cie3_severity_validation.__doc__)

    for inc in cie_rest_data["incidents"]:
        sev = inc.get("severity")
        number = inc.get("number")

        log.info(f"Incident {number} → severity={sev}")
        assert sev in ("1", "2"), f"Invalid severity {sev} in {number}"

    log.info("✓ Severity validation passed for all incidents")



@mark.funct_heat_map_cie
def test_aiops_cie4_json_fields_parsing(cie_rest_data):
    """
    1. Validate JSON-encoded fields are parsable.
    2. Ensure parsed value is a list for each JSON field.
    """
    log.info(test_aiops_cie4_json_fields_parsing.__doc__)

    json_fields = [
        "service_names",
        "regions",
        "locations",
        "affected_ci_list"
    ]

    for inc in cie_rest_data["incidents"]:
        log.info(f"Parsing JSON fields for incident → {inc['number']}")

        for field in json_fields:
            try:
                parsed = json.loads(inc[field])
                assert isinstance(parsed, list)
            except Exception:
                restapi.pretty_print("INVALID JSON FIELD", inc)
                pytest.fail(f"Invalid JSON in field '{field}' for {inc['number']}")

    log.info("✓ All JSON fields parsed successfully")


@mark.funct_heat_map_cie
def test_aiops_cie5_group_by_tribe(cie_rest_data):
    """
    1. Group backend incidents by tribe.
    2. Validate grouping result is not empty.
    """
    log.info(test_aiops_cie5_group_by_tribe.__doc__)

    grouped = {}
    for inc in cie_rest_data["incidents"]:
        tribe = inc.get("tribe", "Unknown")
        grouped[tribe] = grouped.get(tribe, 0) + 1

    restapi.pretty_print("BACKEND – GROUPED BY TRIBE", grouped)

    assert grouped, "Tribe grouping result is empty"
    log.info("✓ Backend tribe grouping successful")


@mark.funct_heat_map_cie
def test_aiops_cie6_no_duplicate_incidents(cie_rest_data):
    """
    1. Validate there are no duplicate incident numbers.
    """
    log.info(test_aiops_cie6_no_duplicate_incidents.__doc__)

    numbers = [i["number"] for i in cie_rest_data["incidents"]]
    duplicates = {n for n in numbers if numbers.count(n) > 1}

    if duplicates:
        restapi.pretty_print("DUPLICATE INCIDENTS FOUND", list(duplicates))
        pytest.fail(f"Duplicate incident numbers detected: {duplicates}")

    log.info("✓ No duplicate incident numbers found")



# ////////////////////////=====================================================================
@mark.funct_heat_map_cie
def test_aiops_cie7_count_backend_vs_datasync(
    cie_rest_data,
    cie_datasync_data
):
    """
    1. Compare total incident count between Backend and DataSync.
    2. Counts must match exactly.
    """
    log.info(test_aiops_cie7_count_backend_vs_datasync.__doc__)

    backend_count = len(cie_rest_data["incidents"])
    datasync_count = len(cie_datasync_data["incidents"])

    log.info(f"Backend count  → {backend_count}")
    log.info(f"DataSync count → {datasync_count}")

    assert backend_count == datasync_count, (
        f"Count mismatch → Backend:{backend_count}, DataSync:{datasync_count}"
    )

    log.info("✓ Backend vs DataSync incident count matches")



@mark.funct_heat_map_cie
def test_aiops_cie8_group_by_tribe_backend_vs_datasync(
    cie_rest_data,
    cie_datasync_data
):
    """
    1. Group incidents by tribe from Backend and DataSync.
    2. Validate tribe-wise counts are identical.
    """
    log.info(test_aiops_cie8_group_by_tribe_backend_vs_datasync.__doc__)

    def group_by_tribe(incidents):
        grouped = {}
        for inc in incidents:
            tribe = inc.get("tribe", "Unknown")
            grouped[tribe] = grouped.get(tribe, 0) + 1
        return grouped

    backend_grouped = group_by_tribe(cie_rest_data["incidents"])
    datasync_grouped = group_by_tribe(cie_datasync_data["incidents"])

    restapi.pretty_print("BACKEND – GROUPED BY TRIBE", backend_grouped)
    restapi.pretty_print("DATASYNC – GROUPED BY TRIBE", datasync_grouped)

    assert backend_grouped == datasync_grouped, "Tribe grouping mismatch"
    log.info("✓ Backend vs DataSync tribe grouping matches")


@mark.funct_heat_map_cie
def test_aiops_cie9_incident_numbers_backend_vs_datasync(
    cie_rest_data,
    cie_datasync_data
):
    """
    1. Compare incident numbers from Backend and DataSync.
    2. Ensure no missing or extra incidents on either side.
    """
    log.info(test_aiops_cie9_incident_numbers_backend_vs_datasync.__doc__)

    backend_ids = {i["number"] for i in cie_rest_data["incidents"]}
    datasync_ids = {i["number"] for i in cie_datasync_data["incidents"]}

    only_backend = backend_ids - datasync_ids
    only_datasync = datasync_ids - backend_ids

    if only_backend or only_datasync:
        log.error(f"Only in Backend  → {only_backend}")
        log.error(f"Only in DataSync → {only_datasync}")

    assert backend_ids == datasync_ids, "Incident ID mismatch detected"
    log.info("✓ Backend vs DataSync incident IDs match")

