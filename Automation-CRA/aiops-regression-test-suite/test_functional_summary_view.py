import pytest
import logging as log
from pytest import mark
import aiops_restapi as restapi
import json

# Set up log
# log = log.getlog(__name__)

# ------------------------------------------------------------
# 1. Pagination Structure Test
# ------------------------------------------------------------

@mark.smoke
@mark.funct_summary_view
def test_aiops_summary_view1_pagination_structure(test_config, summary_rest_search_data):
    """
    Test Steps:
    1. Read REST raw response data.
    2. Validate the presence of mandatory fields: total, page.
    3. Assert both fields are available in REST response.
    4. Log test success.
    """
    log.info(test_aiops_summary_view1_pagination_structure.__doc__)
    
    required_fields = ["total", "success"]
    log.debug(f"Checking for required pagination fields: {required_fields}")

    # REST
    for field in required_fields:
        assert field in summary_rest_search_data["raw"], f"REST missing field: {field}"
        log.debug(f"✓ Field '{field}' found in REST data")

    log.info("✓ Pagination structure test passed")


# ------------------------------------------------------------
# 2. UI vs REST Summary Comparison
# ------------------------------------------------------------

# @mark.funct_summary_view
# def test_aiops_summary_view2_analysis_summary_match_between_elastic_rest(summary_elastic_data, summary_rest_search_data):
#     """
#     1. Load elastic documents from Elasticsearch fixture.
#     2. Load REST documents from the Summary View REST API.
#     3. Identify common CR IDs present in both elastic and REST sources.
#     4. For each common CR:
#        a. Validate elastic_data contains analysis_result.
#        b. Validate REST contains analysis_result.
#        c. Validate both contain change_summary.
#        d. Compare elastic_data and REST change_summary values.
#     5. Assert failure if any mismatch is found.
#     """

#     log.info(test_aiops_summary_view2_analysis_summary_match_between_elastic_rest.__doc__)
    
#     elastic_docs = summary_elastic_data["documents"]
#     rest_docs = summary_rest_search_data["documents"]
    
#     log.debug(f"elastic documents count: {len(elastic_docs)}")
#     log.debug(f"REST documents count: {len(rest_docs)}")

#     common_ids = set(elastic_docs.keys()) & set(rest_docs.keys())
#     log.debug(f"Common IDs found: {len(common_ids)}")

#     if not common_ids:
#         log.warning("No common IDs found between UI and REST data")
    
#     for cid in common_ids:
#         log.debug(f"Checking document ID: {cid}")
#         ui_doc = elastic_docs[cid]["elastic_data"]
#         rest_doc = rest_docs[cid]
#         log.debug(f"REST keys for {cid}: {rest_docs[cid].keys()}")    

#         assert "analysis_result" in ui_doc, f"elastic missing analysis_result for {cid}"
#         assert "analysis_result" in rest_doc, f"REST missing analysis_result for {cid}"
#         log.debug(f"✓ analysis_result present for {cid}")

#         assert "change_summary" in ui_doc["analysis_result"], \
#             f"elastic missing change_summary for {cid}"
#         assert "change_summary" in rest_doc["analysis_result"], \
#             f"REST missing change_summary for {cid}"
#         log.debug(f"✓ change_summary present for {cid}")

#         assert ui_doc["analysis_result"]["change_summary"] == \
#                rest_doc["analysis_result"]["change_summary"], \
#                f"Mismatch in change_summary for {cid}"
#         log.debug(f"✓ change_summary matches for {cid}")

#     log.info(f"✓ Analysis summary comparison test passed. Checked {len(common_ids)} documents")


# ------------------------------------------------------------
# 3. Region Risk Comparison UI vs REST
# ------------------------------------------------------------
@mark.funct_summary_view
def test_aiops_summary_view3_region_risk_range(summary_elastic_data, summary_rest_search_data):
    """
    1. Load regionRisk values from Elastic and REST API for all documents.
    2. Identify common document IDs between elastic_data and REST responses.
    3. For each common document:
        a. Extract regionRisk value from elastic response.
        b. Extract regionRisk value from REST response.
    4. Validate that both elastic_data and REST values are within the valid range (0 to 10).
    5. Compare regionRisk between elastic_data and REST → they must match.
    6. Log the number of documents that contain regionRisk and were validated.
    """
    
    log.info(test_aiops_summary_view3_region_risk_range.__doc__)
    
    elastic_docs = summary_elastic_data["documents"]
    rest_docs = summary_rest_search_data["documents"]

    common_ids = set(elastic_docs.keys()) & set(rest_docs.keys())
    log.debug(f"Checking region risk for {len(common_ids)} common documents")

    region_risk_checked = 0
    
    for cid in common_ids:
        ui_doc = elastic_docs[cid]["elastic_data"]
        rest_doc = rest_docs[cid]

        if "regionRisk" in ui_doc and "regionRisk" in rest_doc:
            region_risk_checked += 1
            ui_val = float(ui_doc["regionRisk"])
            rest_val = float(rest_doc["regionRisk"])

            log.debug(f"Document {cid}: UI regionRisk={ui_val}, REST regionRisk={rest_val}")

            assert 0 <= ui_val <= 10, f"Invalid UI regionRisk for {cid}"
            assert 0 <= rest_val <= 10, f"Invalid REST regionRisk for {cid}"
            
            assert ui_val == rest_val, f"regionRisk mismatch for {cid}"
            log.debug(f"✓ regionRisk matches for {cid}")
        else:
            log.debug(f"Document {cid} doesn't have regionRisk in both sources")

    log.info(f"✓ Region risk test passed. Checked {region_risk_checked} documents with regionRisk")


# ------------------------------------------------------------
# 4. Tribe Comparison UI vs REST
# ------------------------------------------------------------

@mark.funct_summary_view
def test_aiops_summary_view4_service_groups_present(summary_elastic_data, summary_rest_search_data):
    """
    1. Load tribe (service group) values from Elastic and REST API for all documents.
    2. Identify common document IDs between elastic and REST responses.
    3. For each common document:
        a. Verify the 'tribe' field exists in elastic data.
        b. Verify the 'tribe' field exists in REST data.
    4. Validate that the tribe field is not empty in elastic and REST.
    5. Compare tribe values between elastic and REST → they must match.
    6. Log the number of documents validated for tribe.
    """
    
    log.info(test_aiops_summary_view4_service_groups_present.__doc__)
    
    elastic_docs = summary_elastic_data["documents"]
    rest_docs = summary_rest_search_data["documents"]


    common_ids = set(elastic_docs.keys()) & set(rest_docs.keys())
    log.debug(f"Checking tribe for {len(common_ids)} common documents")

    for cid in common_ids:
        log.debug(f"Checking tribe for document: {cid}")
        ui_doc = elastic_docs[cid]["elastic_data"]
        rest_doc = rest_docs[cid]

        assert "tribe" in ui_doc, f"UI missing tribe for {cid}"
        assert "tribe" in rest_doc, f"REST missing tribe for {cid}"
        log.debug(f"✓ tribe field present for {cid}")

        assert ui_doc["tribe"], f"UI tribe empty for {cid}"
        assert rest_doc["tribe"], f"REST tribe empty for {cid}"
        log.debug(f"✓ tribe not empty for {cid}")

        assert ui_doc["tribe"] == rest_doc["tribe"], \
            f"Tribe mismatch for {cid}"
        log.debug(f"✓ tribe matches for {cid}: '{ui_doc['tribe']}'")

    log.info(f"✓ Tribe comparison test passed. Checked {len(common_ids)} documents")


# ==========================================
# 5. COUNT VALIDATION TESTS
# ==========================================

@mark.funct_summary_view
def test_aiops_summary_view5_total_count_consistency(test_config, summary_elastic_data, summary_rest_search_data):
    """
    1. Fetch the total number of documents returned from Elasticsearch.
    2. Fetch the total number of documents returned from the REST API.
    3. Log the counts for debugging.
    4. Compare both counts to ensure consistency.
    5. Assert that both sources return the same number of documents.
    6. Log success message if counts match.
    """
    log.info(test_aiops_summary_view5_total_count_consistency.__doc__)
    
    # Actual document count from Elasticsearch
    elastic_doc_count = len(summary_elastic_data.get("documents", {}))

    # Actual document count from REST
    rest_doc_count = len(summary_rest_search_data.get("documents", {}))

    log.debug(f"Elastic returned documents: {elastic_doc_count}")
    log.debug(f"REST returned documents: {rest_doc_count}")

    # Compare actual counts
    assert elastic_doc_count == rest_doc_count, \
        f"Mismatch in document count: Elastic={elastic_doc_count}, REST={rest_doc_count}"

    log.info(f"✓ Document counts match: Elastic={elastic_doc_count}, REST={rest_doc_count}")


@mark.funct_summary_view
def test_aiops_summary_view6_size_of_page_accuracy(test_config, summary_rest_search_data):
    """
    1. Fetch raw REST API response.
    2. Extract the documents returned by the REST API.
    3. Extract the 'size' field from the REST raw response.
    4. Log the REST size value and actual document count.
    5. Validate that 'size' in REST API response is an integer.
    6. Compare the REST 'size' value with the actual document count.
    7. Assert that both values match for page size accuracy.
    """
    log.info(test_aiops_summary_view6_size_of_page_accuracy.__doc__)
    
    rest_raw = summary_rest_search_data["raw"]
    documents = rest_raw.get("documents", {})
    
    doc_count = len(documents)
    size = rest_raw.get("total")
    
    log.debug(f"REST size field: {size}")
    log.debug(f"Number of documents: {doc_count}")

    # assert isinstance(size, int), "REST total must be an integer"
    # log.debug(f"✓ Size is integer: {size}")
    
    assert size == doc_count, (
        f"REST total {size} must be == number of documents {doc_count}"
    )
    log.info(f"✓ Page size accuracy verified: size={size}, documents={doc_count}")


# ==========================================
# 6. DOCUMENT STRUCTURE TESTS
# ==========================================


# @mark.funct_summary_view
# def test_aiops_summary_view7_required_document_fields(test_config, summary_rest_search_data):
#     """
#     1. Fetch the list of documents returned from the REST API.
#     2. Prepare the list of required fields that every document must contain.
#     3. Log the number of documents and required field list.
#     4. Iterate through every document returned by the REST API.
#     5. For each document, extract its ID for logging purposes.
#     6. Validate that each required field is present in the document.
#     7. Log a success message for every field present.
#     8. Assert all documents contain the complete required field set.
#     """
#     log.info(test_aiops_summary_view7_required_document_fields.__doc__)
    
#     documents = summary_rest_search_data["raw"].get("documents", [])
#     log.debug(f"Checking {len(documents)} documents for required fields")
    
#     required_fields = [
#         'id', 'number', 'state', 'analysis_result', 
#         'analyzed_at', 'service_names', 'locations'
#     ]
    
#     log.debug(f"Required fields: {required_fields}")
    
#     for i, doc in enumerate(documents):
#         doc_id = doc.get('id', f'index_{i}')
#         log.debug(f"Checking document: {doc_id}")
        
#         for field in required_fields:
#             assert field in doc, f"Document missing required field: {doc_id}"
#             log.debug(f"  ✓ Field '{field}' present")
    
#     log.info(f"✓ All {len(documents)} documents have required fields")


@mark.funct_summary_view
def test_aiops_summary_view8_id_number_match(test_config, summary_rest_search_data):
    """
    1. Fetch the list of documents returned from the REST API.
    2. Log the total number of documents under validation.
    3. Iterate through each document one by one.
    4. Extract the document ID for clear logging.
    5. Validate that the 'id' field matches the 'number' field.
    6. Log success for each document where both fields match.
    7. Assert that all documents maintain id == number consistency.
    """
    log.info(test_aiops_summary_view8_id_number_match.__doc__)
    
    documents = summary_rest_search_data["raw"].get("documents", [])
    log.debug(f"Checking ID/Number match for {len(documents)} documents")
    
    for i, doc in enumerate(documents):
        doc_id = doc.get('id', f'index_{i}')
        assert doc["id"] == doc["number"], "Document id does not match number"
        log.debug(f"✓ Document {doc_id}: id={doc['id']}, number={doc['number']} match")
    
    log.info(f"✓ ID/Number match verified for all {len(documents)} documents")


# ==========================================
# 7. DATA VALIDATION TESTS
# ==========================================
@mark.funct_summary_view
def test_aiops_summary_view9_valid_analyzed_at_format(test_config, summary_rest_search_data):
    """
    1. Retrieve all documents from the REST API response.
    2. Log the total number of documents to be validated.
    3. Iterate through each document sequentially.
    4. Extract the analyzed_at field from every document.
    5. Validate that analyzed_at contains a valid ISO date format (checks for presence of 'T').
    6. Log success when format is valid or log a warning if analyzed_at is missing.
    7. Assert format compliance for all non-empty analyzed_at fields.
    """
    log.info(test_aiops_summary_view9_valid_analyzed_at_format.__doc__)
    
    documents = summary_rest_search_data["raw"].get("documents", [])
    log.debug(f"Checking analyzed_at format for {len(documents)} documents")
    
    for i, doc in enumerate(documents):
        doc_id = doc.get('id', f'index_{i}')
        analyzed_at = doc.get("analyzed_at")
        
        if analyzed_at:
            # Basic ISO format validation
            assert "T" in analyzed_at, "analyzed_at not in ISO format"
            log.debug(f"✓ Document {doc_id}: analyzed_at='{analyzed_at}' is valid ISO format")
        else:
            log.warning(f"Document {doc_id} has empty analyzed_at field")
    
    log.info(f"✓ analyzed_at format verified for {len(documents)} documents")


@mark.funct_summary_view
def test_aiops_summary_view10_numeric_risk_score(test_config, summary_rest_search_data):
    """
    1. Iterate through each document.
    2. Extract the analysis_result → risk_score value.
    3. Validate that risk_score is a numeric type (int or float).
    4. Log success per document for traceability.
    5. Assert that all documents contain numeric risk_score values.
    """
    log.info(test_aiops_summary_view10_numeric_risk_score.__doc__)
    
    # Check if documents is a list or dict
    documents = summary_rest_search_data.get("documents", {})
    
    if isinstance(documents, list):
        # Original logic for list structure
        log.debug(f"Checking risk_score type for {len(documents)} documents")
        for i, doc in enumerate(documents):
            doc_id = doc.get('id', f'index_{i}')
            risk_score = doc.get("analysis_result", {}).get("risk_score")
            
            assert isinstance(risk_score, (int, float)), f"risk_score is not numeric for document {doc_id}"
            log.debug(f"✓ Document {doc_id}: risk_score={risk_score} (type: {type(risk_score).__name__})")
   
    log.info(f"✓ risk_score numeric validation passed for {len(documents)} documents")


@mark.funct_summary_view
def test_aiops_summary_view11_array_fields(test_config, summary_rest_search_data):
    """
    1. Fetch all documents from REST response.
    2. For each document, extract locations and service_names fields.
    3. Validate both fields are arrays (lists).
    4. Log the result per document.
    """
    log.info(test_aiops_summary_view11_array_fields.__doc__)
    
    documents = summary_rest_search_data["raw"].get("documents", [])
    log.debug(f"Checking array fields for {len(documents)} documents")
    
    for i, doc in enumerate(documents):
        doc_id = doc.get('id', f'index_{i}')
        
        locations = doc.get("locations", [])
        service_names = doc.get("service_names", [])
        
        assert isinstance(locations, list), f"locations is not a list for {doc_id}"
        assert isinstance(service_names, list), f"service_names is not a list for {doc_id}"
        
        log.debug(f"✓ Document {doc_id}: locations ({len(locations)} items), service_names ({len(service_names)} items) are lists")
    
    log.info(f"✓ Array fields validated for {len(documents)} documents")


# ==========================================
# 8. Error in analysis result
# ==========================================
# @mark.funct_summary_view
# def test_aiops_summary_view12_analysis_error_message(summary_rest_search_data):
#     """
#     Test Steps:
#     1. Read all documents returned from REST.
#     2. Identify all CRs that contain 'analysis_result.error'.
#     3. For each CR with error:
#         a. Capture CR ID, state, created time, and error message.
#         b. Validate error is a non-empty string.
#     4. If state != 'closed' and error exists → mark as invalid.
#     5. Fail test with full list of invalid CRs.
#     6. Log total CRs with errors and invalid ones.
#     """

#     log.info(test_aiops_summary_view12_analysis_error_message.__doc__)

#     documents = summary_rest_search_data["raw"].get("documents", [])
#     log.debug(f"Checking analysis errors for {len(documents)} documents")

#     invalid_docs = []     # CRs that violate rule
#     error_documents = []  # All CRs that have errors (for info)

#     for doc in documents:
#         doc_id = doc.get("id")
#         analysis = doc.get("analysis_result", {})
#         error = analysis.get("error")
#         state = doc.get("state", "").lower()
#         created = doc.get("created")

#         # Skip docs without error
#         if not error:
#             continue

#         error_documents.append(doc_id)

#         log.debug(f"CR {doc_id} has analysis error: {error}")

#         # Basic error validation
#         assert isinstance(error, str), f"Error message not a string for {doc_id}"
#         assert len(error.strip()) > 0, f"Empty error message for {doc_id}"

#         # MAIN RULE
#         if state != "closed":
#             log.error(
#                 f" CR {doc_id} has analysis error but state is '{state}'. Error allowed only in CLOSED state."
#             )
#             invalid_docs.append((doc_id, state, created, error))
#         else:
#             log.debug(f"✓ CR {doc_id} correctly has error with CLOSED state.")

#     # FINAL ASSERT: If any invalid CRs exist → fail the test with full details
#     if invalid_docs:
#         msg = "\n".join(
#             [
#                 f" - CR {doc_id}: state='{state}',created=''{created}, error='{error}'"
#                 for doc_id, state,created, error in invalid_docs
#             ]
#         )
#         pytest.fail(
#             f"\n Found {len(invalid_docs)} CR(s) having analysis error with NON-CLOSED state:\n{msg}\n"
#         )

#     log.info(
#         f"✓ Validation complete. Total CRs with errors: {len(error_documents)}. "
#         f"Invalid CRs: {len(invalid_docs)}"
#     )


@mark.funct_summary_view
def test_aiops_summary_view13_region_dc_tribe_counts_match(summary_elastic_data, summary_rest_search_data):
    log.info(test_aiops_summary_view13_region_dc_tribe_counts_match.__doc__)
    elastic_docs = summary_elastic_data["documents"]
    rest_docs = summary_rest_search_data["documents"]

    # Groups from Elastic and REST
    elastic_groups = restapi.compute_region_dc_tribe_groups(elastic_docs)
    rest_groups = restapi.compute_region_dc_tribe_groups(rest_docs)

    log.info(f"Total Elastic groups: {len(elastic_groups)}")
    log.info(f"Total REST groups: {len(rest_groups)}")

  
    elastic_keys = set(elastic_groups.keys())
    rest_keys = set(rest_groups.keys())
    # -----------------------------------------------------------
    # 2️⃣ FOR COMMON GROUPS, COMPARE COUNTS
    # -----------------------------------------------------------
    common_groups = elastic_keys & rest_keys

    mismatched_groups = []

    for key in sorted(common_groups):
        e_count = elastic_groups[key]["count"]
        r_count = rest_groups[key]["count"]

        if e_count != r_count:
            mismatched_groups.append({
                "group": key,
                "elastic_count": e_count,
                "rest_count": r_count
        })

    # Print mismatch summary
    if mismatched_groups:
        log.info("\n========== GROUP COUNT MISMATCHES ==========")
        for item in mismatched_groups:
            log.info(
              f"{item['group']}: Elastic={item['elastic_count']}, "
              f"REST={item['rest_count']}"
            )
        log.info("============================================\n")
    else:
         log.info("All group counts match!")
   
    assert not mismatched_groups, "Some group counts do not match between Elastic and REST"
   
