import pytest
import logging as log
from pytest import mark
import aiops_restapi as restapi
from datetime import datetime, timedelta, timezone

@mark.funct_detailed_view_filter
def test_aiops_detailed_view1_search_timerange(summary_rest_search_data):
    """
    1. Validate every document's planned_start lies within the requested timeRange.
    2. TimeRange may be default or user-specified.
    """
    log.info(test_aiops_detailed_view1_search_timerange.__doc__)

    raw = summary_rest_search_data["raw"]
    docs = raw["documents"]
    timerange = raw.get("timeRange", None)

    if not timerange:
        log.warning("No timeRange returned by API; skipping validation")
        return

    start = timerange["start"]
    end = timerange["end"]

    for doc in docs:
        ps = doc.get("planned_start")
        assert ps, f"Document missing planned_start: {doc.get('id')}"
        assert start <= ps <= end, \
            f"planned_start {ps} not within expected range {start} → {end}"

    log.info("✓ TimeRange filtering validation passed")

@mark.smoke
@mark.funct_detailed_view_filter
def test_aiops_detailed_view2_search_filters(summary_rest_search_data):
    """
    Validate that the documents returned match applied filters.
    state, regions, dc, tribe, service_names, deployment_method.
    """
    log.info(test_aiops_detailed_view2_search_filters.__doc__)

    raw = summary_rest_search_data["raw"]
    docs = raw["documents"]

    applied_filters = raw.get("appliedFilters", {})   # backend returns this

    for doc in docs:
        # State
        doc_id = doc.get(id)
        log.info(f"Validating document: {doc_id}")

        if "state" in applied_filters:
            log.info(
                f"State check | Doc: {doc.get('state')} | Filter: {applied_filters['state']}"
            )
            assert doc["state"] in applied_filters["state"], \
                f"State mismatch: {doc['id']}"

        # Region
        if "regions" in applied_filters:
            log.info(
                f"Regions check | Doc: {doc.get('regions')} | Filter: {applied_filters['regions']}"
            )
            assert any(r in applied_filters["regions"] for r in doc.get("regions", [])), \
                f"Region mismatch for {doc['id']}"

        # DC
        if "dc" in applied_filters:
            assert any(dc in applied_filters["dc"] for dc in doc.get("dc", [])), \
                f"DC mismatch for {doc['id']}"

        # Tribe
        if "tribe" in applied_filters:
            log.info(
                f"Tribe check | Doc: {doc.get('tribe')} | Filter: {applied_filters['tribe']}"
            )
            assert doc.get("tribe") in applied_filters["tribe"], \
                f"Tribe mismatch for {doc['id']}"

        # Service Names
        if "service_names" in applied_filters:
            assert any(s in applied_filters["service_names"] for s in doc.get("service_names", [])), \
                f"Service Name mismatch for {doc['id']}"

        # Deployment Method
        if "deployment_method" in applied_filters:
            log.info(
                f"Deployment method check | Doc: {doc.get('deployment_method')} | "
                f"Filter: {applied_filters['deployment_method']}"
            )
            assert doc.get("deployment_method") in applied_filters["deployment_method"], \
                f"Deployment method mismatch for {doc['id']}"

    log.info("✓ Filter validation passed")


@mark.funct_detailed_view_filter
def test_aiops_detailed_view3_search_total_count(summary_rest_search_data):
    log.info(f"Keys in summary_rest_search_data: {summary_rest_search_data.keys()}")
    
    """
    Validate that 'total' matches actual number of documents returned.
    """
    log.info(test_aiops_detailed_view3_search_total_count.__doc__)

    raw = summary_rest_search_data["raw"]
    log.info(f"Raw keys: {raw.keys()}")

    total = raw.get("total", 0)
    doc_count = len(raw.get("documents", []))

    log.info(f"Total from response: {total}")
    log.info(f"Number of documents returned: {doc_count}")

    assert raw["total"] == len(raw["documents"]), \
        f"Total={raw['total']} but returned {len(raw['documents'])} docs"

    log.info("✓ Total count validation passed")

@pytest.mark.filters(regions=["us-south"])
@mark.funct_detailed_view_filter
def test_aiops_detailed_view4_region_filter_rest_vs_elastic(summary_rest_search_data,summary_elastic_data):

    log.info(test_aiops_detailed_view4_region_filter_rest_vs_elastic.__doc__)
    rest_docs = summary_rest_search_data["documents"]
    elastic_docs = summary_elastic_data["documents"]

    rest_ids = set(rest_docs.keys())
    elastic_ids = set(elastic_docs.keys())

    log.info(f"REST count: {len(rest_ids)}")
    log.info(f"Elastic count: {len(elastic_ids)}")

    assert rest_ids == elastic_ids, (
        f"Mismatch between REST and Elastic results\n"
        f"Only in REST: {rest_ids - elastic_ids}\n"
        f"Only in Elastic: {elastic_ids - rest_ids}"
    )


@pytest.mark.filters(tribe=["Data Center Operations"])
@mark.funct_detailed_view_filter
def test_aiops_detailed_view5_tribe_filter_rest_vs_elastic(summary_rest_search_data,summary_elastic_data):

    log.info(test_aiops_detailed_view5_tribe_filter_rest_vs_elastic.__doc__)
    rest_docs = summary_rest_search_data["documents"]
    elastic_docs = summary_elastic_data["documents"]

    rest_ids = set(rest_docs.keys())
    elastic_ids = set(elastic_docs.keys())

    log.info(f"REST count: {len(rest_ids)}")
    log.info(f"Elastic count: {len(elastic_ids)}")

    assert rest_ids == elastic_ids, (
        f"Mismatch between REST and Elastic results\n"
        f"Only in REST: {rest_ids - elastic_ids}\n"
        f"Only in Elastic: {elastic_ids - rest_ids}"
    )


@pytest.mark.filters(service_names=["is-fleet"])
@mark.smoke
@mark.funct_detailed_view_filter
def test_aiops_detailed_view6_service_names_filter_rest_vs_elastic(summary_rest_search_data,summary_elastic_data):

    log.info(test_aiops_detailed_view6_service_names_filter_rest_vs_elastic.__doc__)
    rest_docs = summary_rest_search_data["documents"]
    elastic_docs = summary_elastic_data["documents"]

    rest_ids = set(rest_docs.keys())
    elastic_ids = set(elastic_docs.keys())

    log.info(f"REST count: {len(rest_ids)}")
    log.info(f"Elastic count: {len(elastic_ids)}")

    assert rest_ids == elastic_ids, (
        f"Mismatch between REST and Elastic results\n"
        f"Only in REST: {rest_ids - elastic_ids}\n"
        f"Only in Elastic: {elastic_ids - rest_ids}"
    )

@pytest.mark.filters(state=["New"])
@mark.funct_detailed_view_filter
def test_aiops_detailed_view7_state_filter_rest_vs_elastic(summary_rest_search_data,summary_elastic_data):

    log.info(test_aiops_detailed_view6_service_names_filter_rest_vs_elastic.__doc__)
    rest_docs = summary_rest_search_data["documents"]
    elastic_docs = summary_elastic_data["documents"]

    rest_ids = set(rest_docs.keys())
    elastic_ids = set(elastic_docs.keys())

    log.info(f"REST count: {len(rest_ids)}")
    log.info(f"Elastic count: {len(elastic_ids)}")

    assert rest_ids == elastic_ids, (
        f"Mismatch between REST and Elastic results\n"
        f"Only in REST: {rest_ids - elastic_ids}\n"
        f"Only in Elastic: {elastic_ids - rest_ids}"
    )    


@pytest.mark.filters(dc=["in-che"])
@mark.funct_detailed_view_filter
def test_aiops_detailed_view8_dc_filter_rest_vs_elastic(summary_rest_search_data,summary_elastic_data):

    log.info(test_aiops_detailed_view8_dc_filter_rest_vs_elastic.__doc__)
    rest_docs = summary_rest_search_data["documents"]
    elastic_docs = summary_elastic_data["documents"]

    rest_ids = set(rest_docs.keys())
    elastic_ids = set(elastic_docs.keys())

    log.info(f"REST count: {len(rest_ids)}")
    log.info(f"Elastic count: {len(elastic_ids)}")

    assert rest_ids == elastic_ids, (
        f"Mismatch between REST and Elastic results\n"
        f"Only in REST: {rest_ids - elastic_ids}\n"
        f"Only in Elastic: {elastic_ids - rest_ids}"
    )    

@pytest.mark.filters(regions=["us-south"],hours=10)
@mark.smoke
@mark.funct_detailed_view_filter
def test_aiops_detailed_view9_timerange_rest_vs_elastic(summary_rest_search_data,summary_elastic_data):

    log.info(test_aiops_detailed_view8_dc_filter_rest_vs_elastic.__doc__)
    rest_docs = summary_rest_search_data["documents"]
    elastic_docs = summary_elastic_data["documents"]

    rest_ids = set(rest_docs.keys())
    elastic_ids = set(elastic_docs.keys())

    log.info(f"REST count: {len(rest_ids)}")
    log.info(f"Elastic count: {len(elastic_ids)}")

    assert rest_ids == elastic_ids, (
        f"Mismatch between REST and Elastic results\n"
        f"Only in REST: {rest_ids - elastic_ids}\n"
        f"Only in Elastic: {elastic_ids - rest_ids}"
    )    

@pytest.mark.filters(planned_start_date="12/21/2025",planned_end_date="12/21/2025")
@mark.funct_detailed_view_filter
def test_aiops_detailed_view10_planned_start_end_rest_vs_elastic(summary_rest_search_data,summary_elastic_data):

    log.info(test_aiops_detailed_view10_planned_start_end_rest_vs_elastic.__doc__)
    rest_docs = summary_rest_search_data["documents"]
    elastic_docs = summary_elastic_data["documents"]

    rest_ids = set(rest_docs.keys())
    elastic_ids = set(elastic_docs.keys())

    log.info(f"REST count: {len(rest_ids)}")
    log.info(f"Elastic count: {len(elastic_ids)}")

    assert rest_ids == elastic_ids, (
        f"Mismatch between REST and Elastic results\n"
        f"Only in REST: {rest_ids - elastic_ids}\n"
        f"Only in Elastic: {elastic_ids - rest_ids}"
    )      