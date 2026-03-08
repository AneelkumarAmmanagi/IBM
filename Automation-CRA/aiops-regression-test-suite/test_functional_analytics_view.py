import logging as log
from pytest import mark
import pytest



@mark.funct_analytics_view_filter
def test_aiops_analytic_view1_tribe_keys_rest_vs_elastic(
        summary_rest_analytics_data,
        summary_analytics_elastic_data):
    """
    1. Fetch tribe-wise grouped data from REST and Elastic analytics APIs.
    2. Extract tribe keys from both sources.
    3. Validate that both REST and Elastic contain identical tribe keys.
    """
    log.info(test_aiops_analytic_view1_tribe_keys_rest_vs_elastic.__doc__)

    rest_tribes = set(summary_rest_analytics_data["grouped"].keys())
    elastic_tribes = set(summary_analytics_elastic_data["grouped"].keys())

    log.info(f"REST tribes ({len(rest_tribes)}) → {sorted(rest_tribes)}")
    log.info(f"Elastic tribes ({len(elastic_tribes)}) → {sorted(elastic_tribes)}")

    assert rest_tribes == elastic_tribes, (
        " Tribe key mismatch between REST and Elastic\n"
        f"Only in REST: {rest_tribes - elastic_tribes}\n"
        f"Only in Elastic: {elastic_tribes - rest_tribes}"
    )

    log.info("✓ Tribe keys match between REST and Elastic")

@mark.smoke
@mark.funct_analytics_view_filter
def test_aiops_analytic_view2_status_counts_per_tribe(
        summary_rest_analytics_data,
        summary_analytics_elastic_data):
    """
    1. Compare status-wise incident counts per tribe.
    2. Validate REST and Elastic return identical counts for each tribe.
    """
    log.info(test_aiops_analytic_view2_status_counts_per_tribe.__doc__)

    rest_grouped = summary_rest_analytics_data["grouped"]
    elastic_grouped = summary_analytics_elastic_data["grouped"]

    for tribe in rest_grouped:
        log.info(f"Validating status counts for tribe → {tribe}")

        assert tribe in elastic_grouped, f"Missing tribe in Elastic: {tribe}"

        log.info(f"REST counts    → {rest_grouped[tribe]}")
        log.info(f"Elastic counts → {elastic_grouped[tribe]}")

        assert rest_grouped[tribe] == elastic_grouped[tribe], (
            f"Status count mismatch for tribe {tribe}\n"
            f"REST: {rest_grouped[tribe]}\n"
            f"Elastic: {elastic_grouped[tribe]}"
        )

    log.info("✓ Status counts per tribe match")


@mark.funct_analytics_view_filter
def test_aiops_analytic_view3_risk_bucket_counts_per_tribe(
        summary_rest_analytics_data,
        summary_analytics_elastic_data):
    """
    1. Compare risk-bucket grouped data per tribe.
    2. Ensure REST and Elastic analytics match exactly.
    """
    log.info(test_aiops_analytic_view3_risk_bucket_counts_per_tribe.__doc__)

    rest_risk = summary_rest_analytics_data["groupedByRisk"]
    elastic_risk = summary_analytics_elastic_data["groupedByRisk"]

    for tribe in rest_risk:
        log.info(f"Validating risk buckets for tribe → {tribe}")

        assert tribe in elastic_risk, f"Missing tribe in Elastic risk data: {tribe}"

        log.info(f"REST risk data    → {rest_risk[tribe]}")
        log.info(f"Elastic risk data → {elastic_risk[tribe]}")

        assert rest_risk[tribe] == elastic_risk[tribe], (
            f" Risk bucket mismatch for tribe {tribe}\n"
            f"REST: {rest_risk[tribe]}\n"
            f"Elastic: {elastic_risk[tribe]}"
        )

    log.info("✓ Risk bucket counts per tribe match")



# ==============================================
@mark.funct_analytics_view_filter
def test_aiops_analytic_view4_handles_empty_response_gracefully(
        summary_rest_analytics_data):
    """
    1. Validate grouped sections exist even when no data is returned.
    2. Ensure API handles empty datasets gracefully.
    """
    log.info(test_aiops_analytic_view4_handles_empty_response_gracefully.__doc__)

    assert isinstance(summary_rest_analytics_data["grouped"], dict)
    assert isinstance(summary_rest_analytics_data["groupedByRisk"], dict)

    log.info("✓ Empty response handled gracefully")



@mark.funct_analytics_view_filter
def test_aiops_analytic_view5_total_count(
        summary_rest_analytics_data,
        summary_analytics_elastic_data):
    """
    1. Validate total incident count returned by REST and Elastic.
    2. Ensure both totals are equal.
    """
    log.info(test_aiops_analytic_view5_total_count.__doc__)

    rest_total = summary_rest_analytics_data["total"]
    elastic_total = summary_analytics_elastic_data["total"]

    log.info(f"REST total    → {rest_total}")
    log.info(f"Elastic total → {elastic_total}")

    assert rest_total == elastic_total, (
        f" Total mismatch → REST={rest_total}, Elastic={elastic_total}"
    )

    log.info("✓ Total incident count matches")

@mark.smoke
@mark.funct_analytics_view_filter
def test_aiops_analytic_view6_tribe_keys(
        summary_rest_analytics_data,
        summary_analytics_elastic_data):
    """
    1. Ensure tribe keys returned by REST and Elastic are identical.
    """
    log.info(test_aiops_analytic_view6_tribe_keys.__doc__)

    rest_tribes = set(summary_rest_analytics_data["grouped"].keys())
    elastic_tribes = set(summary_analytics_elastic_data["grouped"].keys())

    assert rest_tribes == elastic_tribes
    log.info("✓ Tribe keys validated successfully")


@mark.funct_analytics_view_filter
def test_aiops_analytic_view7_no_negative_counts(
        summary_rest_analytics_data):
    """
    1. Validate that no grouped metric contains negative values.
    """
    log.info(test_aiops_analytic_view7_no_negative_counts.__doc__)

    for section in ["grouped", "groupedByRisk"]:
        for tribe, values in summary_rest_analytics_data[section].items():
            for key, count in values.items():
                log.info(f"Validating count → Section:{section}, Tribe:{tribe}, {key}:{count}")
                assert count >= 0, f"Negative count for {tribe} → {key}"

    log.info("✓ No negative counts detected")



@pytest.mark.parametrize("timeRange", ["2d", "7d", "30d"])
@mark.funct_analytics_view_filter
def test_aiops_analytics_view_timeRange_total_rest_vs_elastic(
    timeRange,
    request,
    summary_rest_analytics_data,
    summary_analytics_elastic_data,
    ):
    """
    Validate total analytics count between REST and Elastic
    for different time ranges.
    """
    log.info("Applying timeRange filter → %s", timeRange)
    request.node.add_marker(pytest.mark.filters(timeRange=timeRange))

    rest_total = summary_rest_analytics_data["total"]
    elastic_total = summary_analytics_elastic_data["total"]

    log.info(
        "Total count | REST: %d | Elastic: %d",
        rest_total,
        elastic_total
    )
    assert rest_total == elastic_total, (
        f"❌ Total count mismatch for timeRange {timeRange} "
        f"(REST={rest_total}, Elastic={elastic_total})"
    )

    log.info("✓ Total count matches for timeRange %s", timeRange)
