const { client } = require("../../databaseSetup/elasticsearchConfig");

const fetchingRiskyCRs = async () => {
  const indexName = process.env.ELASTIC_INDEX || "change-risk-analysis";
  try {
    const result = await client.search({
      index: indexName,
      body: {
        query: {
          bool: {
            must: [
              {
                range: { "analysis_result.final_score": { gte: 7 } },
              },
            ],
            must_not: [
              { wildcard: { "regions.keyword": "*(syd|'')*" } },
              { term: { archived: true } },
            ],
          },
        },
      },
      size: 30,
    });

    const hits = result?.hits?.hits;
    if (!hits || hits.length === 0) {
      console.log("No records found with final_score >= 7.");
      return [null, []];
    }

    const records = hits.map((hit) => ({
      id: hit._id,
      name: hit._source?.name || "Unknown",
      final_score: hit._source?.analysis_result?.final_score || "N/A",
      tribe: hit._source?.tribe || "Unknown",
      service_names: hit._source?.service_names || "Unknown",
      regions: hit._source?.regions || "Unknown",
      dc: hit._source?.dc || "Unknown",
    }));

    if (records.length === 0) {
      console.log("No records found with final_score >= 7.");
      return [null, []];
    } else {
      console.log("Returning the records fetched");
      return [null, records];
    }
  } catch (error) {
    console.error("Error fetching data or sending to Slack:", error);
    return [error?.message, null];
  }
};

module.exports = {
  fetchingRiskyCRs,
};
