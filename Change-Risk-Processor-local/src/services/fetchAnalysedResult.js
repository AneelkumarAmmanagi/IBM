const { client } = require("../databaseSetup/elasticsearchConfig");

async function fetchAnalysedResult(changeId) {
  const index = process.env.ELASTIC_INDEX || "change-risk-analysis";
  try {
    // Check if document exists
    const exists = await client.exists({
      index,
      id: changeId,
    });
    if (!exists) {
      return {
        error: `Document with change id ${changeId} not found in database`,
        resultResponse: null,
      };
    }
    // Fetch the document
    const doc = await client.get({
      index,
      id: changeId,
    });
    const analysis_result = doc._source && doc._source.analysis_result;
    if (!analysis_result) {
      return {
        error: `analysis_result not found for change id ${changeId}`,
        resultResponse: null,
      };
    }
    return { error: null, resultResponse: analysis_result };
  } catch (error) {
    return {
      error: error.message || "Unknown error occurred",
      resultResponse: null,
    };
  }
}

async function fetchMultipleAnalysedResults(changeIds) {
  const index = process.env.ELASTIC_INDEX || "change-risk-analysis";

  try {
    const { docs } = await client.mget({
      index,
      body: {
        ids: changeIds,
      },
    });

    return docs.map((doc, i) => {
      if (!doc.found) {
        return {
          changeId: changeIds[i],
          analysis_result: null,
          error: `Document with change id ${changeIds[i]} not found`,
        };
      }

      const analysis_result = doc._source?.analysis_result;
      return {
        changeId: doc._id,
        analysis_result: analysis_result || null,
        error: analysis_result
          ? null
          : `analysis_result not found for change id ${doc._id}`,
      };
    });
  } catch (error) {
    return changeIds.map((id) => ({
      changeId: id,
      analysis_result: null,
      error: error.message || "Unknown error occurred",
    }));
  }
}

module.exports = { fetchAnalysedResult, fetchMultipleAnalysedResults };
