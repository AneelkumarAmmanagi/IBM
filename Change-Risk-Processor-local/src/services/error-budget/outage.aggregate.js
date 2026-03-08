const { client } = require("../../databaseSetup/elasticsearchConfig");

async function fetchCurrentMonthOutages() {
  const now = new Date();
  const year = now.getFullYear();
  const month = now.toLocaleString("default", { month: "short" });
  const docId = `${month}${year}`;

  let indexName = process.env.OUTAGE_ANALYSIS_INDEX || "change-risk-outages";
  try {
    const result = await client.get({
      index: indexName,
      id: docId,
    });

    return result?._source?.services;
  } catch (error) {
    if (error.meta && error.meta.statusCode === 404) {
      console.log("Document not found");
    } else {
      console.error("Error fetching document:", error);
    }
    return null;
  }
}

module.exports = { fetchCurrentMonthOutages };
