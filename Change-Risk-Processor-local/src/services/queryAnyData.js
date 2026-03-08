const { client } = require("../databaseSetup/elasticsearchConfig");

async function queryAnyElasticDBData(queryBody) {
  if (!queryBody?.body) {
    throw new Error("Missing required fields in queryBody");
  }


  try {
    const result = await client.search({
      index: process.env.ELASTIC_INDEX,
      body: queryBody.body,
    });
    return result?.hits?.hits;
  } catch (error) {
    console.error("Error querying Elasticsearch:", error);
    throw error;
  }
}

module.exports = { queryAnyElasticDBData };
