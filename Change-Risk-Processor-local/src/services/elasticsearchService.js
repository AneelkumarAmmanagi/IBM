const { regionFromCrnMask } = require("../../constants/constants");
const { client } = require("../databaseSetup/elasticsearchConfig");

async function checkElasticsearchConnection() {
  console.log("Checking Elasticsearch connection...");
  try {
    await client.ping();
    console.log("Successfully connected to Elasticsearch");
    return true;
  } catch (error) {
    console.error("Error connecting to Elasticsearch:", error?.message);
    console.error("Full error details:", error?.message);
    return false;
  }
}

async function checkAndCreateIndex() {
  const indexName = process.env.ELASTIC_INDEX || "change-risk-analysis";
  console.log(`Checking if index '${indexName}' exists...`);
  try {
    const indexExists = await client.indices.exists({ index: indexName });
    console.log(`Index existence check result: ${indexExists}`);

    if (!indexExists) {
      console.log(`Creating index '${indexName}' with settings:`, {
        shards: 1,
        replicas: 1,
      });
      await client.indices.create({
        index: indexName,
        settings: {
          number_of_shards: 1,
          number_of_replicas: 1,
        },
      });
      console.log(`Index '${indexName}' created successfully`);
    } else {
      console.log(`Index '${indexName}' already exists`);
    }
    return true;
  } catch (error) {
    console.error(
      `Error in checkAndCreateIndex for '${indexName}':`,
      error.message
    );
    console.error("Full error stack:", error?.message);
    return false;
  }
}

async function saveToElasticsearch(data) {
  const indexName = process.env.ELASTIC_INDEX || "change-risk-analysis";
  console.log(
    `Attempting to save change request ${data?.number} to index '${indexName}'`
  );
  try {
    console.log(`Preparing data for indexing:`, {
      changeNumber: data?.number,
      state: data?.state,
      type: data?.type,
    });

    const crn_mask_details = data?.crn_mask.split(":");
    const region = regionFromCrnMask[crn_mask_details[5]];
    const config_name = crn_mask_details[4];
    const datacenter_name = crn_mask_details[5];

    const enrichedData = {
      ...data,
      region: region,
      config_name: config_name,
      datacenter: datacenter_name,
      indexed_at: new Date().toISOString(),
    };

    console.log(`Indexing document with ID: ${data.number}`);
    const result = await client.index({
      index: indexName,
      document: enrichedData,
      id: data.number,
    });

    console.log(`Successfully indexed change request ${data.number}:`, {
      id: result._id,
      version: result._version,
      result: result.result,
    });
    return result;
  } catch (error) {
    console.error(
      `Failed to index change request ${data?.number}:`,
      error.message
    );
    console.error("Full indexing error details:", error?.message);
    throw {
      message: error.message,
      stack: error.stack,
    };
  }
}

module.exports = {
  saveToElasticsearch,
  checkElasticsearchConnection,
  checkAndCreateIndex,
};
