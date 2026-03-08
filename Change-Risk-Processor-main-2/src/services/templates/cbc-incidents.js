const { client } = require("../../databaseSetup/elasticsearchConfig.js");

const pushToElastic = async (incidents, indexName = 'cbc_incidents') => {
    console.log("Pushing incidents to Elasticsearch");
  if (!Array.isArray(incidents) || incidents.length === 0) {
    console.log("No incidents to push to Elasticsearch.");
    return;
  }

  try {
    if (!Array.isArray(incidents) || incidents.length === 0) {
      console.log("No incidents to push.");
      return;
    }
  
    const bulkBody = incidents.flatMap(doc => [
      { create: { _index: indexName, _id: doc.number } },
      doc
    ]);
  
    const { body: bulkResponse } = await client.bulk({ refresh: true, body: bulkBody });
  
    if (bulkResponse?.errors) {
      const conflicts = [];
      const otherErrors = [];
  
      bulkResponse.items.forEach((action, i) => {
        const operation = Object.keys(action)[0];
        if (action[operation].error) {
          if (action[operation].error.type === "version_conflict_engine_exception") {
            conflicts.push(incidents[i].number);
          } else {
            otherErrors.push({
              status: action[operation].status,
              error: action[operation].error,
              document: incidents[i]
            });
          }
        }
      });
  
      if (conflicts.length) {
        console.log(`⚠️ Skipped ${conflicts.length} existing documents.`);
      }
      if (otherErrors.length) {
        console.error("❌ Some documents failed to index:", otherErrors);
      }
    } else {
      console.log(`✅ Successfully indexed ${incidents.length} documents to "${indexName}"`);
    }
  
  } catch (error) {
    console.error("Error pushing data to Elasticsearch:", error);
    throw error;
  }
  
}



const cbcIncidents = async () => {
  const url = `${process.env.DATASYNC_SERVER_URL}/incidents/customQuery`;

  const headers = {
    "Content-Type": "application/json",
    "x-api-key": process.env.DATASYNC_SERVER_API_KEY
  };

  const body = {
    where: "caused_by_change_number != ''",
    page: 1,
    page_size: 300
  };

  try {
    const response = await fetch(url, {
      method: "POST",
      headers,
      body: JSON.stringify(body)
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status cbc incidents: ${response.status}`);
    }

    const data = await response.json();
    console.log("Data:", data);

    await pushToElastic(data);
    return data;

  } catch (error) {
    console.error("Error fetching CBC incidents:", error);
    throw error;
  }
}

module.exports = {
  cbcIncidents,
  pushToElastic
}