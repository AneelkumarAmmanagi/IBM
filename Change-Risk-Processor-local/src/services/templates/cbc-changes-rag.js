const { client } =require( "../../databaseSetup/elasticsearchConfig");

const sendCbcChangesToApi = async () => {
  const indexName = "cbc-changes-latest";

  // The exact fields you want
  const fields = [
    "backout_plan",
    "close_notes",
    "contact_type",
    "deployment_history",
    "deployment_impact",
    "deployment_method",
    "deployment_risk",
    "customer_impact",
    "description",
    "number",
    "impact",
    "pipeline_name",
    "planned_duration",
    "priority",
    "purpose",
    "risk",
    "short_description",
    "service_environment",
    "tribe",
    "type",
    "locations",
    "service_names",
    "regions",
    "extracted_locations",
    "location_source",
    "dc"
  ];

  try {
    console.log(`📥 Reading documents from "${indexName}"...`);

    const searchResponse = await client.search({
      index: indexName,
      size: 10000,
      _source: fields,
      query: { match_all: {} }
    });

    const docs = searchResponse.hits?.hits?.map(hit => hit._source) || [];
    console.log(`✅ Retrieved ${docs.length} documents from "${indexName}".`);

    if (!docs.length) {
      console.log("No documents found. Nothing to send.");
      return;
    }

    const apiUrl =
      "https://copilot-api-platform-dashboard.1tt53focxwds.us-east.codeengine.appdomain.cloud/api/applications/70ZLWpgBuilC-roNKLSR/flows/2215a269-d4c1-4e4a-a9fd-4be210a24328/execute";

    // Loop over each doc and send individually
    for (const doc of docs) {
      console.log(`📤 Sending change number ${doc.number} to API...`);

      const response = await fetch(apiUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "apiKey": "453a32a6-3450-4b6e-9f66-25eb778a54f1"
        },
        body: JSON.stringify({ data: doc })
      });

      if (!response.ok) {
        console.error(`❌ Failed for ${doc.number}: HTTP ${response.status}`);
        continue; // skip failed docs but continue loop
      }

      const result = await response.json();
      console.log(`✅ Successfully sent ${doc.number}. API response:`, result);
    }

    console.log("🎯 All documents processed.");
  } catch (error) {
    console.error("❌ Error in sendCbcChangesToApi:", error);
    throw error;
  }
}

module.exports = {
  sendCbcChangesToApi
}