const { client } = require("../../databaseSetup/elasticsearchConfig");

const sendCbcIncidentsToApi = async () => {
  const indexName = "cbc_incidents";
  // Target index to check for already-processed incidents (RAG results)
  const ragIndexName = process.env.CBC_INCIDENTS_RAG_INDEX || "cbc-incidents-rag";

  try {
    console.log(`📥 Reading documents from "${indexName}"...`);

    const searchResponse = await client.search({
      index: indexName,
      size: 10000,
      query: { match_all: {} }
    });

    const docs = searchResponse.hits?.hits?.map((hit) => hit._source) || [];
    console.log(`✅ Retrieved ${docs.length} documents from "${indexName}".`);

    if (!docs.length) {
      console.log("No documents found. Nothing to send.");
      return;
    }

    // Deduplicate: send only those incident numbers which don't exist in the target RAG index
    let docsToSend = docs;
    try {
      const ragIndexExists = await client.indices.exists({ index: ragIndexName });
      if (ragIndexExists) {
        const ids = docs.map((d) => d.number).filter(Boolean);
        if (ids.length) {
          const existingDocs = await client.mget({ index: ragIndexName, ids });
          const existingIds = new Set(
            (existingDocs?.docs || [])
              .filter((d) => d?.found)
              .map((d) => d?._id)
          );
          docsToSend = docs.filter((d) => !existingIds.has(d.number));
          console.log(
            `🔎 Deduped incidents for RAG send. Existing: ${existingIds.size}, To send: ${docsToSend.length}`
          );
          if (!docsToSend.length) {
            console.log("No new incidents to send. Exiting.");
            return;
          }
        }
      } else {
        console.log(
          `ℹ️ Target index "${ragIndexName}" does not exist. Proceeding to send all ${docs.length} incidents.`
        );
      }
    } catch (dedupeErr) {
      console.warn(
        `⚠️ Skipping dedupe due to error while checking index "${ragIndexName}":`,
        dedupeErr?.message || dedupeErr
      );
    }

    const apiUrl =
      "https://copilot-api-platform-dashboard.1tt53focxwds.us-east.codeengine.appdomain.cloud/api/applications/70ZLWpgBuilC-roNKLSR/flows/470f8578-3fec-471c-95b6-3a883b72fd5f/execute";

    // Loop over each doc and send individually
    for (const doc of docsToSend) {
      console.log(`📤 Sending Incident number ${doc.number} to API...`);

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
    console.error("❌ Error in sendCbcIncidentsToApi:", error);
    throw error;
  }
}

module.exports = {
    sendCbcIncidentsToApi
}