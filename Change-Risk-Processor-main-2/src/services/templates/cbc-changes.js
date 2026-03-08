const { client } = require("../../databaseSetup/elasticsearchConfig.js");
const { pushToElastic } = require("./cbc-incidents.js");

const cbcChanges = async () => {
  const incidentsIndex = "cbc_incidents";
  const changesIndex = "cbc-changes-latest";

  try {
    const incidentsIndex = "cbc_incidents";

    // 1️⃣ Get all change numbers from incidents index
    const searchResponse = await client.search({
      index: incidentsIndex,
      size: 10000,
      _source: ["caused_by_change_number"],
      query: {
        bool: {
          must: [
            { exists: { field: "caused_by_change_number" } },
            { wildcard: { caused_by_change_number: "*" } }
          ]
        }
      }
    });

    const changeNumbers = [
      ...new Set(
        searchResponse.hits.hits
          .map(hit => hit._source.caused_by_change_number)
          .filter(Boolean)
      )
    ];

    console.log(`Found ${changeNumbers.length} unique change numbers from incidents.`);

    // 2️⃣ Fetch details in chunks
    const chunkSize = 50;
    const changeDetails = [];
    let notFound = [];

    for (let i = 0; i < changeNumbers.length; i += chunkSize) {
      const chunk = changeNumbers.slice(i, i + chunkSize);
      const whereClause = `number IN (${chunk.map(num => `'${num}'`).join(",")})`;

      const response = await fetch(
        `${process.env.DATASYNC_SERVER_URL}/changeRequests/customQuery`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "x-api-key": `${process.env.DATASYNC_SERVER_URL}`
          },
          body: JSON.stringify({
            where: whereClause,
            page: 1,
            page_size: chunkSize
          })
        }
      );

      if (!response.ok) {
        console.error(`❌ Failed to fetch change details for chunk starting with ${chunk[0]}: HTTP ${response.status}`);
        continue;
      }

      const data = await response.json();
      if (data?.data?.length) {
        changeDetails.push(...data.data);
      } else {
        notFound.push(...chunk);
      }
    }

    console.log(`✅ Retrieved ${changeDetails.length} change records from API`);
    console.log(`⚠️ ${notFound.length} change numbers not found in API`);

    // 3️⃣ Check "not found" ones in `change-risk-analysis`
    // 3️⃣ Check "not found" ones in `change-risk-analysis`
if (notFound.length) {
    console.log(`🔍 Checking ${notFound.length} "not found" numbers in change-risk-analysis...`);
  
    const riskCheck = await client.mget({
      index: "change-risk-analysis",
      ids: notFound
    });
  
    const foundInRisk = riskCheck.docs
      .filter(d => d.found && d._source)
      .map(d => ({
        ...d._source,
        number: d._id // ensure number field exists for consistency
      }));
  
    console.log(`✅ Found ${foundInRisk.length} extra records in change-risk-analysis`);
  
    changeDetails.push(...foundInRisk);
    notFound = notFound.filter(num => !foundInRisk.some(doc => doc.number === num));
  }
  
  // 4️⃣ Last-resort: Call single change number API
  if (notFound.length) {
    console.log(`📡 Calling single changeRequests API for ${notFound.length} remaining numbers...`);
  
    for (const changeNum of notFound) {
      try {
        const res = await fetch(
          `${process.env.DATASYNC_SERVER_URL}changeRequests/number/${encodeURIComponent(changeNum)}`,
          {
            method: "GET",
            headers: {
              "Content-Type": "application/json",
              "x-api-key": `${process.env.DATASYNC_SERVER_URL}`
            }
          }
        );
  
        if (res.ok) {
          const record = await res.json();
          if (record && Object.keys(record).length > 0) {
            changeDetails.push({
              ...record,
              number: changeNum
            });
            console.log(`✅ Found change ${changeNum} via single API call`);
          }
        } else {
          console.warn(`⚠️ Single API call failed for ${changeNum}: HTTP ${res.status}`);
        }
      } catch (err) {
        console.error(`❌ Error fetching change ${changeNum} via single API call:`, err);
      }
    }
  }
  

    // 4️⃣ Remove duplicates before pushing
    const uniqueDetails = [
      ...new Map(changeDetails.map(doc => [doc.number, doc])).values()
    ];

    // 5️⃣ Check which ones already exist in target index
    const existingDocs = await client.mget({
      index: changesIndex,
      ids: uniqueDetails.map(doc => doc.number)
    });

    const existingIds = new Set(existingDocs.docs.filter(d => d.found).map(d => d._id));

    const newDocs = uniqueDetails.filter(doc => !existingIds.has(doc.number));

    if (!newDocs.length) {
      console.log("No new change documents to push.");
      return { indexed: 0, notFound };
    }

    // 6️⃣ Bulk insert new docs
    const bulkBody = newDocs.flatMap(doc => [
      { index: { _index: changesIndex, _id: doc.number } },
      doc
    ]);

    const bulkResponse = await client.bulk({ refresh: true, body: bulkBody });

    if (bulkResponse.errors) {
      console.error("Some documents failed to index:", bulkResponse.items);
    } else {
      console.log(`✅ Indexed ${newDocs.length} new documents into "${changesIndex}"`);
    }

    return { indexed: newDocs.length, notFound };
  } catch (err) {
    console.error("Error in cbcChanges:", err);
    throw err;
  }
}

module.exports = {
  cbcChanges
}
