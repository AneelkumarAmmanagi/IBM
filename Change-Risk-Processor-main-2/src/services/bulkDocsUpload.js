const _ = require("lodash");
const { client } = require("../databaseSetup/elasticsearchConfig");
const { getGithubUrlContent } = require('./githubService.js');

async function checkChangeNumbersInElastic(index, changeNumbers) {
  if (!changeNumbers || changeNumbers.length === 0) {
    return new Set();
  }

  const existingNumbers = new Set();
  const batchSize = 1000;
  
  for (let i = 0; i < changeNumbers.length; i += batchSize) {
    const batch = changeNumbers.slice(i, i + batchSize);
    
    try {
      const searchResponse = await client.search({
        index,
        size: batchSize,
        body: {
          query: {
            terms: {
              "number.keyword": batch,
            },
          },
          _source: ["number"],
        },
      });

      const hits = searchResponse.hits?.hits || [];
      hits.forEach((hit) => {
        if (hit._source?.number) {
          existingNumbers.add(hit._source.number);
        }
      });

      // console.log(`Checked batch ${Math.floor(i / batchSize) + 1}: ${batch.length} numbers, ${hits.length} found in Elasticsearch`);
    } catch (error) {
      console.error(`Error checking batch ${Math.floor(i / batchSize) + 1}:`, error?.message);
    }
  }

  console.log(`Total existing numbers in Elasticsearch: ${existingNumbers.size} out of ${changeNumbers.length}`);
  return existingNumbers;
}

async function fetchExistingStatesForNumbers(index, changeNumbers) {
  if (!changeNumbers || changeNumbers.length === 0) {
    console.log("No numbers provided.");
    return new Map();
  }

  const numberToState = new Map();
  const batchSize = 1000;

  console.log(`Fetching states for ${changeNumbers.length} numbers in batches of ${batchSize}...`);

  for (let i = 0; i < changeNumbers.length; i += batchSize) {
    const batch = changeNumbers.slice(i, i + batchSize);
    console.log(`Processing batch ${i / batchSize + 1}: ${batch.length} numbers`);


    try {
      const searchResponse = await client.search({
        index,
        size: batchSize,
        body: {
          query: {
            bool: {
              must: [
                {
                  terms: {
                    "number.keyword": batch,
                  },
                },
              ],
              must_not: [
                {
                  term: {
                    "state.keyword": "Closed", // Exclude closed states
                  },
                },
              ],
            },
          },
          _source: ["number", "state"],
        },
      });


      const hits = searchResponse.hits?.hits || [];
      console.log(`Found ${hits.length} matching documents in this batch.`);

      hits.forEach((hit) => {
        if (hit._source?.number) {
          numberToState.set(hit._source.number, hit._source.state);
          console.log(`Number: ${hit._source.number}, State: ${hit._source.state}`);
        }
      });
    } catch (error) {
      console.error("Error fetching existing states:", error?.message);
      console.error("Failed batch numbers:", batch);
    }
  }

  console.log(`Total unique numbers found: ${numberToState.size}`);
  return numberToState;
}

async function fetchElasticDocuments(index) {
  const searchResponse = await client.search({
    index,
    size: 10000,
    body: {
      query: {
        match_all: {},
      },
      sort: [
        { created: { order: "desc" } },
      ],
    },
  });

  const elasticDocuments = searchResponse.hits.hits.map((hit) => hit._source);
  console.log(`Total documents found: ${elasticDocuments.length}`);

  return elasticDocuments;
}

async function fetchElasticDocumentToClose(index) {
  const searchResponse = await client.search({
    index,
    size: 10000, // Fetch up to 10,000 documents per batch
    body: {
      query: {
        bool: {
          must_not: {
            term: { archived: true }, // Exclude documents where archived is true
          },
        },
      },
    },
  });

  // Extract and log all document numbers
  const elasticDocuments = searchResponse.hits.hits.map((hit) => hit._source);
  console.log(`Total documents found: ${elasticDocuments.length}`);

  return elasticDocuments; // Convert to Set for fast lookup
}

async function fetchElasticClosedDocument(index) {
  const searchResponse = await client.search({
    index,
    size: 10000,
    body: {
      query: {
        bool: {
          must: [
            { term: { archived: true } },
            {
              range: {
                archived_at: {
                  gte: "now-7d/d",
                  lte: "now",
                },
              },
            },
          ],
          must_not: [{ exists: { field: "incident_numbers" } }],
        },
      },
    },
  });

  // Extract and log all document sources
  const elasticDocuments = searchResponse.hits.hits.map((hit) => {
    if (hit._source?.actual_end == null) {
      return {
        number: hit._source.number,
        planned_end: hit._source.planned_end, // make sure this field exists
      };
    } else {
      return {
        number: hit._source.number,
        actual_end: hit._source.actual_end,
      };
    }
  });
  console.log(`Total documents found: ${elasticDocuments.length}`);

  return elasticDocuments;
}

async function bulkUpdateStateOnly(index, changedRecords) {
  if (!changedRecords || changedRecords.length === 0) {
    console.log("No records to update.");
    return;
  }

  const numberToDoc = new Map(); // store both _id and old state
  const numbers = changedRecords.map(r => r.number);
  const batchSize = 1000;

  console.log(`Fetching _id and current state for ${changedRecords.length} records...`);

  for (let i = 0; i < numbers.length; i += batchSize) {
    const batch = numbers.slice(i, i + batchSize);

    const res = await client.search({
      index,
      size: batch.length,
      _source: ["number", "state"],
      body: {
        query: {
          bool: {
            must: [
              { terms: { "number.keyword": batch } },
            ],
            must_not: [
              { term: { "state.keyword": "Closed" } }, // exclude closed records
            ],
          },
        },
      },
    });

    const hits = res.hits.hits || [];
    hits.forEach((hit) => {
      const num = hit._source?.number;
      if (num) {
        numberToDoc.set(num, {
          _id: hit._id,
          oldState: hit._source?.state ?? "N/A",
        });
      }
    });
  }

  console.log(`Found ${numberToDoc.size} matching (non-Closed) documents to update.`);

  const allowedStates = new Set(["New", "Implement", "Scheduled"]);
  const bulkSize = 1000;
  let skipped = 0;

  for (let i = 0; i < changedRecords.length; i += bulkSize) {
    const batch = changedRecords.slice(i, i + bulkSize);

    const body = batch.flatMap(({ number, state }) => {
      const info = numberToDoc.get(number);
      if (!info) {
        skipped++;
        return [];
      }

      if (
        info.oldState === state ||
        state === "Closed" ||
        !allowedStates.has(state)
      ) {
        skipped++;
        return [];
      }

      console.log(`Number: ${number} | Old State: ${info.oldState} → New State: ${state}`);

      return [
        { update: { _index: index, _id: info._id } },
        { doc: { state } },
      ];
    });

    if (body.length === 0) continue;

    try {
      const response = await client.bulk({ refresh: false, body });

      if (response.errors) {
        const errors = response.items.filter(i => i.update?.error);
        console.error(`⚠️ ${errors.length} update errors in this batch.`);
        errors.forEach((err) =>
          console.error(`Failed update: ${err.update?._id} → ${err.update?.error?.reason}`)
        );
      } else {
        console.log(`✅ Updated ${batch.length} documents in this batch.`);
      }
    } catch (err) {
      console.error("❌ Bulk update failed:", err.message);
    }
  }

  await client.indices.refresh({ index });
  const sample = Array.from(numberToDoc.keys()).slice(0, 5);

  if (sample.length > 0) {
    const verify = await client.search({
      index,
      size: 5,
      _source: ["number", "state"],
      body: {
        query: {
          terms: {
            "number.keyword": sample,
          },
        },
      },
    });

    console.log("🔍 Sample post-update verification:");
    verify.hits.hits.forEach((h) =>
      console.log(`Number: ${h._source.number} | Current State: ${h._source.state}`)
    );
  }

  console.log(`🎉 Finished updating all states (Skipped: ${skipped}).`);
}

const filterNewRecords = async (parquetData, index) => {
  console.log("index", index);
  console.log(`Total parquet records: ${parquetData.length}`);
  const changeNumbers = parquetData.map((record) => record.number).filter(Boolean);
  console.log(`Unique change numbers in parquet: ${changeNumbers.length}`);
  
  const existingNumbers = await checkChangeNumbersInElastic(index, changeNumbers);

  // Records not in Elasticsearch
  const newRecords = parquetData.filter(
    (record) => !existingNumbers.has(record.number)
  );

  // For records present in Elasticsearch, check if state differs
  const existingNumbersArray = changeNumbers.filter((n) => existingNumbers.has(n));
  const numberToState = await fetchExistingStatesForNumbers(index, existingNumbersArray);

  const changedRecords = parquetData.filter((record) => {
    if (!existingNumbers.has(record.number)) return false;
    const currentState = numberToState.get(record.number);
    return currentState !== record.state;
  });

  console.log(`New records to insert: ${newRecords.length}`);
  console.log(`Existing records with state changes to update: ${changedRecords.length}`);

  await bulkUpsertElastic(index, newRecords);
  await bulkUpdateStateOnly(index, changedRecords);
  return newRecords;
};

function extractUrls(text) {
  const urlRegex = /https?:\/\/[^\s'"<>]+/g;
  return text.match(urlRegex) || [];
}

async function bulkUpsertElastic(index, records, batchSize = 1000) {
  if (!index) {
    throw new Error("Elasticsearch index is not specified!");
  }

  if (records.length === 0) {
    console.log("No new records to insert.");
    return;
  }

  console.log(`Total records to insert: ${records.length}`);

  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize);

    // Enrich only the current batch to avoid retaining large strings for all records in memory
    for (const record of batch) {
      record.prepostchecks = "";
      const urls = extractUrls(record.description);
      for (const url of urls) {
        const repoFilePattern = /^https:\/\/github\.ibm\.com\/[^\/]+\/[^\/]+\/blob\/[^\/]+\/.+$/;
        if (repoFilePattern.test(url)) {
          const content = await getGithubUrlContent(url);
          record.prepostchecks = record.prepostchecks.concat(content, "\n");
        }
      }
    }

    const bulkBody = batch.flatMap((record) => [
      { update: { _index: index, _id: record.number } },
      { doc: record, doc_as_upsert: true },
    ]);

    try {
      const { body } = await client.bulk({ refresh: false, body: bulkBody });
      if (body?.errors) {
        console.error(`Batch ${i / batchSize + 1} had errors:`, body?.errors);
      } else {
        console.log(
          `Successfully upserted batch ${i / batchSize + 1} (${batch.length
          } records)`
        );
      }
    } catch (error) {
      console.error(`Error in batch ${i / batchSize + 1}:`, error);
    }
  }

  console.log("All records processed successfully.");
}

module.exports = {
  filterNewRecords,
  fetchElasticDocuments,
  fetchElasticDocumentToClose,
  fetchElasticClosedDocument,
};
