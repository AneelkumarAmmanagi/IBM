require("dotenv").config();
const { client: esClient } = require("../databaseSetup/elasticsearchConfig");
const { Table } = require("console-table-printer");
const { readParquetFile } = require("./parquetFileReader");
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
dayjs.extend(utc);
const { fetchChangeRequests } = require("./changeRequestService");
const { postDailySummaryToGithub } = require("./githubService");

async function showTodaysClosedChanges(esClient, indexName) {
  console.log("\n📅 Fetching today's changes based on planned_start...");

  const todayStart = dayjs().utc().startOf("day").toISOString();
  const todayEnd = dayjs().utc().endOf("day").toISOString();

  try {
    const resp = await esClient.search({
        index: indexName,
        size: 10000,
        _source: ["number", "state", "close_category", "planned_start"],
        body: {
          query: {
            bool: {
              must: [
                {
                  range: {
                    planned_start: {
                      gte: todayStart,
                      lte: todayEnd,
                    },
                  },
                },
                {
                  term: {
                    state: "Closed",
                  },
                },
              ],
            },
          },
          sort: [{ planned_start: { order: "asc" } }],
        },
      });      

    const hits = resp.hits?.hits || resp.body?.hits?.hits || [];
    const docs = hits.map((h) => ({
      number: h._source?.number || "—",
      state: h._source?.state || "—",
      close_category: h._source?.close_category || "—",
      planned_start: h._source?.planned_start || "—",
    }));

    console.log(`✅ Found ${docs.length} changes planned for today (${todayStart} to ${todayEnd}).`);

    if (docs.length > 0) {
      const { Table } = require("console-table-printer");
      const todayTable = new Table({
        title: "📋 Today's Closed Changes (based on planned_start)",
        columns: [
          { name: "number", title: "Change Number" },
          { name: "state", title: "State" },
          { name: "close_category", title: "Closed Category" },
          { name: "planned_start", title: "Planned Start (UTC)" },
        ],
      });

      docs.slice(0, 20).forEach((r) => todayTable.addRow(r));
      todayTable.printTable();
    } else {
      console.log("ℹ️ No changes found for today's planned_start date.");
    }
  } catch (err) {
    console.error("❌ Error fetching today's data:", err.message || err);
  }
}



async function compareClosedCategoryBatched(esClient, indexName, parquetRecords, batchSize = 200, concurrency = 8) {
    console.log("\n🔍 Comparing 'close_category' for Closed changes...");
  
    // Filter Parquet records with state = Closed
    const closedRecords = parquetRecords.filter(
      (r) => r.state === "Closed"
    );
  
    console.log(`Found ${closedRecords.length} Closed records in Parquet.`);
  
    const mismatched = [];
    const totalBatches = Math.ceil(closedRecords.length / batchSize);
    let processed = 0;
  
    async function processBatch(batch, batchIndex) {
      const numbers = Array.from(
        new Set(batch.map((r) => String(r.number || "").trim()).filter(Boolean))
      );
  
      if (numbers.length === 0) return;
  
      try {
        // Bulk search: use `terms` query instead of per-doc search
        const resp = await esClient.search({
          index: indexName,
          size: numbers.length,
          _source: ["number", "state", "close_category"],
          body: {
            query: {
              terms: { "number": numbers },
            },
          },
        });
  
        const hits = resp.hits?.hits || resp.body?.hits?.hits || [];
        const esMap = new Map(
          hits.map((h) => [String(h._source?.number || "").trim(), h._source])
        );
  
        for (const pq of batch) {
          const num = String(pq.number || "").trim();
          if (!num) continue;
  
          const pqCat = pq.close_category || "—";
          const esDoc = esMap.get(num);
          if (!esDoc) continue;
  
          const esCat = esDoc.close_category || "—";
          if (esCat !== pqCat) {
            mismatched.push({
              number: num,
              parquet_closed_category: pqCat,
              elastic_closed_category: esCat,
            });
          }
        }
  
        processed += batch.length;
        if (batchIndex < 3) {
          console.log(
            `  🔎 Batch ${batchIndex + 1}/${totalBatches} — Checked ${numbers.length} numbers, found ${hits.length} hits.`
          );
        }
  
        if (processed % 2000 === 0 || processed === closedRecords.length) {
          console.log(`  🔹 Progress: ${processed}/${closedRecords.length}`);
        }
      } catch (err) {
        console.warn(`⚠️ Error in batch ${batchIndex + 1}:`, err.message || err);
      }
    }
  
    // Parallel execution with limited concurrency
    for (let i = 0; i < closedRecords.length; i += batchSize * concurrency) {
      const slice = closedRecords.slice(i, i + batchSize * concurrency);
      const batchGroups = [];
  
      for (let j = 0; j < slice.length; j += batchSize) {
        batchGroups.push(slice.slice(j, j + batchSize));
      }
  
      await Promise.all(
        batchGroups.map((batch, idx) => processBatch(batch, i / batchSize + idx))
      );
    }
  
    console.log(`\n🔹 Found ${mismatched.length} mismatched Closed categories.`);
  
    if (mismatched.length > 0) {
      const { Table } = require("console-table-printer");
      const diffTable = new Table({
        title: "📋 Closed Category Mismatches (sample 20)",
        columns: [
          { name: "number", title: "Change Number" },
          { name: "parquet_closed_category", title: "Parquet Closed Category" },
          { name: "elastic_closed_category", title: "Elastic Closed Category" },
        ],
      });
      mismatched.slice(0, 20).forEach((r) => diffTable.addRow(r));
      diffTable.printTable();
    }
  
    return mismatched;
  }  
  

async function fetchElasticDocsByNumbersRobust(indexName, esClient, parquetRecords, batchSize = 200, concurrency = 8) {
  const esDocsMap = new Map();
  const batches = [];

  // Build batches of numbers
  for (let i = 0; i < parquetRecords.length; i += batchSize) {
    const batch = parquetRecords.slice(i, i + batchSize);
    const numbersRaw = batch.map((r) => (r.number === undefined || r.number === null) ? "" : String(r.number));
    // trim, dedupe and filter out empty
    const numbers = Array.from(new Set(numbersRaw.map(s => s.trim()).filter(Boolean)));
    if (numbers.length > 0) batches.push(numbers);
  }

  let processed = 0;
  let batchCounter = 0;

  // Try multiple query strategies for each batch until we get hits
  async function processBatch(numbers, batchIndex) {
    // Keep a local set of hits to add into esDocsMap
    let hits = [];
    let tried = [];

    // Helper to run a single search body and return hits
    async function runSearch(body, size) {
      try {
        const resp = await esClient.search({
          index: indexName,
          size: size || numbers.length,
          _source: ["number", "created", "state", "tribe"],
          body,
        });
        return resp.hits?.hits || resp.body?.hits?.hits || [];
      } catch (err) {
        // log but keep trying other strategies
        console.warn(`⚠️ ES search error (batch ${batchIndex}):`, err.message || err);
        return [];
      }
    }

    // Strategy A: number.keyword (exact string match)
    tried.push("number.keyword (string terms)");
    hits = await runSearch({
      query: {
        terms: { "number.keyword": numbers }
      }
    });

    // Strategy B: numeric 'number' (if no hits and numeric values exist)
    if ((!hits || hits.length === 0)) {
      const numericVals = numbers.map(n => {
        const v = Number(n);
        return Number.isFinite(v) ? v : null;
      }).filter(v => v !== null);
      if (numericVals.length > 0) {
        tried.push("number (numeric terms)");
        hits = await runSearch({
          query: {
            terms: { number: numericVals }
          }
        });
      }
    }

    // Strategy C: doc _id (if still no hits)
    if ((!hits || hits.length === 0)) {
      tried.push("_id (terms)");
      hits = await runSearch({
        query: {
          terms: { "_id": numbers }
        }
      });
    }

    // Strategy D: fallback to bool should on number.keyword + _id (if still nothing)
    if ((!hits || hits.length === 0)) {
      tried.push("fallback bool should (number.keyword OR _id)");
      hits = await runSearch({
        query: {
          bool: {
            should: [
              { terms: { "number.keyword": numbers } },
              { terms: { "_id": numbers } }
            ],
            minimum_should_match: 1
          }
        }
      });
    }

    // Add hits to the map
    for (const h of hits) {
      const src = h._source || {};
      const num = src.number ? String(src.number) : String(h._id);
      if (!num) continue;
      esDocsMap.set(num, {
        number: num,
        state: src.state || "N/A",
        tribe: src.tribe || "—",
        created: src.created || "—",
      });
    }

    processed += numbers.length;
    batchCounter += 1;

    // Diagnostic logging for the first few batches to understand mapping/issues
    if (batchIndex < 6) { // show for first 6 batches
      console.log(`\n🔎 Batch ${batchIndex + 1}/${Math.ceil(parquetRecords.length / batchSize)} diagnostic:`);
      console.log(`  - requested count: ${numbers.length}`);
      console.log(`  - tried strategies: ${tried.join(" -> ")}`);
      console.log(`  - hits returned: ${hits.length}`);
      if (hits.length > 0) {
        const sample = hits.slice(0, 3).map(h => {
          const s = h._source || {};
          return { number: s.number || h._id, state: s.state || "N/A", tribe: s.tribe || "—" };
        });
        console.log(`  - sample hits:`, sample);
      } else {
        // show the first 8 numbers we asked for for debugging
        console.log(`  - no hits; sample requested numbers:`, numbers.slice(0, 8));
      }
    }

    if (processed % 2000 === 0 || processed === parquetRecords.length) {
      console.log(`🔹 Progress: ${Math.min(processed, parquetRecords.length)}/${parquetRecords.length}`);
    }
  }

  // Parallel execution with limited concurrency
  for (let i = 0; i < batches.length; i += concurrency) {
    const slice = batches.slice(i, i + concurrency);
    // kick off these slice promises in parallel and wait for them
    await Promise.all(slice.map((nums, idx) => processBatch(nums, i + idx)));
  }

  console.log(`\n✅ Retrieved ${esDocsMap.size} matching docs from Elasticsearch (robust fetch)`);
  return esDocsMap;
}

// --- Markdown serialization helpers ---
function tableToMarkdown(title, columns, rows, maxRows = 20) {
  if (!rows || rows.length === 0) return `### ${title}\n_No data._`;
  const headers = columns.map(c => c.title);
  const keys = columns.map(c => c.name);
  const mdRows = rows.slice(0, maxRows).map(r =>
    '| ' + keys.map(k => (r[k] !== undefined ? String(r[k]) : '')).join(' | ') + ' |');
  return [
    `### ${title}`,
    '| ' + headers.join(' | ') + ' |',
    '| ' + headers.map(() => '---').join(' | ') + ' |',
    ...mdRows,
    rows.length > maxRows ? `... (${rows.length} rows, only showing first ${maxRows})` : ''
  ].filter(Boolean).join('\n');
}

// --- Main comparison (keeps detailed output) ---
async function compareElasticAndDatalakedata() {
  try {
    // Trigger COS to parquet download by calling fetchChangeRequests first
    console.log("⏬ Ensuring latest COS parquet snapshot by running fetchChangeRequests() ...");

    const indexName = process.env.ELASTIC_INDEX || "change-risk-analysis";
    const batchSize = parseInt(process.env.BATCH_SIZE || "200", 10);
    const concurrency = parseInt(process.env.CONCURRENCY || "8", 10);

    console.log("📂 Reading Parquet file...");
    const parquetRecords = await readParquetFile();
    console.log(`✅ Retrieved ${parquetRecords.length} records from Parquet`);

    console.log(`🔍 Looking up change numbers in Elasticsearch (batch=${batchSize}, concurrency=${concurrency})...`);
    const esDocsMap = await fetchElasticDocsByNumbersRobust(indexName, esClient, parquetRecords, batchSize, concurrency);

    const pqMap = new Map(parquetRecords.map((r) => [String(r.number).trim(), r]));

    const pqNotInES = [];
    const esNotInPQ = [];

    // Compare both sides and collect full objects
    for (const [num, pq] of pqMap.entries()) {
      if (!esDocsMap.has(num)) pqNotInES.push({
        number: num,
        state: pq.state || "—",
        tribe: pq.tribe || "—",
        created: pq.created || "—"
      });
    }
    for (const [num, es] of esDocsMap.entries()) {
      if (!pqMap.has(num)) esNotInPQ.push(es);
    }

    // --- Summary ---
    const totalPQ = pqMap.size;
    const totalES = esDocsMap.size;
    const overlap = totalPQ - pqNotInES.length;
    const overlapPct = totalPQ === 0 ? "0.00" : ((overlap / totalPQ) * 100).toFixed(2);

    const summaryCols = [
      { name: "metric", title: "Metric" },
      { name: "count", title: "Count" },
    ];
    const summaryRows = [
      { metric: "Parquet total", count: totalPQ },
      { metric: "Elasticsearch total", count: totalES },
      { metric: "In both", count: overlap },
      { metric: "Parquet ∖ Elastic", count: pqNotInES.length },
      { metric: "Elastic ∖ Parquet", count: esNotInPQ.length },
      { metric: "Overlap %", count: `${overlapPct}%` },
    ];
    const summary = new Table({ title: "📊 Elastic vs Parquet Comparison Summary", columns: summaryCols });
    summaryRows.forEach(r => summary.addRow(r));
    summary.printTable();

    const pqDiffCols = [
      { name: "number", title: "Change Number" },
      { name: "state", title: "State" },
      { name: "tribe", title: "Tribe" },
      { name: "created", title: "Created" },
    ];
    const pqDiffTableRows = pqNotInES;
    const pqDiffTable = new Table({ title: "📋 Changes in Parquet but Missing in Elastic (sample 20)", columns: pqDiffCols });
    pqDiffTableRows.slice(0, 20).forEach(r => pqDiffTable.addRow(r));
    pqDiffTable.printTable();

    const esDiffCols = [
      { name: "number", title: "Change Number" },
      { name: "state", title: "State" },
      { name: "tribe", title: "Tribe" },
      { name: "created", title: "Created" },
    ];
    const esDiffTableRows = esNotInPQ;
    const esDiffTable = new Table({ title: "📋 Changes in Elastic but Missing in Parquet (sample 20)", columns: esDiffCols });
    esDiffTableRows.slice(0, 20).forEach(r => esDiffTable.addRow(r));
    esDiffTable.printTable();

    console.log(`\n🔹 Showing top 20 differences from each side. (${pqNotInES.length} missing in ES, ${esNotInPQ.length} missing in PQ)`);

    // Markdown serialization for GitHub
    const parts = [];
    parts.push(`# Daily Change Risk Processor Summary`);
    parts.push(`_Run at: ${new Date().toISOString()}_`);
    parts.push(tableToMarkdown('Elastic vs Parquet Comparison Summary', summaryCols, summaryRows, 10));
    parts.push(tableToMarkdown('Changes in Parquet but Missing in Elastic', pqDiffCols, pqDiffTableRows));
    parts.push(tableToMarkdown('Changes in Elastic but Missing in Parquet', esDiffCols, esDiffTableRows));
    parts.push('---');

    // --- Detailed compareClosedCategoryBatched ---
    const mismatched = await compareClosedCategoryBatched(esClient, indexName, parquetRecords);
    if (mismatched && mismatched.length > 0) {
      const mismatchCols = [
        { name: 'number', title: 'Change Number' },
        { name: 'parquet_closed_category', title: 'Parquet Closed Category' },
        { name: 'elastic_closed_category', title: 'Elastic Closed Category' },
      ];
      parts.push(tableToMarkdown('Closed Category Mismatches (sample 20)', mismatchCols, mismatched));
      parts.push('---');
    }

    // --- Today's closed changes
    // Fetch the docs for today and include in markdown (reuse function, but not console output)
    try {
      const todayStart = dayjs().utc().startOf("day").toISOString();
      const todayEnd = dayjs().utc().endOf("day").toISOString();
      const resp = await esClient.search({
        index: indexName,
        size: 10000,
        _source: ["number", "state", "close_category", "planned_start"],
        body: {
            query: {
              bool: {
                must: [
                  {
                    range: {
                      planned_start: {
                        gte: todayStart,
                        lte: todayEnd,
                      },
                    },
                  },
                  {
                    term: {
                      state: "Closed",
                    },
                  },
                ],
              },
            },
            sort: [{ planned_start: { order: "asc" } }],
          },
      });
      const hits = resp.hits?.hits || resp.body?.hits?.hits || [];
      const docs = hits.map((h) => ({
        number: h._source?.number || "—",
        state: h._source?.state || "—",
        close_category: h._source?.close_category || "—",
        planned_start: h._source?.planned_start || "—",
      }));
      const todayCols = [
        { name: "number", title: "Change Number" },
        { name: "state", title: "State" },
        { name: "close_category", title: "Closed Category" },
        { name: "planned_start", title: "Planned Start (UTC)" },
      ];
      parts.push(tableToMarkdown("Today's Closed Changes (based on planned_start)", todayCols, docs));
      parts.push('---');
    } catch (err) {
      parts.push('_Error fetching today\'s planned_start closed changes._');
    }

    const markdown = parts.join('\n\n');

    // --- GitHub post integration ---
    try {
      
      await postDailySummaryToGithub(markdown);
    } catch (err) {
      console.error("❌ Failed to post summary to GitHub:", err.message || err);
    }

    // Extra helpful diagnostic: if ES returned 0 matches overall, print a direct check of 3 sample numbers
    if (esDocsMap.size === 0 && parquetRecords.length > 0) {
      const sampleNums = parquetRecords.slice(0, 3).map(r => String(r.number).trim());
      console.log("\n⚠️ ES returned zero matches overall. Trying single-number searches for quick debug:");
      for (const n of sampleNums) {
        try {
          const resp = await esClient.search({
            index: indexName,
            size: 3,
            _source: ["number", "created", "state", "tribe"],
            body: { query: { bool: { should: [{ term: { "number.keyword": n } }, { term: { "_id": n } }], minimum_should_match: 1 } } }
          });
          const hits = resp.hits?.hits || resp.body?.hits?.hits || [];
          console.log(`  - sample number "${n}" returned ${hits.length} hits. Sample:`, hits.slice(0,2).map(h => h._source || { _id: h._id }));
        } catch (err) {
          console.warn(`  - error while testing sample "${n}":`, err.message || err);
        }
      }
    }

    await showTodaysClosedChanges(esClient, indexName);


  } catch (err) {
    console.error("❌ Error:", err.message || err);
  }
}

module.exports={
    compareElasticAndDatalakedata
}
