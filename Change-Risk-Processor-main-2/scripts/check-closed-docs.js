require("dotenv").config();

const duckdb = require("duckdb");
const path = require("path");
const fs = require("fs");
const { client } = require("../src/databaseSetup/elasticsearchConfig");

const normalizeDate = (dateValue) => {
  if (!dateValue) return null;
  const d = new Date(dateValue);
  return isNaN(d.getTime()) ? null : d.toISOString();
};

const fetchClosedDocsFromElastic = async (index, numbers, fields) => {
  if (!numbers.length) return [];
  const response = await client.search({
    index,
    size: Math.min(numbers.length, 10000),
    _source: fields,
    query: { terms: { "number.keyword": numbers } },
  });

  return response.hits.hits.map((hit) => hit._source);
};

const bulkUpdateClosedDocs = async (index, updates) => {
  if (!updates || updates.length === 0) {
    console.log("No closed documents need updates.");
    return;
  }

  const BULK_UPDATE_BATCH_SIZE = 1000;
  let totalUpdated = 0;
  let totalErrors = 0;

  for (let i = 0; i < updates.length; i += BULK_UPDATE_BATCH_SIZE) {
    const batchUpdates = updates.slice(i, i + BULK_UPDATE_BATCH_SIZE);
    
    const body = batchUpdates.flatMap((doc) => {
      const partialUpdate = {};
      if (doc.close_category !== undefined && doc.close_category !== null)
        partialUpdate.close_category = doc.close_category;
      if (doc.close_notes !== undefined && doc.close_notes !== null)
        partialUpdate.close_notes = doc.close_notes;
      if (doc.actual_start !== undefined && doc.actual_start !== null)
        partialUpdate.actual_start = doc.actual_start;
      if (doc.actual_end !== undefined && doc.actual_end !== null)
        partialUpdate.actual_end = doc.actual_end;
      if (doc.state !== undefined && doc.state !== null)
        partialUpdate.state = doc.state;

      return [
        { update: { _index: index, _id: doc.number } },
        { doc: partialUpdate, doc_as_upsert: false },
      ];
    });

    if (body.length === 0) {
      continue;
    }

    try {
      const response = await client.bulk({ 
        refresh: false,
        body,
        timeout: '60s'
      });

      if (response.errors) {
        const errorItems = response.items.filter((i) => i.update?.error);
        totalErrors += errorItems.length;
        if (errorItems.length > 0) {
          console.warn(`⚠️ Sub-batch ${Math.floor(i / BULK_UPDATE_BATCH_SIZE) + 1}: ${errorItems.length} errors`);
          if (errorItems.length < 5) {
            errorItems.forEach(item => {
              console.warn(`   Error for ${item.update._id}: ${item.update.error.reason || item.update.error}`);
            });
          }
        }
      } else {
        totalUpdated += batchUpdates.length;
      }
    } catch (error) {
      console.error(`❌ Error updating sub-batch: ${error.message}`);
    }
  }

  if (totalErrors > 0) {
    console.log(`⚠️ Updated ${totalUpdated} documents with ${totalErrors} errors.`);
  } else {
    console.log(`✅ Bulk updated ${totalUpdated} closed documents.`);
  }
};

const processParquetFileInChunks = async (filePath, chunkSize, onChunk) => {
  const db = new duckdb.Database(":memory:");
  const conn = db.connect();

  try {
    const escapedPath = filePath.replace(/'/g, "''");
    
    console.log("📂 Counting closed records...");
    const countResult = await new Promise((resolve, reject) => {
      conn.all(`
        SELECT COUNT(*) AS total
        FROM read_parquet('${escapedPath}')
        WHERE state = 'Closed' AND number IS NOT NULL
      `, (err, rows) => {
        if (err) return reject(err);
        resolve(rows);
      });
    });

    const totalClosed = Number(countResult[0]?.total || 0);
    console.log(`📊 Found ${totalClosed} closed records`);

    if (totalClosed === 0) return;

    let offset = 0;
    while (offset < totalClosed) {
      const rows = await new Promise((resolve, reject) => {
        conn.all(`
          SELECT number, close_category, close_notes, actual_start, actual_end, state
          FROM read_parquet('${escapedPath}')
          WHERE state = 'Closed' AND number IS NOT NULL
          ORDER BY number
          LIMIT ${chunkSize} OFFSET ${offset}
        `, (err, rows) => {
          if (err) return reject(err);
          resolve(rows);
        });
      });

      if (!rows.length) break;

      const normalized = rows.map(r => ({
        number: r.number != null ? String(r.number) : null,
        close_category: r.close_category || null,
        close_notes: r.close_notes || null,
        actual_start: normalizeDate(r.actual_start),
        actual_end: normalizeDate(r.actual_end),
        state: r.state || null,
      }));

      await onChunk(normalized, offset, totalClosed);
      offset += chunkSize;
    }
  } finally {
    conn.close();
  }
};

const checkAndUpdateClosedDocs = async (parquetData, index) => {
  console.log("🔍 Checking closed records for field updates...");
  const closedRecords = parquetData.filter(
    (r) => r.state && r.state === "Closed" && r.number
  );

  if (closedRecords.length === 0) {
    console.log("No closed records found in parquet data.");
    return;
  }

  console.log(`📊 Found ${closedRecords.length} closed records`);

  const chunkSize = 5000;
  const esBatchSize = 5000;
  let totalUpdated = 0;

  for (let i = 0; i < closedRecords.length; i += chunkSize) {
    const chunk = closedRecords.slice(i, i + chunkSize);
    const numbers = [...new Set(chunk.map((r) => String(r.number)).filter(Boolean))];
    
    if (numbers.length === 0) continue;

    const allUpdates = [];

    for (let j = 0; j < numbers.length; j += esBatchSize) {
      const numberBatch = numbers.slice(j, j + esBatchSize);

      const esDocs = await fetchClosedDocsFromElastic(index, numberBatch, [
        "number",
        "close_category",
        "close_notes",
        "actual_start",
        "actual_end",
        "state"
      ]);

      const esMap = new Map(esDocs.map((d) => [String(d.number), d]));

      const updates = chunk
        .filter((r) => esMap.has(String(r.number)))
        .filter((r) => {
          const es = esMap.get(String(r.number));
          if (!es) return false;
          const parquetActualStart = normalizeDate(r.actual_start);
          const parquetActualEnd = normalizeDate(r.actual_end);
          const esActualStart = normalizeDate(es.actual_start);
          const esActualEnd = normalizeDate(es.actual_end);

          return (
            (r.close_category && r.close_category !== es.close_category) ||
            (r.close_notes && r.close_notes !== es.close_notes) ||
            (parquetActualStart && parquetActualStart !== esActualStart) ||
            (parquetActualEnd && parquetActualEnd !== esActualEnd)
          );
        })
        .map((r) => {
          const normalizedActualStart = normalizeDate(r.actual_start);
          const normalizedActualEnd = normalizeDate(r.actual_end);

          return {
            number: String(r.number),
            close_category: r.close_category || null,
            close_notes: r.close_notes || null,
            actual_start: normalizedActualStart,
            actual_end: normalizedActualEnd,
            state: r.state
          };
        });

      allUpdates.push(...updates);
    }

    if (allUpdates.length > 0) {
      await bulkUpdateClosedDocs(index, allUpdates);
      totalUpdated += allUpdates.length;
      console.log(`🧩 Chunk ${Math.floor(i / chunkSize) + 1}: Updated ${allUpdates.length} closed documents`);
    }
  }

  console.log(`✅ Total closed docs updated: ${totalUpdated}`);
};

const checkClosedDocs = async (filePath, index) => {
  const chunkSize = 5000;
  const esBatchSize = 5000;
  let totalUpdated = 0;

  await processParquetFileInChunks(filePath, chunkSize, async (chunk, offset, total) => {
    const allUpdates = [];

    for (let i = 0; i < chunk.length; i += esBatchSize) {
      const batch = chunk.slice(i, i + esBatchSize);
      const numbers = batch.map(r => r.number);

      const esDocs = await fetchClosedDocsFromElastic(index, numbers, [
        "number",
        "close_category",
        "close_notes",
        "actual_start",
        "actual_end",
        "state"
      ]);

      const esMap = new Map(esDocs.map(d => [d.number, d]));
      const updates = batch.filter(r => {
        const es = esMap.get(r.number);
        if (!es) return false;
        return (
          (r.close_category && r.close_category !== es.close_category) ||
          (r.close_notes && r.close_notes !== es.close_notes) ||
          (r.actual_start && r.actual_start !== normalizeDate(es.actual_start)) ||
          (r.actual_end && r.actual_end !== normalizeDate(es.actual_end)) ||
          (r.state && r.state !== es.state)
        );
      });

      allUpdates.push(...updates);
    }

    totalUpdated += allUpdates.length;

    console.log(`🧩 Chunk ${(offset / chunkSize) + 1}: Found ${allUpdates.length} updates`);
    await bulkUpdateClosedDocs(index, allUpdates);
    const pct = Math.round(((offset + chunk.length) / total) * 100);
    console.log(`📦 Progress: ${pct}%`);
  });

  console.log(`✅ Total closed docs with updates: ${totalUpdated}`);
};

if (require.main === module) {
  (async () => {
    const rootDir = path.join(__dirname, "..");
    const parquetFiles = fs.readdirSync(rootDir).filter(f => f.endsWith(".parquet"));
    if (!parquetFiles.length) console.log("No parquet files found");
    
    const latestFile = parquetFiles.sort().reverse()[0];
    const filePath = path.join(rootDir, latestFile);
    const index = process.env.ELASTIC_INDEX || "change-risk-analysis";

    console.log(`🚀 Processing ${latestFile} using index: ${index}`);
    await checkClosedDocs(filePath, index);
  })();
}

module.exports = { checkAndUpdateClosedDocs };
