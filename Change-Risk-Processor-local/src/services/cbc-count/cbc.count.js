const { client } = require("../../databaseSetup/elasticsearchConfig");

function normalizeServiceNameData(rawData) {
    const cleaned = {};
  
    for (const [key, value] of Object.entries(rawData)) {
      let cleanedKey = key
        .replace(/^\[["']?/, '') 
        .replace(/["']?\]$/, '');
      cleaned[cleanedKey] = (cleaned[cleanedKey] || 0) + value;
    }
  
    return cleaned;
  }
  

async function getCbcChangesCount() {
    const indexName = process.env.CBC_CHANGES_INDEX || "cbc-changes";
    
    try {
      console.log(`Searching for CBC changes in index: ${indexName}`);
  
      const result = await client.search({
        index: indexName,
        body: {
          size: 0,
          aggs: {
            template_name_counts: {
              terms: {
                field: "pipeline_name.keyword",
                size: 1000
              }
            }
          }
        }
      });
  
      const buckets = result.aggregations?.template_name_counts?.buckets || [];
  
      const serviceNameCounts = buckets.reduce((acc, bucket) => {
        acc[bucket.key] = bucket.doc_count;
        return acc;
      }, {});

      const cleanedServiceNameCounts = normalizeServiceNameData(serviceNameCounts);
  
      console.log("Service name counts:", cleanedServiceNameCounts);
  
      return {
        success: true,
        count: result.hits.total.value,
        data: cleanedServiceNameCounts,
        message: "Successfully retrieved service name counts"
      };
    } catch (error) {
      console.error("Error fetching CBC changes count:", error?.message);
      throw {
        message: error.message || "Failed to fetch CBC changes count",
        stack: error.stack
      };
    }
  };

async function enrichCbcChangesWithPipeline() {
    const CBC_INDEX = process.env.CBC_CHANGES_INDEX || "cbc-changes";
    const CRA_INDEX = process.env.CHANGE_RISK_ANALYSIS_INDEX || "change-risk-analysis";
    const PAGE_SIZE = 1000;      // scroll page size
    const BULK_BATCH = 500;      // how many updates per bulk call
  
    /** ------------------------------------------------------------------
     * 1️⃣ Collect all CBC docs + their change numbers via the scroll API
     * ------------------------------------------------------------------*/
    const cbcDocs = [];
    let scrollId;
  
    try {
      let resp = await client.search({
        index: CBC_INDEX,
        scroll: "2m",
        size: PAGE_SIZE,
        _source: ["caused_by_change_number"],  // we only need the change #
        body: { query: { exists: { field: "caused_by_change_number" } } }
      });
  
      while (resp.hits.hits.length) {
        resp.hits.hits.forEach(hit => {
          const changeNumber = hit._source.caused_by_change_number;
          console.log("changeNumber", changeNumber);
          if (changeNumber) {
            cbcDocs.push({ id: hit._id, changeNumber });
          }
        });
  
        scrollId = resp._scroll_id;
        resp = await client.scroll({ scroll_id: scrollId, scroll: "2m" });
      }
    } finally {
      if (scrollId) {
        await client.clearScroll({ scroll_id: scrollId });
      }
    }
  
    if (!cbcDocs.length) {
      console.log("No CBC documents with caused_by_change_number found.");
      return { success: true, updated: 0 };
    }
  
    /** ------------------------------------------------------------------
     * 2️⃣ Build a lookup map: change_number  ->  pipeline_name
     *     (done in one terms query, handles up to 10 k unique numbers)
     * ------------------------------------------------------------------*/
    const uniqueChangeNumbers = [
      ...new Set(cbcDocs.map(d => d.changeNumber))
    ];
  
    const pipelineLookup = {};
    let from = 0;
    const TERMS_LIMIT = 10000;            // ES max terms per query
  console.log("uniqueChangeNumbers", uniqueChangeNumbers);
    while (from < uniqueChangeNumbers.length) {
      const batch = uniqueChangeNumbers.slice(from, from + TERMS_LIMIT);
      from += TERMS_LIMIT;
  
      const craResp = await client.search({
        index: CRA_INDEX,
        size: batch.length,
        _source: ["number", "pipeline_name"],
        body: {
          query: {
            terms: {
              "number.keyword": batch
            }
          }
        }
      });
  
      craResp.hits.hits.forEach(hit => {
        const {number, pipeline_name } = hit._source;
        console.log("change_number", number);
        console.log("pipeline_name", pipeline_name);
        pipelineLookup[number] = pipeline_name ?? null;
      });
    }
  
    /** ------------------------------------------------------------------
     * 3️⃣ Bulk‑update CBC docs with the pipeline_name
     * ------------------------------------------------------------------*/
    let updated = 0;
    const bulkOps= [];
  
    for (const { id, changeNumber } of cbcDocs) {
      const pipeline = pipelineLookup[changeNumber] ?? null;
      if (pipeline === null) continue;          // nothing to add
  
      bulkOps.push(
        { update: { _index: CBC_INDEX, _id: id } },
        { doc: { pipeline_name: pipeline } }
      );
  
      if (bulkOps.length >= BULK_BATCH * 2) {
        await client.bulk({ refresh: false, body: bulkOps });
        updated += BULK_BATCH;
        bulkOps.length = 0;
      }
    }
  
    // send any leftovers
    if (bulkOps.length) {
      await client.bulk({ refresh: false, body: bulkOps });
      updated += bulkOps.length / 2;
    }
  
    console.log(`Enriched ${updated} CBC documents with pipeline_name.`);
    return { success: true, updated };
  }
  

module.exports = {
  getCbcChangesCount,
  enrichCbcChangesWithPipeline
}; 