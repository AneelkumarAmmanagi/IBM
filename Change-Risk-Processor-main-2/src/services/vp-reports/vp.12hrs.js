const { vpMapping } = require("../../../constants/constants");
const { client } = require("../../databaseSetup/elasticsearchConfig");

function getServicesForOwner(ownerPlatform) {
    if (!ownerPlatform) return [];
    const ownerData = vpMapping.find(item => item.platform === ownerPlatform);
    return ownerData ? ownerData.services : [];
}

async function fetchChangesForWindow(hours, direction,platform, aiRiskScore,planned_start,planned_end) {
    const indexName = process.env.ELASTIC_INDEX || "change-risk-analysis";
    const size = 500;
    const allDocuments = [];

    let gteBound, lteBound;
    if ((hours === undefined || hours === null) && planned_start && planned_end) {
        gteBound = planned_start;
        lteBound = planned_end;
    } else if(direction){
        if (direction === "past") {
            gteBound = `now-${hours}h`;
            lteBound = "now";
        } else if (direction === "future") {
            gteBound = "now-1m";
            lteBound = `now+${hours}h`;
        } else {
            throw new Error("Invalid duration. Use 'past' or 'future'");
        }
    }else {
        const today = new Date();
        const yyyy = today.getFullYear();
        const mm = String(today.getMonth() + 1).padStart(2, "0");
        const dd = String(today.getDate()).padStart(2, "0");
    
        const todayStr = `${yyyy}-${mm}-${dd}`;
    
        gteBound = `${todayStr}T00:00:00`;
        lteBound = `${todayStr}T23:59:59`;
    }

    const ownerServices = getServicesForOwner(platform);
    const mustFilters = [
        {
            range: {
                planned_start: {
                    gte: gteBound,
                    lte: lteBound,
                },
            },
        }
    ];

    if (aiRiskScore !== undefined) {
        mustFilters.push({
            range: {
                "analysis_result.final_score": {
                    gte: aiRiskScore,
                },
            },
        });
    }
    
    if (ownerServices.length > 0) {
        mustFilters.push({
          terms: {
            "service_names.keyword": ownerServices
          }
        });
      }

      console.log("mustFilters",JSON.stringify(mustFilters,null,2));
      
      const searchResponse = await client.search({
        index: indexName,
        scroll: "30s",
        size: size,
        sort: ["_doc"],
        query: {
          bool: {
            must: mustFilters,
          },
        },
      });      

    let scrollId = searchResponse._scroll_id;
    let documents = searchResponse.hits.hits;
    const total = searchResponse.hits.total.value;

    documents.forEach((doc) => {
        allDocuments.push({
            id: doc._id,
            ...doc._source
        });
    });

    while (documents.length > 0 && allDocuments.length < total) {
        const scrollResponse = await client.scroll({
            scroll_id: scrollId,
            scroll: "30s",
        });

        scrollId = scrollResponse._scroll_id;
        documents = scrollResponse.hits.hits;

        documents.forEach((doc) => {
            allDocuments.push({
                id: doc._id,
                ...doc._source,
            });
        });
    }

    if (scrollId) {
        await client.clearScroll({ scroll_id: scrollId });
    }

    return allDocuments;
}

async function fetchPastOrFutureChanges(hours, duration,platform,aiRiskScore,planned_start="",planned_end="") {
    try {
        const docs = await fetchChangesForWindow(hours, duration,platform,aiRiskScore,planned_start,planned_end);

        return {
            success: true,
            total: docs.length,
            documents: docs,
        };
    } catch (error) {
        console.error(`Error fetching ${duration} changes:`, error.message);
        throw error;
    }
}

module.exports = {
    fetchPastOrFutureChanges,
};
