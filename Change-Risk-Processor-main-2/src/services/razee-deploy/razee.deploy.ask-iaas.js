const { client } = require("../../databaseSetup/elasticsearchConfig");

const DEFAULT_SERVICE_NAME = "is-razee-deploy";
const DEFAULT_INDEX_NAME =
  process.env.ELASTIC_INDEX || "change-risk-analysis";

async function getRazeeDeployChanges({
  page = 1,
  size = 100,
  timeRangeStart,
  timeRangeEnd,
  timeRangeField = "planned_start",
  sortField = "planned_start",
  sortOrder = "desc",
  region,
  regions,
  serviceNames,
  timeRange
} = {}) {
  const parsedPage = Number.isFinite(Number(page)) && Number(page) > 0 ? Number(page) : 1;
  const parsedSize =
    Number.isFinite(Number(size)) && Number(size) > 0 && Number(size) <= 1000
      ? Number(size)
      : 100;
  const from = (parsedPage - 1) * parsedSize;

  const must = [];

  const normalizedServiceNames = Array.isArray(serviceNames)
    ? serviceNames.filter((name) => typeof name === "string" && name.trim().length > 0)
    : [];
  const effectiveServiceNames =
    normalizedServiceNames.length > 0
      ? normalizedServiceNames
      : [DEFAULT_SERVICE_NAME];

  if (effectiveServiceNames.length === 1) {
    must.push({
      term: {
        "service_names.keyword": effectiveServiceNames[0],
      },
    });
  } else {
    must.push({
      terms: {
        "service_names.keyword": effectiveServiceNames,
      },
    });
  }

  const normalizedRegions = Array.isArray(regions)
    ? regions.filter((value) => typeof value === "string" && value.trim().length > 0)
    : [];

  if (normalizedRegions.length > 0) {
    must.push({
      terms: {
        "regions.keyword": normalizedRegions,
      },
    });
  } else if (typeof region === "string" && region.trim().length > 0) {
    must.push({
      term: {
        "regions.keyword": region.trim(),
      },
    });
  }

  const safeField =
    typeof timeRangeField === "string" && timeRangeField.trim().length > 0
      ? timeRangeField
      : "planned_start";

  const twelveHoursAgoIso = new Date(Date.now() - 12 * 60 * 60 * 1000).toISOString();
  const nowIso = new Date().toISOString();
  let gte = timeRangeStart || twelveHoursAgoIso;
  let lte = timeRangeEnd || nowIso;

  if(timeRange){
    gte = timeRange.start;
    lte = timeRange.end;
  }

  must.push({
    range: {
      [safeField]: {
        gte,
        lte,
      },
    },
  });

  console.log("musttttt",JSON.stringify(must,null,2))

  const safeSortField =
    typeof sortField === "string" && sortField.trim().length > 0
      ? sortField
      : "planned_start";
  const normalizedSortOrder =
    typeof sortOrder === "string" && sortOrder.toLowerCase() === "asc"
      ? "asc"
      : "desc";

  try {
    const response = await client.search({
      index: DEFAULT_INDEX_NAME,
      from,
      size: parsedSize,
      _source: [
        "number",
        "planned_start",
        "planned_end",
        "state",
        "analysis_result.final_score",
        "analysis_result.change_summary",
        "workspace",
        "service_names",
        "tribe",
        "regions",
        "created",
      ],
      sort: [{ [safeSortField]: { order: normalizedSortOrder } }],
      query: {
        bool: {
          must,
        },
      },
    });

    const total = response?.hits?.total?.value || 0;
    const documents =
      response?.hits?.hits?.map((hit) => ({
        id: hit?._id,
        ...hit?._source,
      })) || [];
console.log("documents",documents.length,JSON.stringify(documents,null,2))
    return {
      success: true,
      total,
      page: parsedPage,
      size: parsedSize,
      documents,
    };
  } catch (error) {
    console.error(
      "Error fetching IS Razee Deploy changes from Elasticsearch:",
      error?.message || error
    );
    throw error;
  }
}

module.exports = {
  getRazeeDeployChanges,
};

