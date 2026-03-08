const { vpMapping } = require("../../../constants/constants");
const { client } = require("../../databaseSetup/elasticsearchConfig");

const DEFAULT_INDEX = process.env.ELASTIC_INDEX || "change-risk-analysis";
const DEFAULT_SCROLL = "30s";
const DEFAULT_SIZE = 500;

const getTodayBounds = () => {
  const today = new Date();
  const yyyy = today.getFullYear();
  const mm = String(today.getMonth() + 1).padStart(2, "0");
  const dd = String(today.getDate()).padStart(2, "0");

  const dateStr = `${yyyy}-${mm}-${dd}`;
  return {
    gte: `${dateStr}T00:00:00`,
    lte: `${dateStr}T23:59:59`,
  };
};

const mapDocumentToSummary = (docSource = {}, fallbackId) => {
  const regionArray = Array.isArray(docSource.regions) ? docSource.regions : [];
  const primaryRegion = regionArray[0] || docSource.region || "Unknown";
  const changeSummary = docSource.analysis_result?.change_summary;
  const score = docSource.analysis_result?.final_score;
  return {
    number:
      docSource.number ||
      fallbackId ||
      "N/A",
    description:
      changeSummary ||
      docSource.short_description ||
      docSource.summary ||
      docSource.description ||
      "No description available",
    region: primaryRegion,
    score:score
  };
};

const fetchManualChangesForPlatform = async (mapping, options = {}) => {
  const { aiRiskScore } = options;
  const { gte, lte } = getTodayBounds();
  const filters = [
    {
      range: {
        planned_start: {
          gte,
          lte,
        },
      },
    },
    {
      term: {
        "deployment_method.keyword": "manual",
      },
    },
  ];

  if (Array.isArray(mapping.services) && mapping.services.length > 0) {
    filters.push({
      terms: {
        "service_names.keyword": mapping.services,
      },
    });
  }


  const response = await client.search({
    index: DEFAULT_INDEX,
    scroll: DEFAULT_SCROLL,
    size: DEFAULT_SIZE,
    sort: ["_doc"],
    _source: [
      "number",
      "region",
      "analysis_result.change_summary",
      "regions",
      "analysis_result.final_score"
    ],
    query: {
      bool: {
        must: filters,
      },
    },
  });

  let scrollId = response._scroll_id;
  let documents = response.hits.hits;
  const total = response.hits.total.value;

  const mappedDocs = documents.map((doc) =>
    mapDocumentToSummary(doc._source, doc._id)
  );

  while (documents.length > 0 && mappedDocs.length < total) {
    const scrollResponse = await client.scroll({
      scroll_id: scrollId,
      scroll: DEFAULT_SCROLL,
    });

    scrollId = scrollResponse._scroll_id;
    documents = scrollResponse.hits.hits;

    documents.forEach((doc) => {
        console.log("docmapped",JSON.stringify(doc._source,null,4))
      mappedDocs.push(mapDocumentToSummary(doc._source, doc._id));
    });
  }

  if (scrollId) {
    await client.clearScroll({ scroll_id: scrollId });
  }

  return mappedDocs;
};

const filterPlatforms = (platforms) => {
  if (!Array.isArray(platforms) || platforms.length === 0) {
    return vpMapping;
  }

  return vpMapping.filter((entry) => platforms.includes(entry.platform));
};

const fetchTodaysManualVpChanges = async (options = {}) => {
  const { aiRiskScore, platforms } = options;
  const platformEntries = filterPlatforms(platforms);

  const results = await Promise.all(
    platformEntries.map(async (mapping) => {
      try {
        const documents = await fetchManualChangesForPlatform(mapping, {
          aiRiskScore,
        });

        return {
          platform: mapping.platform,
          serviceGroups: mapping.serviceGroups || [],
          services: mapping.services || [],
          total: documents.length,
          documents,
        };
      } catch (error) {
        console.error(
          `Error fetching manual changes for platform ${mapping.platform}:`,
          error?.message || error
        );
        return {
          platform: mapping.platform,
          serviceGroups: mapping.serviceGroups || [],
          services: mapping.services || [],
          total: 0,
          documents: [],
          error: error?.message || "Unknown error",
        };
      }
    })
  );

  return results;
};

module.exports = {
  fetchTodaysManualVpChanges,
};

