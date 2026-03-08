const { getServicesForOwner } = require("./utils/access");
const { client } = require("../../databaseSetup/elasticsearchConfig");

function getDayBounds(daySpecifier) {
  if (!daySpecifier) {
    return null;
  }

  const normalized = daySpecifier.trim().toLowerCase();
  const offsets = {
    today: 0,
    yesterday: -1,
  };

  if (!(normalized in offsets)) {
    return null;
  }

  const targetDate = new Date();
  targetDate.setDate(targetDate.getDate() + offsets[normalized]);

  const yyyy = targetDate.getFullYear();
  const mm = String(targetDate.getMonth() + 1).padStart(2, "0");
  const dd = String(targetDate.getDate()).padStart(2, "0");

  const dateStr = `${yyyy}-${mm}-${dd}`;
  return {
    gte: `${dateStr}T00:00:00`,
    lte: `${dateStr}T23:59:59`,
  };
}

function mapDocumentToResponse(doc) {
  return {
    number: doc._source.number || doc._id,
    planned_start: doc._source.planned_start || null,
    tribe: doc._source.tribe || null,
    regions: doc._source.regions || [],
    state: doc._source.state || null,
    planned_end: doc._source.planned_end || null,
    analysis_result: {
      final_score: doc._source.analysis_result?.final_score,
      change_summary: doc._source.analysis_result?.change_summary?.business_justification,
    },
  };
}

async function fetchChangesForWindow(
  platform,
  daySpecifier,
  ownerEmail
) {
  const indexName = process.env.ELASTIC_INDEX || "change-risk-analysis";
  const size = 500;
  const allDocuments = [];

  let gteBound;
  let lteBound;

  const dayBounds = getDayBounds(daySpecifier);

  if (dayBounds) {
    gteBound = dayBounds.gte;
    lteBound = dayBounds.lte;
  } else {
    const today = new Date();
    const yyyy = today.getFullYear();
    const mm = String(today.getMonth() + 1).padStart(2, "0");
    const dd = String(today.getDate()).padStart(2, "0");

    const todayStr = `${yyyy}-${mm}-${dd}`;

    gteBound = `${todayStr}T00:00:00`;
    lteBound = `${todayStr}T23:59:59`;
  }

  const ownerServices = getServicesForOwner(platform, ownerEmail);
  const mustFilters = [
    {
      range: {
        planned_start: {
          gte: gteBound,
          lte: lteBound,
        },
      },
    },
  ];

  if (ownerServices.length > 0) {
    mustFilters.push({
      terms: {
        "service_names.keyword": ownerServices,
      },
    });
  }

  const searchResponse = await client.search({
    index: indexName,
    scroll: "30s",
    size,
    _source: [
      "number",
      "planned_start",
      "planned_end",
      "tribe",
      "regions",
      "state",
      "analysis_result.final_score",
      "analysis_result.change_summary",
    ],
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
    allDocuments.push(mapDocumentToResponse(doc));
  });

  while (documents.length > 0 && allDocuments.length < total) {
    const scrollResponse = await client.scroll({
      scroll_id: scrollId,
      scroll: "30s",
    });

    scrollId = scrollResponse._scroll_id;
    documents = scrollResponse.hits.hits;

    documents.forEach((doc) => {
      allDocuments.push(mapDocumentToResponse(doc));
    });
  }

  if (scrollId) {
    await client.clearScroll({ scroll_id: scrollId });
  }

  return allDocuments;
}

async function fetchAskIaasChanges({
  platform,
  email,
  day,
} = {}) {
  try {
    const docs = await fetchChangesForWindow(
      platform,
      day,
      email
    );

    return {
      success: true,
      total: docs.length,
      documents: docs,
    };
  } catch (error) {
    console.error("Error fetching Ask IaaS changes:", error.message);
    throw error;
  }
}

module.exports = {
  fetchAskIaasChanges,
};

