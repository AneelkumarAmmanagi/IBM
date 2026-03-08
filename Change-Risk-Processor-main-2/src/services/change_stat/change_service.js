const { client } = require("../../databaseSetup/elasticsearchConfig.js");
const indexName = 'change-stat-test-shaj';
// const indexName = process.env.ELASTIC_INDEX || 'change-risk-analysis';


async function getCbcIncidents(start, end) {
  const groupedByCbcNumber = {};
  try {
    const body = {
      fields: ["created", "number", "tribe", "service_names", "upstream_problem", "close_notes", "status", "caused_by_change_number", "problem"],
      where: `(status = 'confirmed_cie' AND caused_by_change='true' AND upstream_problem = '' AND created>='${start}' AND created<='${end}')`
    };

    const fetchRelatedIncidentsResponse = await fetch(
      `${process.env.DATASYNC_SERVER_URL}/incidents/customQuery`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-api-key": `${process.env.DATASYNC_SERVER_API_KEY}`,
        },
        body: JSON.stringify(body)
      }
    );

    if (!fetchRelatedIncidentsResponse.ok) {
      const errorData = await fetchRelatedIncidentsResponse.json();
      throw new Error(
        errorData.error ||
          `Request failed with status ${fetchRelatedIncidentsResponse.status}`
      );
    }

    const data = await fetchRelatedIncidentsResponse.json();
    const incidents = data || [];

    return incidents;
  } catch (err) {
    console.log("Err from grouped incidents", err?.message);
    throw err;
  }
}

async function getCbcIncidents2(start, end, fields = ["created", "service_names", "upstream_problem", "close_notes", "status", "caused_by_change_number", "problem"]) {
  const docSize = 10000;
  const cbcIndexName = 'cbc_incidents';
  const gteBound = start;
  const lteBound = end;
  try {
    const searchResponse = await client.search({
      _source: fields,
      index: cbcIndexName,
      size: docSize,
      body: {
        query: {
          bool: {
            filter: [
              {
                range: {
                  created: {
                    gte: gteBound,
                    lte: lteBound
                  }
                }
              }
            ]
          },
        },
      },
    });

    const total = searchResponse.hits.total.value;
    const documents = searchResponse.hits.hits.map((doc) => ({
      id: doc._id,
      ...doc._source,
    }));

    return {
      success: true,
      total,
      docSize,
      documents,
    };
  } catch (error) {
    console.error("Error fetching documents:", error?.message);
    console.error("Full Error Stack:", error);
    throw {
      message: error.message,
      stack: error.stack,
    };
  }
}

async function getClosedCRsByActualEndTime(start, end, onlyProduction = false, fields = ["_id",
  "actual_start",
  "actual_end",
  "planned_start",
  "planned_end",
  "state",
  "service_environment",
  "tribe",
  "service_names",
  "close_category",
  "deployment_method",
  "close_notes", 
  "number",
  "planned_duration",
  "analysis_result.change_summary.business_justification",
  // "comments",
]) {
  const docSize = 10000;
  const gteBound = start;
  const lteBound = end;
  try {
    const filter = [
      // {
      //   "term": {
      //     "state.keyword": "Closed"
      //   }
      // },
      {
        range: {
          actual_end: {
            gte: gteBound,
            lte: lteBound
          }
        }
      }
    ];

    if (onlyProduction) {
      filter.push(
        {
          "term": {
            "service_environment.keyword": "production"
          }
        });
    }
    const searchResponse = await client.search({
      _source: fields,
      index: indexName,
      size: docSize,
      body: {
        query: {
          bool: {
            filter: filter
          },
        },
        "sort": [
          {
            "actual_end": {
              "order": "desc"
            }
          }
        ],
      },
    });

    const total = searchResponse.hits.total.value;
    const documents = searchResponse.hits.hits.map((doc) => ({
      id: doc._id,
      ...doc._source,
    }));

    return {
      success: true,
      total,
      docSize,
      documents,
    };
  } catch (error) {
    console.error("Error fetching documents:", error?.message);
    console.error("Full Error Stack:", error);
    throw {
      message: error.message,
      stack: error.stack,
    };
  }
}

async function getCRsByActualStartTime(start, end, fields = ["_id",
  "actual_start",
  "actual_end",
  "planned_start",
  "planned_end",
  "state",
  "service_environment",
  "tribe",
  "service_names",
  "close_category",
  "deployment_method",
  "close_notes",
  "number",
  "planned_duration",
  "analysis_result.change_summary.business_justification",
  // "comments",
]) {
  const docSize = 10000;
  const gteBound = start;
  const lteBound = end;
  try {
    const searchResponse = await client.search({
      _source: fields,
      index: indexName,
      size: docSize,
      body: {
        query: {
          bool: {
            filter: [
              {
                "term": {
                  "service_environment.keyword": "production"
                }
              },
              {
                range: {
                  actual_start: {
                    gte: gteBound,
                    lte: lteBound
                  }
                }
              }
            ]
          },
        },
      },
    });

    const total = searchResponse.hits.total.value;
    const documents = searchResponse.hits.hits.map((doc) => ({
      id: doc._id,
      ...doc._source,
    }));

    return {
      success: true,
      total,
      docSize,
      documents,
    };
  } catch (error) {
    console.error("Error fetching documents:", error?.message);
    console.error("Full Error Stack:", error);
    throw {
      message: error.message,
      stack: error.stack,
    };
  }
}

async function getCRsNotClosedAsExpected(start, end, fields = ["_id",
  "actual_start",
  "actual_end",
  "planned_start",
  "planned_end",
  "state",
  "service_environment",
  "tribe",
  "service_names",
  "close_category",
  "deployment_method",
  "close_notes",
  "number",
  "planned_duration",
  "analysis_result.change_summary.business_justification",
  // "comments",
]) {
  const docSize = 10000;
  const gteBound = start;
  const lteBound = end;
  try {
    const searchResponse = await client.search({
      _source: fields,
      index: indexName,
      size: docSize,
      body: {
        query: {
          bool: {
            "must_not": [
              {
                "exists": {
                  "field": "actual_end"
                }
              },
              {
                "term": {
                  "state.keyword": "Closed"
                }
              }
            ],
            filter: [
              {
                "term": {
                  "service_environment.keyword": "production"
                }
              },
              {
                range: {
                  actual_start: {
                    gte: gteBound,
                    lte: lteBound
                  }
                }
              },
              {
                range: {
                  planned_end: {
                    // gte: gteBound,
                    lte: lteBound
                  }
                }
              }
            ]
          },
        },
      },
    });

    const total = searchResponse.hits.total.value;
    const documents = searchResponse.hits.hits.map((doc) => ({
      id: doc._id,
      ...doc._source,
    }));

    return {
      success: true,
      total,
      docSize,
      documents,
    };
  } catch (error) {
    console.error("Error fetching documents:", error?.message);
    console.error("Full Error Stack:", error);
    throw {
      message: error.message,
      stack: error.stack,
    };
  }
}

async function getImproperCRsByActualStartTime(start, end, fields = ["_id",
  "actual_start",
  "actual_end",
  "planned_start",
  "planned_end",
  "state",
  "service_environment",
  "tribe",
  "service_names",
  "close_category",
  "deployment_method",
  "close_notes",
  "number",
  "planned_duration",
  "comments",
  "analysis_result.change_summary.business_justification",
]) {
  const docSize = 10000;
  const gteBound = start;
  const lteBound = end;
  try {
    const searchResponse = await client.search({
      _source: fields,
      index: indexName,
      size: docSize,
      body: {
        query: {
          bool: {
            "should": [
              {
                "match_phrase": {
                  "comments": "Business Hours - please reschedule off business hours"
                }
              },
              {
                "match_phrase": {
                  "comments": "Template / Pipeline Name is missing -- your change was not created properly, IAAS Change Process requires changes to be created from the standard change catalog"
                }
              },
              {
                "match_phrase": {
                  "comments": "Validation Record - please update the validation record field with proof of testing"
                }
              },
              {
                "match_phrase": {
                  "comments": "Populate the Validation Record field with Proof of Testing or explanation of why change couldn't be tested"
                }
              }
            ],
            "minimum_should_match": 1,
            filter: [
              {
                "term": {
                  "service_environment.keyword": "production"
                }
              },
              {
                range: {
                  actual_start: {
                    gte: gteBound,
                    lte: lteBound
                  }
                }
              }
            ]
          },
        },
      },
    });

    const total = searchResponse.hits.total.value;
    const documents = searchResponse.hits.hits.map((doc) => ({
      id: doc._id,
      ...doc._source,
    }));
    // console.log("getImproperCRsByActualStartTime: documents:", documents);

    return {
      success: true,
      total,
      docSize,
      documents,
    };
  } catch (error) {
    console.error("Error fetching documents:", error?.message);
    console.error("Full Error Stack:", error);
    throw {
      message: error.message,
      stack: error.stack,
    };
  }
}

module.exports = {
  getCbcIncidents,
  getClosedCRsByActualEndTime,
  getCRsByActualStartTime,
  getCRsNotClosedAsExpected,
  getImproperCRsByActualStartTime,
}