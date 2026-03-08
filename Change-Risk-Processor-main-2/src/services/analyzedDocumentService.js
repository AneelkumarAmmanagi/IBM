const { client } = require("../databaseSetup/elasticsearchConfig");
const axios = require("axios");
const path = require("path");
const fs = require("fs");
const { parseISO, isValid, parse } = require("date-fns");
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
dayjs.extend(utc);
const {
  fetchElasticDocumentToClose,
  fetchElasticClosedDocument,
} = require("./bulkDocsUpload");
const duckdb = require("duckdb");
const updateGroupedServiceDocument = require("./error-budget/monthly.outage.doc.update");

const axiosConfig = {
  auth: {
    username: "apikey",
    password: process.env.CHANGE_REQUEST_API_KEY,
  },
  headers: {
    Accept: "application/json",
    "Content-Type": "application/json",
  },
};

async function fetchChangeRequestState(changeRequestId) {
  try {
    const url = `${process.env.CHANGE_REQUEST_API_URL}/changemgmt/api/v3/change_requests/${changeRequestId}`;
    const response = await axios.get(url, axiosConfig);
    return [undefined, response.data?.state || "Unknown"];
  } catch (error) {
    console.error(
      `Error fetching state for change request ${changeRequestId}:`,
      error?.message
    );
    return [error?.message, undefined];
  }
}

async function getAnalyzedDocuments(page = 1, size = 10) {
  const indexName = process.env.ELASTIC_INDEX || "change-risk-analysis";
  const from = (page - 1) * size;

  try {
    const result = await client.search({
      index: indexName,
      body: {
        query: {
          exists: {
            field: "analysis_result",
          },
        },
        sort: [{ analyzed_at: { order: "desc" } }],
      },
      from,
      size,
    });

    const total = result.hits.total.value;
    const documents = result.hits.hits.map((doc) => ({
      id: doc._id,
      ...doc._source,
    }));

    return {
      total,
      page,
      size,
      documents,
    };
  } catch (error) {
    console.error("Error fetching analyzed documents:", error?.message);
    throw {
      message: error.message,
      stack: error.stack,
    };
  }
}

async function getAllDocumentsIncludingClosed(page, size = 1000, timeRange) {
  const indexName = process.env.ELASTIC_INDEX || "change-risk-analysis";
  const allDocuments = [];
  const groupedByTribe = {};
  const groupedByRisk = {};

  try {
    // Open a Point-in-Time (PIT) context
    const { id: pitId } = await client.openPointInTime({
      index: indexName,
      keep_alive: "1m",
    });

    let searchAfter = undefined;
    let hasMore = true;

    while (hasMore) {
      const response = await client.search({
        size,
        pit: {
          id: pitId,
          keep_alive: "1m",
        },
        sort: [{ created: "asc" }],
        _source: ["tribe", "created", "state", "analysis_result.final_score"],
        query: {
          range: {
            created: {
              gte: `now-${timeRange}/d`,
              lt: "now/d",
            },
          },
        },
        search_after: searchAfter,
      });

      const hits = response.hits.hits;
      if (hits.length === 0) {
        hasMore = false;
        break;
      }

      const newDocs = hits.map((doc) => ({
         id: doc._id,
         tribe: doc._source.tribe,
         created: doc._source.created,
         state: doc._source.state,
         risk_score: doc._source.analysis_result?.final_score,
       }));

       allDocuments.push(...newDocs);


      searchAfter = hits[hits.length - 1].sort;

      for (const doc of newDocs) {
        const tribe = doc.tribe || "Unknown";
        const state = doc.state || "Other";

        if (!groupedByTribe[tribe]) {
          groupedByTribe[tribe] = {};
        }

        if (!groupedByTribe[tribe][state]) {
          groupedByTribe[tribe][state] = 0;
        }

        groupedByTribe[tribe][state] += 1;

        let risk = 0;
        if (doc.risk_score && doc.risk_score && doc.risk_score != undefined) {
          risk = doc.risk_score;
          risk = Math.max(0, Math.min(10, risk));
          const bucket = Math.min(Math.floor(risk), 9);
          const riskInterval = `${bucket}-${bucket + 1}`;

          if (!groupedByRisk[tribe]) {
            groupedByRisk[tribe] = {};
          }

          if (!groupedByRisk[tribe][riskInterval]) {
            groupedByRisk[tribe][riskInterval] = 0;
          }

          groupedByRisk[tribe][riskInterval] += 1;
        }
      }

      console.log(`Fetched ${allDocuments.length} documents so far...`);
    }

    // Close the PIT to free resources
    await client.closePointInTime({ id: pitId });

    console.log(`Successfully retrieved ${allDocuments.length} documents`);
    return {
      success: true,
      total: allDocuments.length,
      grouped: groupedByTribe,
      groupedByRisk: groupedByRisk,
    };
  } catch (error) {
    console.error("Error fetching documents:", error.message);
    throw {
      message: error.message,
      stack: error.stack,
    };
  }
}

async function getAllDocuments(page = 1, size = 100, timeRangeStart, timeRangeEnd, timeRangeField) {
  const indexName = process.env.ELASTIC_INDEX || "change-risk-analysis";
  const from = (page - 1) * size;

  try {
    const must = [];
    if (timeRangeStart && timeRangeEnd) {
      const field = timeRangeField && typeof timeRangeField === "string" && timeRangeField.trim() !== ""
        ? timeRangeField
        : "created";
      must.push({
        range: {
          [field]: {
            gte: timeRangeStart,
            lte: timeRangeEnd,
          },
        },
      });
    }

    console.log("must",JSON.stringify(must,null,2))
    const searchResponse = await client.search({
      index: indexName,
      from,
      size,
      body: {
        sort: [{ [timeRangeField || "created"]: { order: "desc" } }],
        query: {
          bool: {
            must,
            must_not: [
              { term: { archived: true } },
              { term: { "state.keyword": "Closed" } },
            ],
          },
        },
      },
    });

    const total = searchResponse.hits.total.value || 0;
    const documents = searchResponse.hits.hits.map(doc => ({ id: doc._id, ...doc._source }));
    console.log("documents",documents.length);
    console.log("total",total);

    return {
      success: true,
      total,
      page,
      size,
      totalPages: Math.ceil(total / size),
      documents,
    };
  } catch (error) {
    console.error("❌ Error fetching all documents:", error);
    return {
      success: false,
      message: error.message,
      stack: error.stack,
    };
  }
}


async function getAllClosedDocumentsFromElastic(
  pageNo = 1,
  docSize = 100,
  timeRange,
  filters = {},
  hours = ""
) {
  const indexName = process.env.ELASTIC_INDEX || "change-risk-analysis";
  let sortField = "planned_end";
  let sortOrder = "desc";
  const intHours = parseInt(hours)

  // Build the query filters
  const must = [
    { term: { archived: true } },
    { range: { planned_start: { lte: "now" } } },
  ];

  // If hours > 0, add the planned_start range
  if (intHours > 0) {
    const gteBound = `now-${hours}h`;
    const lteBound = "now";
    must.push({
      range: {
        planned_start: {
          gte: gteBound,
          lte: lteBound,
        },
      },
    });
  }

// Ensure filters is a proper object
if (filters && typeof filters === "string") {
  try {
    filters = JSON.parse(filters); // parse if it's a JSON string
  } catch {
    console.warn("Filters was a string but not valid JSON:", filters);
    filters = {};
  }
}

if (filters && typeof filters === "object" && !Array.isArray(filters)) {
  for (const [key, value] of Object.entries(filters)) {
    if (typeof value === "object" && value !== null && value.range) {
      must.push({ range: { [key]: value.range } });
    } else {
      must.push({ term: { [key]: value } });
    }
  }
}


  const from = (pageNo - 1) * docSize;

  try {
    const searchResponse = await client.search({
      index: indexName,
      from,
      size: docSize,
      body: {
        query: {
          bool: {
            must,
          },
        },
        sort: [{ [sortField]: { order: sortOrder } }],
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
      pageNo,
      docSize,
      documents,
    };
  } catch (error) {
    console.error("Error fetching closed documents:", error?.message);
    console.error("Full Error Stack:", error);
    throw {
      message: error.message,
      stack: error.stack,
    };
  }
}

async function getChangeRequestsToArchive(validChangeRequests, index) {
  console.log(
    `Starting archive check process with ${validChangeRequests.length} closed change requests`
  );
  const indexName = process.env.ELASTIC_INDEX || "change-risk-analysis";
  const groupedByCbcNumber = {};

  try {
    console.log(`Searching for non-archived documents in index: ${indexName}`);
    const elasticDocs = await fetchElasticDocumentToClose(index);
    console.log(`Found ${elasticDocs.length} records in Elasticsearch`);
    // Create a Set for fast lookups of valid numbers
    const validNumbersSet = new Set(
      validChangeRequests.map((record) => record.number)
    );
    // Find records that need to be archived
    const archiveRecords = elasticDocs.filter((record) => {
      const isValid = validNumbersSet.has(record.number); // Check if number exists in validChangeRequests
      const isArchived = record.archived === true; // Check if already archived
      return !isValid || isArchived; // Archive if not in validChangeRequests OR already archived
    });
    console.log(`Records to archive: ${archiveRecords.length}`);

    const changeNumbersToArchive = archiveRecords.map((record) => record.number);

    const chunkSize = 2000;
    for (let start = 0; start < changeNumbersToArchive.length; start += chunkSize) {
      const chunk = changeNumbersToArchive.slice(start, start + chunkSize);

      try {
        const response = await fetch(
          `${process.env.DATASYNC_SERVER_URL}/changeRequests/byNumbersWithFields`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "x-api-key": `${process.env.DATASYNC_SERVER_API_KEY}`,
            },
            body: JSON.stringify({
              numbers: chunk,
              fields: ["number", "outage_duration", "actual_start", "actual_end", "close_category", "close_notes"],
            }),
          }
        );

        if (!response.ok) {
          const errorData = await response.json();
          console.error(errorData.error || `Request failed with status ${response.status}`);
          return;
        }

        const crData = await response.json();

        // Fetch incidents for this chunk only
        let incidents = [];
        try {
          const encoded = encodeURIComponent(chunk.join(","));
          const fetchRelatedIncidentsResponse = await fetch(
            `${process.env.DATASYNC_SERVER_URL}/incidents/byTime?page=1&pageSize=1000&caused_by_change_number=${encoded}`,
            {
              method: "GET",
              headers: {
                "Content-Type": "application/json",
                "x-api-key": `${process.env.DATASYNC_SERVER_API_KEY}`,
              },
            }
          );

          if (!fetchRelatedIncidentsResponse.ok) {
            const errorData = await fetchRelatedIncidentsResponse.json();
            console.error(errorData.error || `Request failed with status ${fetchRelatedIncidentsResponse.status}`);
            return;
          }

          incidents = await fetchRelatedIncidentsResponse.json();
        } catch (err) {
          console.log("Err from grouped incidents", err?.message);
          incidents = [];
        }

        // Build grouped incidents only for this chunk
        const groupedChunkIncidents = {};
        for (const incident of (incidents || [])) {
          const { number, outage_start, outage_end, disruption_time, caused_by_change_number, status } = incident;
          if (typeof status === "string" && status === "confirmed_cie") {
            const outageStartDate = new Date(outage_start);
            const outageEndDate = new Date(outage_end);
            const outageDurationMs = outageEndDate - outageStartDate;
            const outageDurationHours = outageDurationMs / (1000 * 60 * 60);
            const formattedIncident = {
              number,
              outage_start,
              outage_end,
              disruption_time,
              outageDurationMs,
              outageDurationHours: parseFloat(outageDurationHours.toFixed(2)),
            };
            if (!groupedChunkIncidents[caused_by_change_number]) {
              groupedChunkIncidents[caused_by_change_number] = [];
            }
            groupedChunkIncidents[caused_by_change_number].push(formattedIncident);
          }
        }

        // Merge chunk incidents into global grouping for reference (optional)
        for (const [cbc, list] of Object.entries(groupedChunkIncidents)) {
          if (!groupedByCbcNumber[cbc]) groupedByCbcNumber[cbc] = [];
          groupedByCbcNumber[cbc].push(...list);
        }

        const updatedRecords = crData.map((record) => {
          const changeNumber = record.number;
          const incs = groupedChunkIncidents[changeNumber] || [];
          let outageDuration = 0;
          const nonZeroIncident = incs.find((inc) => inc.outageDurationHours > 0);
          if (nonZeroIncident) {
            outageDuration = nonZeroIncident.outageDurationHours;
          } else if (incs.length > 0) {
            outageDuration = incs[0].outageDurationHours;
          }
          const incidentNumbers = incs.map((inc) => inc.number);
          return {
            ...record,
            outage_duration: outageDuration,
            incident_numbers: incidentNumbers,
          };
        });

        const operations = updatedRecords.flatMap((record) => {
          const doc = {
            actual_start: record?.actual_start,
            actual_end: record?.actual_end,
            outage_duration: record.outage_duration,
            archived: true,
            archived_at: new Date().toISOString(),
            close_category: record?.close_category,
            close_notes: record?.close_notes,
          };
          doc.state = 'Closed';
          if (record.incident_numbers && record.incident_numbers.length > 0) {
            doc.incident_numbers = record.incident_numbers;
          }
          return [{ update: { _index: indexName, _id: record.number } }, { doc }];
        });

        if (operations.length > 0) {
          console.log(`Archiving ${operations.length / 2} documents in bulk (chunk ${Math.floor(start / chunkSize) + 1})...`);
          const bulkResponse = await client.bulk({ body: operations });
          if (bulkResponse.errors) {
            const failedItems = bulkResponse.items
              .filter((item) => item.update && item.update.error)
              .map((item) => item.update);
            console.error("Some documents failed to archive:", failedItems);
          } else {
            console.log(`Successfully archived ${operations.length / 2} documents in this chunk`);
          }
        }
      } catch (error) {
        console.error("Error processing archive chunk:", error.message);
      }
    }

    return { totalToArchive: archiveRecords.length };
  } catch (error) {
    console.error("Error finding change requests to archive:", error?.message);
    console.error("Error details:", {
      message: error.message,
      stack: error.stack,
    });
    throw {
      message: error.message,
      stack: error.stack,
    };
  }
}

// async function getChangeRequestsToArchive(validChangeRequests, index) {
//   console.log(
//     `Starting archive check process with ${validChangeRequests.length} closed change requests`
//   );
//   const indexName = process.env.ELASTIC_INDEX || "change-risk-analysis";

//   try {
//     console.log(`Searching for non-archived documents in index: ${indexName}`);

//     const elasticDocs = await fetchElasticDocumentToClose(index);
//     console.log(`Found ${elasticDocs.length} records in Elasticsearch`);

//     // Create a Set for fast lookups of valid numbers
//     const validNumbersSet = new Set(
//       validChangeRequests.map((record) => record.number)
//     );

//     // Find records that need to be archived
//     const archiveRecords = elasticDocs.filter((record) => {
//       const isValid = validNumbersSet.has(record.number); // Check if number exists in validChangeRequests
//       const isArchived = record.archived === true; // Check if already archived

//       return !isValid || isArchived; // Archive if not in validChangeRequests OR already archived
//     });

//     console.log(
//       `Records to archive:`,
//       archiveRecords.map((record) => record.number)
//     );

//     // Bulk update the documents to set archived: true
//     const operations = archiveRecords.flatMap((record) => [
//       { update: { _index: indexName, _id: record.number } },
//       { doc: { archived: true, archived_at: new Date().toISOString() } },
//     ]);

//     if (operations.length > 0) {
//       console.log(`Archiving ${archiveRecords.length} documents in bulk...`);
//       const bulkResponse = await client.bulk({ body: operations });

//       if (bulkResponse.errors) {
//         const failedItems = bulkResponse.items
//           .filter((item) => item.update.error)
//           .map((item) => item.update);
//         console.error("Some documents failed to archive:", failedItems);
//       } else {
//         console.log(`Successfully archived ${archiveRecords.length} documents`);
//       }
//     }

//     return archiveRecords;
//   } catch (error) {
//     console.error("Error finding change requests to archive:", error?.message);
//     console.error("Error details:", {
//       message: error.message,
//       stack: error.stack,
//     });
//     throw {
//       message: error.message,
//       stack: error.stack,
//     };
//   }
// }

// async function archiveChangeRequests(changeRequestIds) {
//   const indexName = process.env.ELASTIC_INDEX || "change-risk-analysis";
//   const results = [];

//   try {
//     for (const crNumber of changeRequestIds) {
//       const [crStateError, crState] = await fetchChangeRequestState(crNumber);
//       console.log(
//         `Second check for the change request number ${crNumber} state`,
//         crState
//       );
//       if (crState === "Closed") {
//         try {
//           const updateResult = await client.update({
//             index: indexName,
//             id: crNumber,
//             body: {
//               doc: {
//                 archived: true,
//                 archived_at: new Date().toISOString(),
//               },
//             },
//           });

//           results.push({
//             id: crNumber,
//             success: true,
//             result: updateResult.result,
//           });

//           console.log(`Successfully archived change request ${crNumber}`);
//         } catch (error) {
//           console.error(
//             `Error archiving change request ${crNumber}:`,
//             error?.message
//           );
//           results.push({
//             id: crNumber,
//             success: false,
//             error: error.message,
//           });
//         }
//       } else {
//         console.log(
//           `Change request ${crNumber} is not in a closed state. Skipping archival.`
//         );
//       }
//     }

//     return {
//       total: changeRequestIds.length,
//       successful: results.filter((r) => r.success).length,
//       failed: results.filter((r) => !r.success).length,
//       results,
//     };
//   } catch (error) {
//     console.error("Error in archival process:", error?.message);
//     throw {
//       message: error.message,
//       stack: error.stack,
//     };
//   }
// }

function chunkChangesArray(array, chunkSize) {
  const chunks = [];
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize));
  }
  return chunks;
}

async function getOutagesBasedOnIncidents() {
  const indexName = process.env.ELASTIC_INDEX || "change-risk-analysis";

  try {
    console.log(`Searching for non-archived documents in index: ${indexName}`);
    const elasticDocs = await fetchElasticClosedDocument(indexName);
    console.log(`Found ${elasticDocs.length} records in Elasticsearch`);

    const groupedByService = {};

    const changeNumberChunks = chunkChangesArray(elasticDocs, 500); // Adjust chunk size as needed

    for (const chunk of changeNumberChunks) {
      const changeNumbers = chunk.map((item) => item.number);
      const encodedChangeNumbers = encodeURIComponent(changeNumbers.join(","));
      const fetchRelatedIncidentsResponse = await fetch(
        `${process.env.DATASYNC_SERVER_URL}/incidents/byTime?page=1&pageSize=1000&caused_by_change_number=${encodedChangeNumbers}`,
        {
          method: "GET",
          headers: {
            "Content-Type": "application/json",
            "x-api-key": `${process.env.DATASYNC_SERVER_API_KEY}`,
          },
        }
      );

      if (!fetchRelatedIncidentsResponse.ok) {
        const errorData = await fetchRelatedIncidentsResponse.json();
        console.error(
          errorData.error ||
          `Request failed with status ${fetchRelatedIncidentsResponse.status}`
        );
        return;
      }

      const data = await fetchRelatedIncidentsResponse.json();
      const incidents = data || [];

      for (const incident of incidents) {
        // console.log("incident", incident);
        const {
          number,
          outage_start,
          outage_end,
          disruption_time,
          caused_by_change_number,
          service_names,
        } = incident;

        const outageStartDate = new Date(outage_start);
        const outageEndDate = new Date(outage_end);
        const outageDurationMs = outageEndDate - outageStartDate;
        const outageDurationHours = outageDurationMs / (1000 * 60 * 60);

        const formattedIncident = {
          number,
          outage_start,
          outage_end,
          disruption_time,
          outageDurationMs,
          outageDurationHours: parseFloat(outageDurationHours.toFixed(2)),
          service_names: JSON.parse(service_names),
        };

        // Group by service_names[0] with numeric change number as key
        const primaryService = formattedIncident.service_names[0];

        // Use actual_end from the original change doc
        const matchedChange = elasticDocs.find(
          (doc) => doc.number === caused_by_change_number
        );

        const actualEnd =
          matchedChange?.actual_end != null
            ? matchedChange.actual_end
            : matchedChange?.planned_end != null
              ? matchedChange.planned_end
              : undefined;

        const changeNumberNumeric = caused_by_change_number.replace(
          /[^\d]/g,
          ""
        );

        if (!groupedByService[primaryService]) {
          groupedByService[primaryService] = {
            outage_duration: 0,
            outage_details: {},
          };
        }

        if (
          formattedIncident.outageDurationHours >
          groupedByService[primaryService].outage_duration
        ) {
          groupedByService[primaryService].outage_duration =
            formattedIncident.outageDurationHours;
          groupedByService[primaryService].time_stamp = actualEnd;
        }

        // Add outage_details per numeric change number
        if (
          !groupedByService[primaryService].outage_details[changeNumberNumeric]
        ) {
          groupedByService[primaryService].outage_details[changeNumberNumeric] =
          {
            incident_numbers: [],
            time_stamp: actualEnd,
          };
        }

        groupedByService[primaryService].outage_details[
          changeNumberNumeric
        ].incident_numbers.push(formattedIncident.number);
      }
    }

    await updateGroupedServiceDocument(groupedByService);
    return {
      groupedByService,
    };
  } catch (error) {
    console.error("Error finding outages:", error?.message);
    console.error("Error details:", {
      message: error.message,
      stack: error.stack,
    });
  }
}

function parseAnyDate(value) {
  const parts = value.split(/[\/\-]/).map((p) => parseInt(p, 10));

  if (parts.length === 3) {
    let year, month, day;

    if (value.includes("/")) {
      [month, day, year] = parts;
    } else {
      [year, month, day] = parts;
    }

    if (!isNaN(year) && !isNaN(month) && !isNaN(day)) {
      return new Date(Date.UTC(year, month - 1, day));
    }
  }

  return null;
}

function formatTimeOnly(dateString) {
  if (!dateString) return null;
  try {
    const date = new Date(dateString);
    return date.toTimeString().split(' ')[0]; // Returns HH:MM:SS format
  } catch (error) {
    return dateString; // Return original if parsing fails
  }
}

function formatDocumentTimes(document) {
  const timeFields = ['planned_start', 'planned_end', 'actual_start', 'actual_end', 'created', 'updated'];
  const formattedDoc = { ...document };
  
  timeFields.forEach(field => {
    if (formattedDoc[field]) {
      formattedDoc[field] = formatTimeOnly(formattedDoc[field]);
    }
  });
  
  return formattedDoc;
}

async function searchAnalyzedChanges(filters = {}) {
  const indexName = process.env.ELASTIC_INDEX || "change-risk-analysis";

  // Normalize filters if passed as JSON string
  if (typeof filters === "string") {
    try {
      filters = JSON.parse(filters);
    } catch {
      filters = {};
    }
  }

  const mustClauses = [];

  let plannedStartISO = null;
  let plannedEndISO = null;
  let timeRangeObj = null;

  for (const [key, value] of Object.entries(filters)) {
    if (value === undefined || value === null) continue;

    if (
      (key === "planned_start_date" || key === "planned_end_date") &&
      typeof value === "string" &&
      value.trim() !== ""
    ) {
      const date = parseAnyDate(value);
      if (date) {
        if (key === "planned_start_date") {
          plannedStartISO = dayjs(date).utc().startOf("day").toISOString();
        } else if (key === "planned_end_date") {
          plannedEndISO = dayjs(date).utc().endOf("day").toISOString();
        }
      }
      continue;
    }

    if (key === "timeRange" && typeof value === "object" && value.start && value.end) {
      timeRangeObj = value;
      continue;
    }

    if (key === "hours" && !isNaN(value) && Number(value) > 0) {
      mustClauses.push({
        range: {
          planned_start: {
            gte: "now",
            lte: `now+${value}h`,
          },
        },
      });
      continue;
    }

    if (key === "analysis_result.final_score" && typeof value === "number") {
      mustClauses.push({
        range: {
          "analysis_result.final_score": { gte: value },
        },
      });
      continue;
    }

    if (key === "changeNumbers" && Array.isArray(value) && value.length > 0) {
      mustClauses.push({ terms: { "number.keyword": value } });
      continue;
    }

    if (Array.isArray(value) && value.length > 0) {
      mustClauses.push({ terms: { [`${key}.keyword`]: value } });
      continue;
    }

    if (typeof value === "string" && value.trim() !== "") {
      mustClauses.push({ term: { [`${key}.keyword`]: value } });
      continue;
    }

    if (typeof value === "number" || typeof value === "boolean") {
      mustClauses.push({ term: { [key]: value } });
      continue;
    }

    if (typeof value === "object" && value.range) {
      mustClauses.push({ range: { [key]: value.range } });
      continue;
    }
  }

  if ((plannedStartISO || plannedEndISO) && timeRangeObj) {
    const startTime = dayjs(timeRangeObj.start);
    const endTime = dayjs(timeRangeObj.end);
    const startDate = plannedStartISO
      ? dayjs(plannedStartISO).startOf("day")
      : dayjs(plannedEndISO).startOf("day");
    const endDate = plannedEndISO
      ? dayjs(plannedEndISO).startOf("day")
      : dayjs(plannedStartISO).startOf("day");

    const diffDays = endDate.diff(startDate, "day");
    const dayRanges = [];

    for (let i = 0; i <= diffDays; i++) {
      const current = startDate.add(i, "day");

      const rangeStart = current
        .hour(startTime.hour())
        .minute(startTime.minute())
        .second(startTime.second())
        .millisecond(0)
        .toISOString();

      const rangeEnd = current
        .hour(endTime.hour())
        .minute(endTime.minute())
        .second(endTime.second())
        .millisecond(999)
        .toISOString();

      dayRanges.push({
        range: {
          planned_start: {
            gte: rangeStart,
            lte: rangeEnd,
          },
        },
      });
    }

    mustClauses.push({
      bool: { should: dayRanges, minimum_should_match: 1 },
    });

  } else if (timeRangeObj) {
    // Only timeRange when no planned date is given
    mustClauses.push({
      range: {
        [timeRangeObj?.date_field || "planned_start"]: {
          gte: timeRangeObj.start,
          lte: timeRangeObj.end,
        },
      },
    });
  } else if (plannedStartISO || plannedEndISO) {
    if (plannedStartISO) {
      mustClauses.push({
        range: {
          [timeRangeObj?.date_field || "planned_start"]: {
            gte: plannedStartISO,
          },
        },
      });
    }
    
    if (plannedEndISO) {
      mustClauses.push({
        range: {
          [timeRangeObj?.date_field || "planned_start"]: {
            lte: plannedEndISO,
          },
        },
      });
    }
  }

  const queryBody = {
    query: { bool: { must: mustClauses } },
    size: 10000,
  };

  console.log("queryBody search", JSON.stringify(queryBody, null, 2));

  try {
    const response = await client.search({
      index: indexName,
      body: queryBody,
    });

    const total = response.hits.total.value;
    const timeOnly = filters.timeOnly === true;

    const documents = response.hits.hits.map((doc) => {
      const document = { id: doc._id, ...doc._source };
      return timeOnly ? formatDocumentTimes(document) : document;
    });

    console.log("documents", total, documents.length);
    return {
      success: true,
      total,
      documents,
    };
  } catch (error) {
    console.error(
      "[searchAnalyzedChanges] error",
      error?.meta?.body || error?.message || error
    );
    throw { message: error.message, stack: error.stack };
  }
}

module.exports = {
  getAnalyzedDocuments,
  getAllDocuments,
  getChangeRequestsToArchive,
  getAllDocumentsIncludingClosed,
  getOutagesBasedOnIncidents,
  getAllClosedDocumentsFromElastic,
  searchAnalyzedChanges,
};
