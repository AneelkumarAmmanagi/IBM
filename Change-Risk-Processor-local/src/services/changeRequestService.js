const axios = require("axios");
const fs = require("fs");
const path = require("path");
const { saveToElasticsearch } = require("./elasticsearchService");
const { client } = require("../databaseSetup/elasticsearchConfig");
// const {
//   fetchChangeRequestComments,
// } = require("./changeRequestCommentsService");
const { getChangeRequestsToArchive } = require("./analyzedDocumentService");
const { configNames } = require("../../constants/constants");
const { readParquetFile } = require("./parquetFileReader");
const { getItem } = require("./readCosBucketData");
const { deleteParquetFiles } = require("./deleteParquetFile");
const { filterNewRecords } = require("./bulkDocsUpload");
const { checkAndUpdateClosedDocs } = require("../../scripts/check-closed-docs");
const { compareElasticAndDatalakedata } = require("./compareElasticPostgres");

let lastChangeRequestsFetchAt = null;
const LAST_RUN_FILE = path.join(__dirname, "../../outputs/lastRunAt.json");

function loadLastRunFromDisk() {
  try {
    if (fs.existsSync(LAST_RUN_FILE)) {
      const content = fs.readFileSync(LAST_RUN_FILE, "utf8");
      const parsed = JSON.parse(content || "{}");
      if (parsed?.lastRunAt) {
        lastChangeRequestsFetchAt = parsed.lastRunAt;
      }
    }
  } catch (e) {
    console.error("Failed to read lastRunAt from disk:", e?.message || e);
  }
}

function persistLastRunToDisk(isoString) {
  try {
    const dir = path.dirname(LAST_RUN_FILE);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    fs.writeFileSync(LAST_RUN_FILE, JSON.stringify({ lastRunAt: isoString }, null, 2));
  } catch (e) {
    console.error("Failed to write lastRunAt to disk:", e?.message || e);
  }
}

loadLastRunFromDisk();

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

async function fetchChangeRequestsPage(url, params = {}) {
  try {
    console.log(`Fetching change requests from: ${url}`);
    const response = await axios.get(url, {
      ...axiosConfig,
      params,
    });

    console.log(
      `Received response with ${
        response.data?.change_requests?.length || 0
      } change requests`
    );
    let allRequests = response.data?.change_requests || [];

    // Check if there's a next page
    if (response.data?.next?.href) {
      console.log(`Found next page link: ${response.data.next.href}`);
      const nextPageRequests = await fetchChangeRequestsPage(
        response.data.next.href
      );
      console.log(
        `Retrieved ${nextPageRequests.length} requests from next page`
      );
      allRequests = [...allRequests, ...nextPageRequests];
      console.log(`Total requests after merging: ${allRequests.length}`);
    } else {
      console.log("No more pages to fetch");
    }

    return allRequests;
  } catch (error) {
    console.error("Error fetching change requests page:", {
      errorMessage: error?.message,
      errorName: error.name,
    });
    throw {
      message: error.message,
      stack: error.stack,
    };
  }
}

async function fetchChangeRequests() {
  try {
    // Delete all parquet files before fetching
    console.log("Deleting all parquet files before fetching...");
    await deleteParquetFiles();
    console.log("Downloading the data from the cos bucket...");
    await getItem(
      "obs-snow-change-requests-feed-raw",
      "obs-snow-changes-60dayssnapshot-latest.json"
    );
    console.log("Fetching the data from the duck db...");
    const duckdbResponses = await readParquetFile();
    if(process.env.NODE_ENV!=="development" ){
      compareElasticAndDatalakedata()
    }
    const allChangeRequests = duckdbResponses?.flat();
    
    // Check and update closed documents from parquet data
    const index = process.env.ELASTIC_INDEX || "change-risk-analysis";
    await checkAndUpdateClosedDocs(allChangeRequests, index);
    
    const newRecordsToProcess = await filterNewRecords(
      allChangeRequests,
      index
    );

    console.log(
      `Retrieved ${allChangeRequests.length} total change requests across all states`
    );

    console.log(
      `Processing ${newRecordsToProcess.length} change requests for Elasticsearch storage...`
    );
    const runAt = new Date().toISOString();
    lastChangeRequestsFetchAt = runAt;
    persistLastRunToDisk(runAt);
    console.log(`Change requests fetch run at: ${runAt}`);
    // Check for change requests that need to be archived
    console.log("Checking for change requests that need to be archived...");
    await getChangeRequestsToArchive(
      allChangeRequests,
      process.env.ELASTIC_INDEX || "change-risk-analysis"
    );
    // console.log(`Identified ${archivedRequests.length} requests for archival`);

    // console.log("Beginning individual change request processing...");
    // const results = [];
    // let processedCount = 0,
    // crnData = "";
    // for (const changeRequest of allChangeRequests) {
    //   processedCount++;
    //   if (changeRequest.state != "Closed") {
    //     console.log(
    //       `Processing change request ${changeRequest.number} (${processedCount}/${allChangeRequests.length})`
    //     );
    //     crnData = changeRequest.crn_mask.split(":")[4];
    //     if (configNames.includes(crnData)) {
    //       try {
    //         // Check if the change request already exists
    //         console.log(
    //           `Checking if change request ${changeRequest.number} exists...`
    //         );
    //         const exists = await client.exists({
    //           index: process.env.ELASTIC_INDEX || "cra-testing-duckdb-ruchika",
    //           id: changeRequest.number,
    //         });
    //         if (exists) {
    //           console.log(
    //             `Change request ${changeRequest.number} already exists, skipping...`
    //           );
    //           results.push({
    //             number: changeRequest.number,
    //             success: true,
    //             skipped: true,
    //           });
    //         } else {
    //           const enrichedChangeRequest = {
    //             ...changeRequest,
    //           };

    //           console.log(
    //             `Saving change request ${changeRequest.number} to Elasticsearch...`
    //           );
    //           const result = await saveToElasticsearch(enrichedChangeRequest);
    //           console.log(
    //             `Successfully stored change request ${changeRequest.number} in Elasticsearch with ID: ${result._id}`
    //           );
    //           results.push({
    //             number: changeRequest.number,
    //             success: true,
    //             id: result._id,
    //           });
    //         }
    //       } catch (error) {
    //         console.error(
    //           `Error processing change request ${changeRequest.number}:`,
    //           error?.message
    //         );
    //         console.error("Error details:", {
    //           changeRequestNumber: changeRequest.number,
    //           errorName: error.name,
    //           errorStack: error.stack,
    //         });
    //         results.push({
    //           number: changeRequest.number,
    //           success: false,
    //           error: error.message,
    //         });
    //       }
    //     } else {
    //       console.log(
    //         `Skipping change request ${changeRequest.number} as it belongs to paas`
    //       );
    //     }
    //   }
    // }

    // const successCount = results.filter((r) => r.success).length;
    // const skippedCount = results.filter((r) => r.skipped).length;
    // const failedCount = results.filter((r) => !r.success).length;

    console.log("Change request processing summary:");
    console.log(`- Total processed: ${newRecordsToProcess.length}`);
    // console.log(`- Successfully processed: ${successCount}`);
    // console.log(`- Skipped (already exists): ${skippedCount}`);
    // console.log(`- Failed: ${failedCount}`);

    return allChangeRequests;
  } catch (error) {
    console.error("Fatal error in fetchChangeRequests:", {
      errorMessage: error?.message,
      errorName: error.name,
      errorStack: error.stack,
    });
  }
}

function getLastChangeRequestsFetchAt() {
  return lastChangeRequestsFetchAt;
}

module.exports = { fetchChangeRequests, axiosConfig, getLastChangeRequestsFetchAt };
