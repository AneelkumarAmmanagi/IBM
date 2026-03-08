const cron = require("node-cron");
const { fetchChangeRequests } = require("./changeRequestService");
const {
  checkDocumentsForAnalysis,
  processQueuedDocuments,
} = require("./documentAnalysisService");
const { sendMessageToSlack } = require("./slack/slack.post");
const { sendOutageMessageToSlack } = require("./slack/slack.post.outages");
const { fetchCurrentMonthOutages } = require("./error-budget/outage.aggregate");
const createMonthlyServiceDoc = require("./error-budget/monthly.outage.doc.creation");
const { getOutagesBasedOnIncidents } = require("./analyzedDocumentService");
const { fetchAndReanalyzeIncompleteRiskAnalysis } = require("./riskAnalysisService");
const { getAllChangeNumbersAndSummariesFromWorkspaceIndex } = require("./workspace-services/workspace.summary");
const { sendVpDailySummaryToSlack } = require("./slack/slack.vp.summary");

function startCronJob() {
  // Schedule the job to run every 5 minutes
  cron.schedule("*/5 * * * *", async () => {
    console.log("Running scheduled change request processing...");
    fetchChangeRequests();
    checkDocumentsForAnalysis();
    processQueuedDocuments();
  });
  console.log("Document fetching from datalake cron job scheduled to run every 5 minutes");
}

const slackAlertCronJob = () => {
  // Schedule the job to run every 5 minutes
  cron.schedule("30 2 * * *", async () => {
    console.log("Running scheduled change request processing...");
    sendMessageToSlack();
  });
  console.log("Sending risky CR to slack cron job scheduled to run every 8am in the morning");
};

const outageSlackAlertCronJob = () => {
  // Schedule the job to run every 5 minutes
  cron.schedule("30 11,23 * * *", async () => {
    console.log("Running scheduled outage fetch request processing...");
    try {
      const data = await fetchCurrentMonthOutages();
      sendOutageMessageToSlack(data);
      console.log("Returning outages for the config items");
      return { outages: data };
    } catch (error) {
      console.error(
        "Error fetching the outage duration for config items:",
        error.meta?.body || error.message || error
      );
      throw new Error("Error fetching the outage duration for config items:.");
    }
  });
  console.log("Outages data sent to slack cron job scheduled to run every 5 minute");
};

const monthlyOutageDocCreationCronJob = () => {
  cron.schedule("0 0 1 * *", async () => {
    console.log("Running scheduled monthly outage doc creation...");
    createMonthlyServiceDoc();
  });
  console.log("Monthly outage document creation cron job scheduled to run every month on day 1.");
};

const incidentDetectionCronJob = () => {
  cron.schedule("30 11,23 * * *", async () => {
    console.log("Running scheduled incident detection...");
    getOutagesBasedOnIncidents();
  });
  console.log("Incident detection for outages cron job scheduled to run every day at 6AM and 6PM.");
};

const riskAnalysisReProcessingCronJob = () => {
  cron.schedule("*/10 * * * *", async () => {
    console.log("Running scheduled risk analysis re-processing...");
    try {
      const result = await fetchAndReanalyzeIncompleteRiskAnalysis();
      console.log(`Risk analysis re-processing completed: ${result.processed} processed, ${result.success} successful, ${result.failed} failed`);
    } catch (error) {
      console.error("Error in risk analysis re-processing cron job:", error.message);
    }
  });
  console.log("Risk analysis re-processing cron job scheduled to run every 10 minutes");
};

const addWorkspaceSummariesCronJob=()=>{
  cron.schedule("*/10 * * * *", async () => {
    console.log("Running scheduled workspace summary cron job...");
    getAllChangeNumbersAndSummariesFromWorkspaceIndex();
  });
  console.log("Workspace summary cron job scheduled to run every 10 minutes");
};

const vpSummaryCronJob = () => {
  cron.schedule(
    "0 0 * * *",
    async () => {
      console.log("Running scheduled VP daily summary cron job...");
      try {
        await sendVpDailySummaryToSlack();
        console.log("VP daily summary sent to Slack successfully.");
      } catch (error) {
        console.error("Error sending VP daily summary via cron:", error?.message || error);
      }
    },
    {
      timezone: "UTC",
    }
  );
  console.log(
    "VP daily summary cron job scheduled to run every day at 12:00 AM"
  );
};

module.exports = {
  startCronJob,
  slackAlertCronJob,
  outageSlackAlertCronJob,
  monthlyOutageDocCreationCronJob,
  incidentDetectionCronJob,
  riskAnalysisReProcessingCronJob,
  addWorkspaceSummariesCronJob,
  vpSummaryCronJob
};
