require("dotenv").config();
const express = require("express");
const cors = require("cors");
const { Client } = require("@modelcontextprotocol/sdk/client/index.js");
const {
  StreamableHTTPClientTransport,
} = require("@modelcontextprotocol/sdk/client/streamableHttp.js");
const { fetchChangeRequests, getLastChangeRequestsFetchAt } = require("./services/changeRequestService");
const {
  startCronJob,
  slackAlertCronJob,
  outageSlackAlertCronJob,
  monthlyOutageDocCreationCronJob,
  incidentDetectionCronJob,
  riskAnalysisReProcessingCronJob,
  addWorkspaceSummariesCronJob,
  vpSummaryCronJob,
} = require("./services/cronService");
const {
  checkElasticsearchConnection,
  checkAndCreateIndex,
} = require("./services/elasticsearchService");
const {
  checkDocumentsForAnalysis,
  processQueuedDocuments,
  processAllQueuedDocuments,
  getAnalysisQueue,
} = require("./services/documentAnalysisService");
const { getLoadBalancer } = require("./services/copilot-platform-apis/load.balancer");
const {
  getAnalyzedDocuments,
  getAllDocuments,
  getAllDocumentsIncludingClosed,
  getOutagesBasedOnIncidents,
  getAllClosedDocumentsFromElastic,
  searchAnalyzedChanges,
} = require("./services/analyzedDocumentService");
const { analyzeChangeRequestRisk, fetchAndReanalyzeIncompleteRiskAnalysis } = require("./services/riskAnalysisService");
const {
  fetchAnalysedResult,
  fetchMultipleAnalysedResults,
} = require("./services/fetchAnalysedResult");
const {
  authMiddleware,
  mcpAuthMiddleware,
} = require("./middleware/authMiddleware");
const { queryAnyElasticDBData } = require("./services/queryAnyData");
const {
  fetchCurrentMonthOutages,
} = require("./services/error-budget/outage.aggregate");
const { sendOutageMessageToSlack } = require("./services/slack/slack.post.outages");
const { sendVpDailySummaryToSlack } = require("./services/slack/slack.vp.summary");
const { fetchAndGroupCieIncidents } = require("./services/cie-check/cie.check");
const { getCbcChangesCount, enrichCbcChangesWithPipeline } = require("./services/cbc-count/cbc.count");
const { cbcIncidents } = require("./services/templates/cbc-incidents");
const { cbcChanges } = require("./services/templates/cbc-changes");
const { sendCbcChangesToApi } = require("./services/templates/cbc-changes-rag");
const { sendCbcIncidentsToApi } = require("./services/templates/cbc-incidents-rag");
const { fetchLastNHoursChanges } = require("./services/next-12-hrs/changes.12hrs");
const { fetchPastOrFutureChanges } = require("./services/vp-reports/vp.12hrs");
const { fetchAskIaasCiesReports } = require("./services/vp-reports/vp.askiaas.cies.reports");
const { fetchAskIaasTipsReports } = require("./services/vp-reports/vp.askiaas.tips.reports");
const { getAllChangeNumbersAndSummariesFromWorkspaceIndex } = require("./services/workspace-services/workspace.summary");
const { fetchAskIaasChanges } = require("./services/vp-reports/vp.askiaas.reports");
const { getDocsWithWorkspaceAndSummary } = require("./services/fetchWorkspaceSummaryDetails");
const { getCbcIncidents, getClosedCRsByActualEndTime, getImproperCRsByActualStartTime, getCRsByActualStartTime, getCRsNotClosedAsExpected } = require("./services/change_stat/change_service");
const { getRazeeDeployChanges } = require("./services/razee-deploy/razee.deploy.ask-iaas");

const app = express();
app.use(cors());
app.use(express.json());


const PORT = process.env.PORT || 8080;

app.get("/api/process-changes", authMiddleware, async (req, res) => {
  try {
    const changeRequests = await fetchChangeRequests();
    res.json({
      success: true,
      count: changeRequests.length,
      data: changeRequests,
      lastRunAt: getLastChangeRequestsFetchAt(),
    });
  } catch (error) {
    console.error("Error processing change requests:", error?.message);
    res.status(500).json({
      success: false,
      error: error?.message || "Internal server error",
    });
  }
});

app.get("/api/process-changes/status", authMiddleware, (req, res) => {
  try {
    res.json({
      success: true,
      lastRunAt: getLastChangeRequestsFetchAt(),
    });
  } catch (error) {
    console.error("Error fetching last run status:", error?.message);
    res.status(500).json({
      success: false,
      error: error?.message || "Internal server error",
    });
  }
});

app.get("/api/analyzed-documents", authMiddleware, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const size = parseInt(req.query.size) || 100;
    const result = await getAnalyzedDocuments(page, size);
    res.json({
      success: true,
      ...result,
    });
  } catch (error) {
    console.error("Error fetching analyzed documents:", error?.message);
    res.status(500).json({
      success: false,
      error: error.message || "Internal server error",
    });
  }
});

app.get("/api/all-documents", authMiddleware, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const size = parseInt(req.query.size) || 500;
    const { timeRangeStart, timeRangeEnd, timeRangeField } = req.query || {};
    const result = await getAllDocuments(
      page,
      size,
      timeRangeStart,
      timeRangeEnd,
      timeRangeField
    );
    res.json({
      success: true,
      ...result,
    });
  } catch (error) {
    console.error("Error fetching all documents:", error?.message);
    res.json({
      success: false,
      error: error?.message || "Internal server error",
    });
  }
});

app.post("/api/all-elastic-documents", authMiddleware, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const size = parseInt(req.query.size) || 500;
    const result = await getAllDocumentsIncludingClosed(
      page,
      size,
      req?.body?.timeRange
    );
    res.json({
      success: true,
      ...result,
    });
  } catch (error) {
    console.error("Error fetching all documents:", error?.message);
    res.json({
      success: false,
      error: error?.message || "Internal server error",
    });
  }
});

app.post("/api/analyzed-changes/search", authMiddleware, async (req, res) => {
  try {
    const filters = req.body || {};
    console.log("filters", filters);
    const start = Date.now();
    const result = await searchAnalyzedChanges(filters);
    const durationMs = Date.now() - start;
    console.log("Result", result.total);
    res.json(result);
  } catch (error) {
    console.error("Error searching analyzed changes:", error?.message || error);
    res.status(500).json({ success: false, error: error?.message || "Internal server error" });
  }
});

app.get("/api/closed-documents", authMiddleware, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const size = parseInt(req.query.size) || 100;
    const result = await getAllClosedDocumentsFromElastic(
      page,
      size,
      req?.body?.timeRange,
      "",
      req?.headers?.hours
    );
    res.json({
      success: true,
      ...result,
    });
  } catch (error) {
    console.error("Error fetching all documents:", error?.message);
    res.json({
      success: false,
      error: error?.message || "Internal server error",
    });
  }
});



app.get(`/api/:changeId/analysis-result`, authMiddleware, async (req, res) => {
  try {
    const changeId = req.params.changeId;
    const result = await fetchAnalysedResult(changeId);
    if (result?.resultResponse !== null) {
      res.json({ analysis_result: result?.resultResponse });
    } else {
      res.status(404).json({ error: result?.error });
    }
  } catch (error) {
    console.error("Error fetching analysis result:", error?.message);
    res.status(500).json({
      error: error?.message || "Internal server error",
    });
  }
});

app.get("/api/analysis-results", authMiddleware, async (req, res) => {
  try {
    const changeIds = req.body.changeIds;

    if (!Array.isArray(changeIds) || changeIds.length === 0) {
      return res
        .status(400)
        .json({ error: "changeIds must be a non-empty array" });
    }

    const results = await fetchMultipleAnalysedResults(changeIds);
    res.json({ results });
  } catch (error) {
    console.error("Error fetching multiple analysis results:", error?.message);
    res.status(500).json({
      error: error?.message || "Internal server error",
    });
  }
});

app.get("/api/:changeId/workspace-summary-details", authMiddleware, async (req, res) => {
  const changeId = req.params.changeId;
  try {
    const results = await getDocsWithWorkspaceAndSummary(changeId);
    res.json({ results });
  } catch (error) {
    console.error("Error fetching multiple analysis results:", error?.message);
    res.status(500).json({
      error: error?.message || "Internal server error",
    });
  }
});

app.post("/api/changes/analyze-risk", authMiddleware, async (req, res) => {
  try {
    const analysisObject = await analyzeChangeRequestRisk(req.body);
    res.json({
      success: true,
      changeRequest: req?.body?.id,
      analysis: analysisObject,
    });
  } catch (error) {
    console.error("Error analyzing change request:", error?.message);
    res.status(500).json({
      success: false,
      error: error.message || "Internal server error",
    });
  }
});

app.post("/api/query-any-data", mcpAuthMiddleware, async (req, res) => {
  try {
    const data = await queryAnyElasticDBData(req.body);
    console.log("Returning elastic data from query-any-data");
    return res.json(data);
  } catch (error) {
    console.error(
      "Error querying Elasticsearch:",
      error.meta?.body || error.message || error
    );
  }
});

app.get("/api/get-outages", async (req, res) => {
  try {
    const data = await fetchCurrentMonthOutages();
    console.log("Returning outages for the config items");
    return res.status("200").json({ outages: data });
  } catch (error) {
    console.error(
      "Error fetching the outage duration for config items:",
      error.meta?.body || error.message || error
    );
    throw new Error("Error fetching the outage duration for config items:.");
  }
});

app.post("/api/cie-check", authMiddleware, async (req, res) => {
  try {
    const cieIncidents = await fetchAndGroupCieIncidents();
    res.json({ cieIncidents });
  } catch (error) {
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

app.post("/api/cbc-incidents", async (req, res) => {
  try {
    const cieIncidents = await cbcIncidents();
    res.json({ cieIncidents });
  } catch (error) {
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

app.post("/api/cbc-changes", async (req, res) => {
  try {
    const cieIncidents = await sendCbcChangesToApi();
    res.json({ cieIncidents });
  } catch (error) {
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});


app.post("/api/next-12hr-changes", authMiddleware, async (req, res) => {
  try {
    const { hours, closedChanges } = req.body || {};
    const changes = await fetchLastNHoursChanges(hours, closedChanges);
    res.json({ changes });
  } catch (error) {
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

app.post("/api/previous-next-12hr-changes", authMiddleware, async (req, res) => {
  try {
    const { hours, duration, platform, aiRiskScore, planned_start, planned_end } = req.body || {};
    const changes = await fetchPastOrFutureChanges(hours, duration, platform, aiRiskScore, planned_start, planned_end);
    res.json({ changes });
  } catch (error) {
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

app.post("/api/ask-iaas/vp-reports", authMiddleware, async (req, res) => {
  try {
    const { platform, email, day } = req.body || {};
    console.log("request vps body", req.body);

    if (!email || !day) {
      return res.status(400).json({
        success: false,
        error: "Both 'email' and 'day' parameters are required",
        message: "Please provide both email and day (today/yesterday) in the request body",
      });
    }

    const result = await fetchAskIaasChanges({ platform, email, day });
    res.json(result);
  } catch (error) {
    console.error("Error fetching Ask IaaS VP changes:", error?.message || error);
    res.status(500).json({
      success: false,
      error: error?.message || "Internal server error",
    });
  }
});

app.post("/api/ask-iaas/razee-deploy-changes", authMiddleware, async (req, res) => {
  try {
    const {
      page,
      size,
      timeRangeStart,
      timeRangeEnd,
      timeRangeField,
      sortField,
      sortOrder,
      region,
      regions,
      serviceNames,
      service_names,
      timeRange
    } = req.body || {};

    const result = await getRazeeDeployChanges({
      page,
      size,
      timeRangeStart,
      timeRangeEnd,
      timeRangeField,
      sortField,
      sortOrder,
      region,
      regions,
      serviceNames: serviceNames || service_names,
      timeRange
    });

    res.json(result);
  } catch (error) {
    console.error("Error fetching IS Razee Deploy changes:", error?.message || error);
    res.status(500).json({
      success: false,
      error: error?.message || "Internal server error",
    });
  }
});

app.post("/api/ask-iaas/vp-cies", authMiddleware, async (req, res) => {
  try {
    const { platform, email, day } = req.body || {};
    console.log("request ask iaas cies body", req.body);

    if (!email || !day) {
      return res.status(400).json({
        success: false,
        error: "Both 'email' and 'day' parameters are required",
        message: "Please provide both email and day (today/yesterday) in the request body",
      });
    }

    const result = await fetchAskIaasCiesReports({ platform, email, day });
    res.json(result);
  } catch (error) {
    console.error("Error fetching Ask IaaS CIE reports:", error?.message || error);
    res.status(500).json({
      success: false,
      error: error?.message || "Internal server error",
    });
  }
});

app.post("/api/ask-iaas/vp-tips", authMiddleware, async (req, res) => {
  try {
    const { day } = req.body || {};
    console.log("request ask iaas tips body", req.body);

    const result = await fetchAskIaasTipsReports({ day });
    res.json(result);
  } catch (error) {
    console.error("Error fetching Ask IaaS TIP reports:", error?.message || error);
    res.status(500).json({
      success: false,
      error: error?.message || "Internal server error",
    });
  }
});

app.post("/api/slack/vp-summary", authMiddleware, async (req, res) => {
  try {
    const { aiRiskScore, highRiskThreshold, topChangesLimit, platforms, webhookUrl } = req.body || {};
    const result = await sendVpDailySummaryToSlack({
      aiRiskScore,
      highRiskThreshold,
      topChangesLimit,
      platforms,
      webhookUrl,
    });
    res.json({ success: true, ...result });
  } catch (error) {
    console.error("Error sending VP summary to Slack:", error?.message || error);
    res.status(500).json({
      success: false,
      error: error?.message || "Internal server error",
    });
  }
});

app.post("/api/reprocess-incomplete-risk-analysis", authMiddleware, async (req, res) => {
  try {
    const result = await fetchAndReanalyzeIncompleteRiskAnalysis();
    res.json({
      success: true,
      message: "Risk analysis re-processing completed",
      result
    });
  } catch (error) {
    console.error("Error in manual risk analysis re-processing:", error.message);
    res.status(500).json({
      success: false,
      error: error.message || "Internal server error"
    });
  }
});

app.post("/api/process-all-queued-documents", authMiddleware, async (req, res) => {
  try {
    console.log("Starting continuous processing of all queued documents via API...");
    
    const loadBalancer = getLoadBalancer();
    const urlCount = loadBalancer.getUrlCount();
    const throughput = urlCount * 100;
    
    const queueState = getAnalysisQueue();
    const totalDocs = queueState.queueSize + queueState.waitingSize;
    const estimatedTime = totalDocs > 0 ? Math.ceil(totalDocs / throughput) : 0;
    
    res.json({
      success: true,
      message: "Continuous processing started. This will process all queued documents in batches respecting rate limits.",
      note: "Processing is running in the background. Check logs for progress.",
      queueInfo: {
        queueSize: queueState.queueSize,
        waitingSize: queueState.waitingSize,
        totalDocuments: totalDocs
      },
      loadBalancing: {
        activeUrls: urlCount,
        throughput: `${throughput} requests/minute`,
        estimatedTime: totalDocs > 0 ? `~${estimatedTime} minutes` : "Queue is empty"
      }
    });

    processAllQueuedDocuments().then(result => {
      console.log("Continuous processing completed:", result);
      console.log("Load Balancer Stats:", JSON.stringify(loadBalancer.getStats(), null, 2));
    }).catch(error => {
      console.error("Error in continuous processing:", error.message);
    });
  } catch (error) {
    console.error("Error starting continuous processing:", error.message);
    res.status(500).json({
      success: false,
      error: error.message || "Internal server error"
    });
  }
});

app.get("/api/load-balancer/stats", authMiddleware, async (req, res) => {
  try {
    const loadBalancer = getLoadBalancer();
    const stats = loadBalancer.getStats();
    const throughputPerMin = loadBalancer.getUrlCount() * 100;
    
    // Helper function to estimate processing time
    const estimateTime = (docCount) => `~${Math.ceil(docCount / throughputPerMin)} min`;
    
    res.json({
      success: true,
      stats: stats,
      throughput: {
        requestsPerMinute: throughputPerMin,
        requestsPerHour: throughputPerMin * 60,
        estimatedProcessingTimes: {
          "100_docs": estimateTime(100),
          "500_docs": estimateTime(500),
          "1000_docs": estimateTime(1000),
          "2000_docs": estimateTime(2000),
          "5000_docs": estimateTime(5000)
        }
      }
    });
  } catch (error) {
    console.error("Error fetching load balancer stats:", error.message);
    res.status(500).json({
      success: false,
      error: error.message || "Internal server error"
    });
  }
});

app.post("/api/load-balancer/reset-stats", authMiddleware, async (req, res) => {
  try {
    const loadBalancer = getLoadBalancer();
    loadBalancer.resetStats();
    
    res.json({
      success: true,
      message: "Load balancer statistics have been reset"
    });
  } catch (error) {
    console.error("Error resetting load balancer stats:", error.message);
    res.status(500).json({
      success: false,
      error: error.message || "Internal server error"
    });
  }
});

app.get("/api/cbc-changes/count", authMiddleware, async (req, res) => {
  try {
    const result = await getCbcChangesCount();
    res.json(result);
  } catch (error) {
    console.error("Error fetching CBC changes count:", error?.message);
    res.status(500).json({
      success: false,
      error: error?.message || "Internal server error",
    });
  }
});

app.get("/health", (req, res) => {
  res.status(200).json({
    success: "Healthy",
  });
});

app.get("/api/cbc-incidents", async (req, res) => {
  console.log("In /api/cbc-incidents");
  const start = req.query.start;
  const end = req.query.end;
  console.log(`${start} - ${end}`);
  try {
    const cbcIncidents = await getCbcIncidents(start, end);
    console.log("cbcIncidents:", cbcIncidents?.length);
    res.json(cbcIncidents);
  } catch (error) {
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

app.get("/api/closed-crs-by-actual-end", async (req, res) => {
  console.log("In /api/closed-crs-by-actual-end");
  const start = req.query.start;
  const end = req.query.end;
  const onlyProduction = req.query.onlyProduction || false;
  console.log(`${start} - ${end}`);
  if (!start || !end) {
    console.log("Invalid parameters");
    res.status(400).json({ error: "Invalid parameters" });
    return;
  }

  try {
    const closedCRs = await getClosedCRsByActualEndTime(start, end, onlyProduction);
    console.log("closedCRs:", closedCRs?.documents?.length);
    res.json(closedCRs);
  } catch (error) {
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

app.get("/api/improper-crs-by-actual-start", async (req, res) => {
  console.log("In /api/improper-crs-by-actual-start");
  const start = req.query.start;
  const end = req.query.end;
  console.log(`${start} - ${end}`);
  try {
    const improperCRsByActualStartTime = await getImproperCRsByActualStartTime(start, end);
    console.log("improperCRsByActualStartTime:", improperCRsByActualStartTime?.documents?.length);
    res.json(improperCRsByActualStartTime);
  } catch (error) {
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

app.get("/api/crs-by-actual-start", async (req, res) => {
  console.log("In /api/crs-by-actual-start");
  const start = req.query.start;
  const end = req.query.end;
  console.log(`${start} - ${end}`);
  try {
    const crsByActualStartTime = await getCRsByActualStartTime(start, end);
    console.log("crsByActualStartTime:", crsByActualStartTime?.documents?.length);
    res.json(crsByActualStartTime);
  } catch (error) {
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

app.get("/api/crs-not-closed-as-expected", async (req, res) => {
  console.log("In /api/crs-not-closed-as-expected");
  const start = req.query.start;
  const end = req.query.end;
  console.log(`${start} - ${end}`);
  try {
    const crsNotClosedAsExpected = await getCRsNotClosedAsExpected(start, end);
    console.log("crsNotClosedAsExpected:", crsNotClosedAsExpected?.documents?.length);
    res.json(crsNotClosedAsExpected);
  } catch (error) {
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

app.listen(PORT, async () => {
  console.log(`Server is running on port ${PORT}`);

  // Check Elasticsearch connection and setup index
  const isConnected = await checkElasticsearchConnection();
  if (!isConnected) {
    console.error(
      "Failed to connect to Elasticsearch. Server will continue but data storage will be affected."
    );
  } else {
    const indexCreated = await checkAndCreateIndex();
    if (!indexCreated) {
      console.error(
        "Failed to setup Elasticsearch index. Server will continue but data storage will be affected."
      );
    }
  }

  // Process changes immediately when server starts
  console.log("Processing changes on server startup...");
  console.log("running the latest version");
  fetchChangeRequests();
  checkDocumentsForAnalysis();
  processQueuedDocuments();
  getOutagesBasedOnIncidents();
  // Start the cron job for subsequent processing
  startCronJob();
  slackAlertCronJob();
  outageSlackAlertCronJob();
  monthlyOutageDocCreationCronJob();
  incidentDetectionCronJob();
  riskAnalysisReProcessingCronJob();
  addWorkspaceSummariesCronJob();
  if (process.env.NODE_ENV === "production") {
    vpSummaryCronJob();
  }
});
