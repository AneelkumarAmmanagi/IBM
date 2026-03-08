const { client } = require("../databaseSetup/elasticsearchConfig");
const { analyzeCopilot } = require("./copilotService");
const { getLoadBalancer } = require("./copilot-platform-apis/load.balancer");

// In-memory queue to store documents that need analysis
const analysisQueue = [];
const waitingList = [];
const MAX_QUEUE_SIZE = 1000;
const MAX_RETRIES = 3;
const RATE_LIMIT_DELAY = 1000;
const BATCH_DELAY = 60000;
const BASE_CONCURRENT_LIMIT = 100;

function getConcurrentLimit() {
  const loadBalancer = getLoadBalancer();
  const urlCount = loadBalancer.getUrlCount();
  return BASE_CONCURRENT_LIMIT * urlCount;
}

const CONCURRENT_LIMIT = getConcurrentLimit();

const analysisCache = new Map();
const CACHE_DURATION = 30 * 60 * 1000;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function isRecentlyAnalyzed(documentId) {
  const cached = analysisCache.get(documentId);
  if (!cached) return false;
  
  const now = Date.now();
  if (now - cached.timestamp > CACHE_DURATION) {
    analysisCache.delete(documentId);
    return false;
  }
  
  return true;
}

function markAsRecentlyAnalyzed(documentId) {
  analysisCache.set(documentId, { timestamp: Date.now() });
}

// Function to clean up expired cache entries
function cleanupAnalysisCache() {
  const now = Date.now();
  let cleanedCount = 0;
  
  for (const [documentId, cached] of analysisCache.entries()) {
    if (now - cached.timestamp > CACHE_DURATION) {
      analysisCache.delete(documentId);
      cleanedCount++;
    }
  }
  
  if (cleanedCount > 0) {
    console.log(`Cleaned up ${cleanedCount} expired cache entries`);
  }
}

// Function to get the current queue state
function getAnalysisQueue() {
  return {
    queue: analysisQueue,
    waitingList: waitingList,
    queueSize: analysisQueue.length,
    waitingSize: waitingList.length,
  };
}

// Function to add documents to the queue
function addToAnalysisQueue(documents) {
  const availableSpace = MAX_QUEUE_SIZE - analysisQueue.length;

  // Filter out documents that are already in either queue
  const uniqueDocuments = documents.filter((doc) => {
    const isInAnalysisQueue = analysisQueue.some(
      (qDoc) => qDoc.number === doc.number
    );
    const isInWaitingList = waitingList.some(
      (wDoc) => wDoc.number === doc.number
    );
    return !isInAnalysisQueue && !isInWaitingList;
  });

  const documentsToAdd = uniqueDocuments.map((doc) => ({
    ...doc,
    retryCount: 0, // Initialize retry counter
  }));

  if (availableSpace > 0) {
    const toAdd = documentsToAdd.slice(0, availableSpace);
    const remaining = documentsToAdd.slice(availableSpace);

    analysisQueue.push(...toAdd);
    // Sort queue by planned_start ascending (earliest first). Missing values go last.
    analysisQueue.sort((a, b) => {
      const aTime = a.planned_start ? new Date(a.planned_start).getTime() : Infinity;
      const bTime = b.planned_start ? new Date(b.planned_start).getTime() : Infinity;
      return aTime - bTime;
    });
    if (remaining.length > 0) {
      waitingList.push(...remaining);
    }

    console.log(
      `Added ${toAdd.length} documents to analysis queue. Queue size: ${analysisQueue.length}. ` +
        `${remaining.length} documents added to waiting list. Waiting list size: ${waitingList.length}`
    );
  } else {
    waitingList.push(...documentsToAdd);
    console.log(
      `Queue is full. Added ${documentsToAdd.length} documents to waiting list. ` +
        `Waiting list size: ${waitingList.length}`
    );
  }
}

// Function to remove a document from the queue and move waiting documents if space available
function removeFromAnalysisQueue(documentId) {
  const index = analysisQueue.findIndex((doc) => doc.id === documentId);
  if (index !== -1) {
    analysisQueue.splice(index, 1);

    // Move documents from waiting list to queue if available
    if (waitingList.length > 0 && analysisQueue.length < MAX_QUEUE_SIZE) {
      const availableSpace = MAX_QUEUE_SIZE - analysisQueue.length;
      const documentsToMove = waitingList.splice(0, availableSpace);
      analysisQueue.push(...documentsToMove);
      analysisQueue.sort((a, b) => {
        const aTime = a.planned_start ? new Date(a.planned_start).getTime() : Infinity;
        const bTime = b.planned_start ? new Date(b.planned_start).getTime() : Infinity;
        return aTime - bTime;
      });

      console.log(
        `Removed document ${documentId} from analysis queue. Moved ${documentsToMove.length} documents from waiting list. ` +
          `Queue size: ${analysisQueue.length}. Waiting list size: ${waitingList.length}`
      );
    } else {
      console.log(
        `Removed document ${documentId} from analysis queue. Queue size: ${analysisQueue.length}`
      );
    }
  }
}

async function checkDocumentsForAnalysis() {
  const indexName = process.env.ELASTIC_INDEX || "change-risk-analysis";
  const documentsWithoutAnalysis = [];
  const documentsWithAnalysis = [];

  try {
    // Only fetch documents that need analysis (no analysis_result or incomplete analysis)
    const searchResponse = await client.search({
      index: indexName,
      size: 10000, // Increased size to fetch all documents in one go
      body: {
        sort: [{ created: { order: "desc" } }],
        query: {
          bool: {
            must_not: [
              {
                term: {
                  archived: true,
                },
              },
              {
                term: {
                  state: "Closed",
                },
              },
              {
                term: {
                  state: "New",
                },
              },
            ],
            should: [
              {
                bool: {
                  must_not: [
                    {
                      exists: {
                        field: "analysis_result.risk_factors"
                      }
                    },
                    {
                      exists: {
                        field: "analysis_result.risk_score"
                      }
                    }
                  ]
                }
              }
            ],
            minimum_should_match: 1
          },
        },
      },
    });

    // Process documents that need analysis
    searchResponse.hits.hits.forEach((doc) => {
      const hasAnalysis = doc._source.analysis_result;
      const hasValidAnalysis = hasAnalysis && 
                              hasAnalysis.risk_score && 
                              hasAnalysis.risk_factors && 
                              Array.isArray(hasAnalysis.risk_factors) &&
                              hasAnalysis.risk_factors.length > 0;
      
      if (!hasValidAnalysis) {
        documentsWithoutAnalysis.push({
          id: doc._id || doc._source.number,
          number: doc._source.number,
          state: doc._source.state,
          planned_start: doc._source.planned_start,
        });
      } else {
        documentsWithAnalysis.push({
          id: doc._id || doc._source.number,
          number: doc._source.number,
          state: doc._source.state,
        });
      }
    });

    // Only add documents that aren't already in the queue
    const newDocuments = documentsWithoutAnalysis.filter(doc => 
      !analysisQueue.some(queueDoc => queueDoc.number === doc.number) &&
      !waitingList.some(waitingDoc => waitingDoc.number === doc.number)
    );
    
    addToAnalysisQueue(newDocuments);

    console.log(
      `Found ${documentsWithoutAnalysis.length} documents without analysis`
    );
    console.log(
      `Found ${documentsWithAnalysis.length} documents with analysis`
    );
    console.log(`Current analysis queue size: ${analysisQueue.length}`);
    console.log(`Current waiting list size: ${waitingList.length}`);

    return {
      documentsWithoutAnalysis,
      documentsWithAnalysis,
      totalDocuments:
        documentsWithoutAnalysis.length + documentsWithAnalysis.length,
    };
  } catch (error) {
    console.error("Error checking documents for analysis:", error?.message);
    // throw {
    //   message: error.message,
    //   stack: error.stack,
    // };
  }
}

async function processSingleDocument(currentDocument, indexName) {
  if (!currentDocument?.number) {
    console.error(`Skipping invalid document`);
    return {
      success: false,
      documentId: currentDocument?.number || 'unknown',
      error: 'Invalid document structure',
      shouldRemoveFromQueue: true
    };
  }

  if (isRecentlyAnalyzed(currentDocument.number)) {
    console.log(`Document ${currentDocument.number} was recently analyzed, skipping...`);
    return {
      success: true,
      documentId: currentDocument.number,
      skipped: true,
      reason: "recently_analyzed",
      shouldRemoveFromQueue: true
    };
  }

  try {
    const document = await client.get({
      index: indexName,
      id: currentDocument.number,
    });

    // Check if document already has valid analysis before processing
    if (document._source.analysis_result && 
        document._source.analysis_result.risk_score && 
        document._source.analysis_result.risk_factors && 
        Array.isArray(document._source.analysis_result.risk_factors) &&
        document._source.analysis_result.risk_factors.length > 0) {
      console.log(`Document ${currentDocument.number} already has valid analysis, skipping...`);
      return {
        success: true,
        documentId: currentDocument.number,
        skipped: true,
        reason: "already_analyzed",
        shouldRemoveFromQueue: true
      };
    }

    // Perform analysis
    const analysis = await analyzeCopilot(document._source);
    let analysisObject = null;
    
    try {
      if (analysis) {
        // If analysis is already an object, use it directly; otherwise parse it
        analysisObject =
          typeof analysis === "object" && analysis !== null
            ? analysis
            : JSON.parse(analysis);

        // Additional validation to ensure we have a valid object
        if (typeof analysisObject !== "object" || analysisObject === null) {
          throw new Error("Analysis result must be an object");
        }
      } else {
        throw new Error("No analysis result received");
      }
    } catch (parseError) {
      console.error(
        `Error parsing analysis result for document ${currentDocument.number}: ${parseError?.message}`
      );
      throw {
        message: parseError?.message,
        stack: parseError?.stack,
      };
    }

    // Update the document with the validated analysis object
    await client.update({
      index: indexName,
      id: currentDocument.number,
      body: {
        doc: {
          analysis_result: analysisObject,
          analyzed_at: new Date().toISOString(),
        },
      },
    });

    console.log(
      `Successfully analyzed and updated document ${currentDocument.number}`
    );
    
    markAsRecentlyAnalyzed(currentDocument.number);

    return {
      success: true,
      documentId: currentDocument.number,
      shouldRemoveFromQueue: true
    };
  } catch (error) {
    console.error(
      `Error processing document ${currentDocument.number}:`,
      error?.message
    );

    // Increment retry counter and only remove if max retries reached
    currentDocument.retryCount = (currentDocument.retryCount || 0) + 1;

    const shouldRemove = currentDocument.retryCount >= MAX_RETRIES;

    if (shouldRemove) {
      console.log(
        `Document ${currentDocument.number} has reached maximum retries (${MAX_RETRIES}), will be removed from queue`
      );
    } else {
      console.log(
        `Document ${currentDocument.number} analysis failed, attempt ${currentDocument.retryCount}/${MAX_RETRIES}, keeping in queue`
      );
    }

    return {
      success: false,
      documentId: currentDocument.number,
      error: error?.message || 'Unknown error',
      retryCount: currentDocument.retryCount,
      shouldRemoveFromQueue: shouldRemove
    };
  }
}

async function processQueuedDocuments(waitAfterBatch = true) {
  if (analysisQueue.length === 0) {
    console.log("Analysis queue is empty. Nothing to process.");
    return {
      success: true,
      processedCount: 0,
      results: [],
      queueSize: 0,
      waitingSize: waitingList.length,
    };
  }

  const indexName = process.env.ELASTIC_INDEX || "change-risk-analysis";
  const batchSize = Math.min(CONCURRENT_LIMIT, analysisQueue.length);
  
  // Create a copy of documents to process (avoid modification during parallel processing)
  const batchDocuments = analysisQueue.slice(0, batchSize);

  try {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`Processing batch of ${batchSize} documents in parallel...`);
    console.log(`Remaining in queue: ${analysisQueue.length}`);
    console.log(`Waiting list: ${waitingList.length}`);
    console.log(`${'='.repeat(60)}\n`);
    
    const startTime = Date.now();

    // Process all documents in parallel using Promise.allSettled
    const results = await Promise.allSettled(
      batchDocuments.map(doc => processSingleDocument(doc, indexName))
    );

    // Extract results from Promise.allSettled format
    const processedDocuments = results.map((result, index) => {
      if (result.status === 'fulfilled') {
        return result.value;
      } else {
        // Handle rejected promises
        const doc = batchDocuments[index];
        console.error(`Promise rejected for document ${doc?.number}:`, result.reason);
        return {
          success: false,
          documentId: doc?.number || 'unknown',
          error: result.reason?.message || 'Promise rejected',
          shouldRemoveFromQueue: false
        };
      }
    });

    // Now handle queue removal AFTER all parallel processing is done (prevents race conditions)
    const documentsToRemove = [];
    processedDocuments.forEach((result, index) => {
      if (result.shouldRemoveFromQueue) {
        documentsToRemove.push(batchDocuments[index].number);
      }
    });

    // Remove processed documents from queue in one operation
    documentsToRemove.forEach(docNumber => {
      removeFromAnalysisQueue(docNumber);
    });

    const elapsedTime = ((Date.now() - startTime) / 1000).toFixed(2);
    const successCount = processedDocuments.filter(r => r.success).length;
    const failureCount = processedDocuments.filter(r => !r.success).length;
    const skippedCount = processedDocuments.filter(r => r.skipped).length;

    console.log(`\n${'='.repeat(60)}`);
    console.log(`Batch processing completed in ${elapsedTime}s`);
    console.log(`Success: ${successCount} | Failures: ${failureCount} | Skipped: ${skippedCount}`);
    console.log(`Removed from queue: ${documentsToRemove.length}`);
    console.log(`Remaining in queue: ${analysisQueue.length}`);
    console.log(`Waiting list: ${waitingList.length}`);
    console.log(`${'='.repeat(60)}\n`);

    // Add delay between batches if more documents are queued and waiting is enabled
    if (waitAfterBatch && (analysisQueue.length > 0 || waitingList.length > 0)) {
      console.log(`Rate limiting: Waiting ${BATCH_DELAY/1000}s before next batch to respect API limits...`);
      await sleep(BATCH_DELAY);
    }

    // Clean up expired cache entries
    cleanupAnalysisCache();

    return {
      success: true,
      processedCount: processedDocuments.length,
      successCount,
      failureCount,
      skippedCount,
      removedCount: documentsToRemove.length,
      results: processedDocuments,
      queueSize: analysisQueue.length,
      waitingSize: waitingList.length,
      processingTimeSeconds: parseFloat(elapsedTime),
    };
  } catch (error) {
    console.error("Error in batch processing:", error?.message);
    return {
      success: false,
      error: error?.message || 'Unknown error',
      queueSize: analysisQueue.length,
      waitingSize: waitingList.length,
    };
  }
}

async function processAllQueuedDocuments() {
  const loadBalancer = getLoadBalancer();
  const urlCount = loadBalancer.getUrlCount();
  const throughput = urlCount * BASE_CONCURRENT_LIMIT;
  const totalDocs = analysisQueue.length + waitingList.length;
  const estimatedTime = Math.ceil(totalDocs / throughput);

  console.log(`\n${'#'.repeat(70)}`);
  console.log(`Starting continuous processing of all queued documents...`);
  console.log(`Initial queue size: ${analysisQueue.length}`);
  console.log(`Initial waiting list size: ${waitingList.length}`);
  console.log(`Total documents to process: ${totalDocs}`);
  console.log(`\n--- Load Balancing Configuration ---`);
  console.log(`Active API URLs: ${urlCount}`);
  console.log(`Throughput: ${throughput} requests/minute`);
  console.log(`Batch size: ${throughput} documents per batch`);
  console.log(`Estimated time: ~${estimatedTime} minutes`);
  console.log(`${'#'.repeat(70)}\n`);

  const overallStartTime = Date.now();
  let batchNumber = 0;
  let totalProcessed = 0;
  let totalSuccess = 0;
  let totalFailures = 0;
  let totalSkipped = 0;

  while (analysisQueue.length > 0 || waitingList.length > 0) {
    batchNumber++;
    console.log(`\nStarting Batch #${batchNumber}...`);
    
    const batchResult = await processQueuedDocuments(true);
    
    if (batchResult.success) {
      totalProcessed += batchResult.processedCount || 0;
      totalSuccess += batchResult.successCount || 0;
      totalFailures += batchResult.failureCount || 0;
      totalSkipped += batchResult.skippedCount || 0;
    } else {
      console.error(`Batch #${batchNumber} failed:`, batchResult.error);
    }

    if (batchResult.processedCount === 0 && analysisQueue.length > 0) {
      console.error('Warning: Queue not decreasing. Breaking to prevent infinite loop.');
      break;
    }
  }

  const overallElapsedTime = ((Date.now() - overallStartTime) / 1000 / 60).toFixed(2);
  const loadBalancerStats = loadBalancer.getStats();

  console.log(`\n${'#'.repeat(70)}`);
  console.log(`CONTINUOUS PROCESSING COMPLETED`);
  console.log(`Total batches processed: ${batchNumber}`);
  console.log(`Total documents processed: ${totalProcessed}`);
  console.log(`Success: ${totalSuccess} | Failures: ${totalFailures} | Skipped: ${totalSkipped}`);
  console.log(`Total time: ${overallElapsedTime} minutes`);
  console.log(`Average throughput: ${(totalProcessed / parseFloat(overallElapsedTime)).toFixed(1)} docs/min`);
  console.log(`\n--- Load Balancer Statistics ---`);
  console.log(`URLs used: ${urlCount}`);
  console.log(`Total requests distributed: ${loadBalancerStats.totalRequests}`);
  loadBalancerStats.urls.forEach((url, i) => {
    console.log(`  URL ${i + 1}: ${url.success} success, ${url.failures} failures (${url.successRate})`);
  });
  console.log(`\nFinal queue size: ${analysisQueue.length}`);
  console.log(`Final waiting list size: ${waitingList.length}`);
  console.log(`${'#'.repeat(70)}\n`);

  return {
    success: true,
    batchesProcessed: batchNumber,
    totalProcessed,
    totalSuccess,
    totalFailures,
    totalSkipped,
    totalTimeMinutes: parseFloat(overallElapsedTime),
    averageThroughput: parseFloat((totalProcessed / parseFloat(overallElapsedTime)).toFixed(1)),
    finalQueueSize: analysisQueue.length,
    finalWaitingSize: waitingList.length,
    loadBalancing: {
      urlsUsed: urlCount,
      totalRequests: loadBalancerStats.totalRequests,
      distributionStats: loadBalancerStats.urls
    }
  };
}

module.exports = {
  checkDocumentsForAnalysis,
  getAnalysisQueue,
  addToAnalysisQueue,
  removeFromAnalysisQueue,
  processQueuedDocuments,
  processAllQueuedDocuments,
};
