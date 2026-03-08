const { analyzeCopilot } = require("./copilotService");
const { client } = require("../databaseSetup/elasticsearchConfig");
const { removeFromAnalysisQueue } = require("./documentAnalysisService");

async function analyzeChangeRequestRisk(change) {
  const indexName = process.env.ELASTIC_INDEX || "change-risk-analysis";
  // Validate input
  if (!change || !change.id) {
    throw new Error("Invalid change object: missing id field");
  }
  // Check if document already has valid analysis to prevent duplicate processing
  if (change.analysis_result && change.analysis?.risk_factors.length > 0) {
    console.log(`Document ${change.id} already has valid analysis, skipping...`);
    removeFromAnalysisQueue(change.id);
    return change.analysis_result;
  }
  
  // Remove analysis_result before sending for analysis
  const { analysis_result, ...documentWithoutAnalysis } = change;
  // Analyze the change request using Copilot
  const analysis = await analyzeCopilot(documentWithoutAnalysis);

  // Parse and validate the analysis result
  let analysisObject = null;
  try {
    analysisObject =
      typeof analysis === "object" && analysis !== null
        ? analysis
        : JSON.parse(analysis);

    if (typeof analysisObject !== "object" || analysisObject === null) {
      throw new Error("Analysis result must be an object");
    }
  } catch (parseError) {
    console.error("Error parsing analysis result:", parseError?.message);
    throw new Error("Error processing analysis result");
  }

  // Extract fields to be moved to root level
  const { region, datacenter, config_name, ...remainingAnalysis } =
    analysisObject;

  // Update the document with analysis results
  await client.update({
    index: indexName,
    id: change.id,
    body: {
      doc: {
        region,
        datacenter,
        config_name,
        analysis_result: remainingAnalysis,
        analyzed_at: new Date().toISOString(),
      },
    },
  });

  return analysisObject;
}

async function fetchAndReanalyzeIncompleteRiskAnalysis() {
  const indexName = process.env.ELASTIC_INDEX || "change-risk-analysis";

  try {
    console.log("Fetching documents with incomplete risk analysis...");

    const searchResponse = await client.search({
      index: indexName,
      size: 100,
      body: {
        query: {
          bool: {
            must: [
              {
                exists: {
                  field: "analysis_result"
                }
              }
            ],
            should: [
              {
                term: {
                  "analysis_result.risk_score": 0
                }
              },
              {
                bool: {
                  must: [
                    {
                      exists: {
                        field: "analysis_result.risk_score"
                      }
                    }
                  ],
                  must_not: [
                    {
                      exists: {
                        field: "analysis_result.risk_factors"
                      }
                    }
                  ]
                }
              }
            ],
            minimum_should_match: 1,
            must_not: [
              {
                term: {
                  archived: true
                }
              },
              {
                term: {
                  state: "Closed"
                }
              }
            ]
          }
        }
      }
    });
    
    

    const documents = searchResponse.hits.hits;
    console.log(`Found ${documents.length} documents with incomplete risk analysis`);


    console.log(`After filtering, found ${documents.length} documents that need re-analysis`);

    if (documents.length === 0) {
      console.log("No documents found with incomplete risk analysis");
      return { processed: 0, success: 0, failed: 0 };
    }

    let successCount = 0;
    let failedCount = 0;

    for (const doc of documents) {
      try {
        console.log(`Re-analyzing document ${doc._id}...`);
        
        let fullDoc;
        try {
          fullDoc = await client.get({
            index: indexName,
            id: doc._id
          });
        } catch (getError) {
          console.error(`Failed to get document ${doc._id} from Elasticsearch:`, getError.message);
          throw new Error(`Document ${doc._id} not found in Elasticsearch`);
        }

        try {
          await client.update({
            index: indexName,
            id: doc._id,
            body: {
              script: {
                source: `
                  if (ctx._source.analysis_result != null) {
                    ctx._source.analysis_result.remove("risk_score");
                    ctx._source.analysis_result.remove("error");
                    ctx._source.analysis_result.remove("recommendations");
                    if (ctx._source.analysis_result.risk_factors != null && ctx._source.analysis_result.risk_factors.size() == 0) {
                      ctx._source.analysis_result.remove("risk_factors");
                    }
                  }
                `,
                lang: "painless"
              }
            }
          });
    
          console.log(`Cleaned analysis_result fields for document ${doc._id}`);
        } catch (updateError) {
          console.error(`Failed to update document ${doc._id} in Elasticsearch:`, updateError.message);
          throw new Error(`Failed to update document ${doc._id}`);
        }

        console.log("Document ID:", doc._id);

        if (!fullDoc._source || typeof fullDoc._source !== 'object') {
          throw new Error(`Invalid document source for ${doc._id}: ${typeof fullDoc._source}`);
        }

        console.log(`Cleaning up invalid analysis_result for document ${doc._id}:`, JSON.stringify(fullDoc._source.analysis_result, null, 2))
        await analyzeChangeRequestRisk({
          id: doc._id,
          ...fullDoc._source
        });

        console.log(`Successfully re-analyzed document ${doc._id}`);
        removeFromAnalysisQueue(doc._id)
        successCount++;
      } catch (error) {
        console.error(`Failed to re-analyze document ${doc._id}:`, error.message);
        failedCount++;
      }
    }

    console.log(`Risk analysis re-processing completed. Success: ${successCount}, Failed: ${failedCount}`);
    return {
      processed: documents.length,
      success: successCount,
      failed: failedCount
    };

  } catch (error) {
    console.error("Error fetching documents with incomplete risk analysis:", error.message);
    throw error;
  }
}

module.exports = { 
  analyzeChangeRequestRisk,
  fetchAndReanalyzeIncompleteRiskAnalysis
};
