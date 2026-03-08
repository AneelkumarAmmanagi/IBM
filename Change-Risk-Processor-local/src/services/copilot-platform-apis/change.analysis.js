const { getLoadBalancer } = require('./load.balancer');

const changeAnalysis = async (changeRequest, peakHourAnalysis) => {
  const loadBalancer = getLoadBalancer();
  let endpoint = null;
  
  try {
    const filteredChangeRequest = createFiltered(changeRequest, llmInputFieldList);
    filteredChangeRequest.comments = cleanedComments(filteredChangeRequest);
    endpoint = loadBalancer.getNextEndpoint();
    
    const analyseChangerequest = await fetch(
      endpoint.url,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          apiKey: endpoint.apiKey,
        },
        body: JSON.stringify({
          change_request: filteredChangeRequest,
          peak_hour: peakHourAnalysis,
        }),
      }
    );

    if (!analyseChangerequest.ok) {
      const errorData = await analyseChangerequest.json();
      loadBalancer.recordFailure(endpoint.index);
      throw new Error(
        JSON.stringify(errorData?.error ?? {}) ||
          `Request failed with status ${analyseChangerequest.status}`
      );
    }

    const data = await analyseChangerequest.json();
    loadBalancer.recordSuccess(endpoint.index);

    console.log(
      `Data returned from change analysis copilot platform for ${
        changeRequest.number
      } (URL ${endpoint.index + 1}): ${JSON.stringify(data, null, 2)}`
    );
    return [undefined, data];
  } catch (err) {
    if (endpoint) {
      loadBalancer.recordFailure(endpoint.index);
    }
    console.log(
      `Err returned from change analysis copilot platform (URL ${endpoint ? endpoint.index + 1 : 'N/A'}):`,
      err?.message
    );
    return [err, undefined];
  }
};

const llmInputFieldList = [
  "backout_plan",
  "close_notes",
  "comments",
  "contact_type",
  "customer_impact",
  "deployment_history",
  "deployment_impact",
  "deployment_method",
  "deployment_risk",
  "deployment_ready",
  "pipeline_name",
  "pipeline_version",
  "prepostchecks",
  "purpose",
  "description",
  "impact",
  "outage_duration",
  "short_description",
  "planned_start",
  "planned_end",
  "planned_duration",
  "locations",
  "service_names",
  "regions",
  "extracted_locations",
  "location_source",
  "dc",
  "service_environment",
  "service_environment_detail",
  "impact",
  "tribe",
];

function createFiltered(obj, fields) {
  const result = {};
  for (const field of fields) {
    if (obj.hasOwnProperty(field)) {
      result[field] = obj[field];
    }
  }
  return result;
}

function cleanedComments(cr) {
  try {
    var comments = cr.comments || '';

    if (comments.length < 10000) {
      return comments;
    }

    comments = comments.replace(/"/g, '%%');
    comments = comments.replace(/'/g, '"');
    comments = comments.replace(/%%/g, '\\"');

    // Parse the JSON data
    const data = JSON.parse(comments);

    // Function to check if a comment is likely a system-generated message
    const isSystemComment = (comment) => {
      const systemKeywords = [
        'WorkflowId:',
        'SUMMARY:',
        'COMPLETE',
        'START',
        'hostos',
        'Deployment Complete',
        'an error occurred creating the Jira ticket'
      ];

      return systemKeywords.some(keyword => comment.includes(keyword));
    };

    // Filter the comments
    const cleanedComments = data.comments.filter(commentObject => {
      // Check if the comment text is a system message
      return !isSystemComment(commentObject.comment);
    });

    // Prepare the output object
    const output = {
      ...data,
      comments: cleanedComments
    };

    const cleanedCommentsJson = JSON.stringify(output, null, 2);

    return cleanedCommentsJson;
  }
  catch (error) {
    return "";
  }
}

module.exports = { changeAnalysis };
