const axios = require("axios");

const SYSTEM_PROMPT = {
  DEFAULT:
    "You are an AI assistant helping with IT Change Management and Risk Analysis.",
};

const OUTPUT_SCHEMA = {
  type: "object",
  properties: {
    risk_factors: {
      description: "Detailed risk contributors with reasoning and score",
      type: "array",
      items: {
        type: "object",
        properties: {
          factor: {
            description: "Risk factor name",
            type: "string",
          },
          finding: {
            description: "Observed issue or context related to the factor",
            type: "string",
          },
          score: {
            description: "Numerical representation of risk severity",
            type: "number",
          },
          reason: {
            description: "Explanation for assigned score",
            type: "string",
          },
        },
        required: ["factor", "finding", "score", "reason"],
      },
    },
    final_score: {
      description: "Cumulative risk score based on all factors",
      type: "number",
    },
    risk_elevators: {
      description: "Factors that increase the risk",
      type: "array",
      items: {
        type: "object",
        properties: {
          factor: {
            description: "Elevator factor name",
            type: "string",
          },
          impact: {
            description: "Impact on risk score",
            type: "number",
          },
        },
        required: ["factor", "impact"],
      },
    },
    mitigating_factors: {
      description: "Factors that reduce the risk",
      type: "array",
      items: {
        type: "object",
        properties: {
          factor: {
            description: "Mitigating factor name",
            type: "string",
          },
          impact: {
            description: "Impact on reducing risk",
            type: "number",
          },
        },
        required: ["factor", "impact"],
      },
    },
    summary: {
      description: "Overall summary of the assessment",
      type: "string",
    },
    risk_summary: {
      description: "High-level summary of risk posture",
      type: "string",
    },
    close_notes_summary: {
      description: "Important data from the close_notes.",
      type: "string",
    },
    comments_summary: {
      description: "Summary of comments",
      type: "string",
    },
    theme: {
      description: "Overarching theme or pattern in risk profile",
      type: "string",
    },
    recommendations: {
      description: "Actionable suggestions to mitigate or manage risk",
      type: "array",
      items: {
        type: "string",
      },
    },
    approval_level: {
      description: "Level of authorization needed based on the risk",
      type: "string",
    },
    technical_details: {
      description: "Technical implementation and planning metadata",
      type: "object",
      properties: {
        complexity: {
          description: "Overall complexity of the effort",
          type: "string",
        },
        impact_scope: {
          description: "Scope of systems, services, or users affected",
          type: "string",
        },
        implementation_plan: {
          description: "Summary of the rollout or implementation steps",
          type: "string",
        },
        rollback_capability: {
          description: "Ability to revert the change if needed",
          type: "string",
        },
      },
      required: [
        "complexity",
        "impact_scope",
        "implementation_plan",
        "rollback_capability",
      ],
    },
  },
  required: [
    "risk_factors",
    "final_score",
    "risk_elevators",
    "mitigating_factors",
    "summary",
    "risk_summary",
    "theme",
    "recommendations",
    "approval_level",
    "technical_details",
  ],
};

const generate = async (prompt, systemPrompt, temperature = 0.5, chatId) => {
  const options = {
    method: "POST",
    headers: { Authorization: `Bearer ${process.env.COPILOT_API_KEY}` },
    url: `${process.env.COPILOT_HOST}/v1/chat/completions`,
    data: {
      model: process.env.model || "copilot-llama-medium",
      stream: false,
      messages: [
        {
          role: "system",
          content: systemPrompt || SYSTEM_PROMPT.DEFAULT,
        },
        {
          role: "user",
          content: prompt,
        },
      ],
      options: {
        temperature,
        context: chatId ? true : false,
      },
      response_format: {
        type: "json_schema",
        json_schema: {
          name: "response",
          schema: OUTPUT_SCHEMA,
        },
      },
    },
    // timeout: 30000, // 30 seconds timeout
    // validateStatus: (status) => status >= 200 && status < 500, // Handle only 5xx errors as failures
  };

  console.log("Sending request to:", options.url);

  try {
    const response = await axios.request(options);
    if (!response?.data) {
      throw new Error("No data received from Copilot API");
    }
    console.log("Response received from Copilot API");
    return [null, response.data];
  } catch (error) {
    const errorMessage = error.response
      ? `API Error: ${error.response.status} - ${JSON.stringify(
          error.response.data
        )}`
      : error.code === "ECONNABORTED"
      ? "Request timeout - the API took too long to respond"
      : `Request failed: ${error.message}`;

    console.error("Copilot API Error:", errorMessage);
    return [new Error(errorMessage), null];
  }
};

module.exports = { generate, SYSTEM_PROMPT };
