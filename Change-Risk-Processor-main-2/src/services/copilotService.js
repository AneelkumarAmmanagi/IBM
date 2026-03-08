const { generate } = require("../utils/copilotUtils");
const { changeAnalysis } = require("./copilot-platform-apis/change.analysis");
const {
  getPrbCorrelation,
} = require("./copilot-platform-apis/prb.correlation");

const RISK_ANALYSIS_PROMPT = `You are an expert in IT Change Management and Risk Analysis. Analyze the following change request and provide a detailed risk assessment. Your analysis must be strictly based on the provided information **without making any assumptions**. Your evaluation must ensure **internal consistency**, and the final risk score and risk level must accurately match. Do **not hallucinate, infer, or invent any values**.

Evaluate each risk factor using the provided criteria and assign a score from 0–10. Use only the available data to evaluate each category. Do **not guess** or assume intent. Missing or vague information should be penalized appropriately.

Each factor has a defined weight. The final risk score is calculated as a weighted average and modified by specific risk elevators or mitigators. Then the score must be normalized to a maximum of 10. Do not exceed this cap.

### RISK FACTORS (with weights and detailed evaluation rules):

1. **Backout Plan Analysis (Weight: 25%)**:
   - Score 10: No backout plan provided or only states "revert changes"
   - Score 7–9: Vague/incomplete plan lacking detailed steps
   - Score 4–6: Basic plan with some steps but lacks verification or testing
   - Score 1–3: Clear step-by-step plan, partial validation
   - Score 0: Fully detailed plan with steps, validation, rollback testing

2. **Duration Impact (Weight: 15%)**:
   - Calculate based on time difference between planned_end and planned_start
   - >8 hours: Score 10
   - 4–8 hours: Score 8–9
   - 2–4 hours: Score 5–7
   - 1–2 hours: Score 2–4
   - <1 hour: Score 0–1

3. **Zone/Region Impact (Weight: 15%)**:
   - Global/multiple production regions: Score 10
   - Single region, critical system: Score 7–9
   - Single region, non-critical system: Score 4–6
   - Non-production/test region only: Score 0–3

4. **Customer Impact (Weight: 15%)**:
   - Multiple enterprise clients: Score 10
   - Single enterprise client: Score 7–9
   - General customer base (non-enterprise): Score 4–6
   - No customer impact: Score 0–3

5. **Configuration Complexity (Weight: 10%)**:
   - Multiple critical changes without PR: Score 10
   - Multiple changes with PR: Score 7–9
   - Single core change with PR: Score 4–6
   - Minor/non-core changes: Score 0–3

6. **Environment Risk (Weight: 10%)**:
   - Production-critical systems: Score 10
   - Production non-critical systems: Score 6–9
   - Pre-production/staging: Score 3–5
   - Development/test: Score 0–2

7. **Deployment Impact (Weight: 10%)**:
   - Complex manual deployment: Score 10
   - Multi-system deployment: Score 7–9
   - Single system deployment: Score 4–6
   - Automated with rollback: Score 0–3

8. **Peak Hour Analysis (Weight: 5%)**:
   - Use ONLY peak_hour_analysis.peak_hour value
   - 10–16 (inclusive): Score 10
   - >8 to <10 OR >16 to <18: Score 7–9
   - >6 to <8 OR >18 to <20: Score 4–6
   - All other values: Score 0–3

### RISK SCORE CALCULATION:

1. **Weighted average** = Sum of (factor_score * weight)
2. Apply modifiers:
   - +2.0 if backout plan = 10
   - +1.0 if environment = production critical
   - +1.0 if deployment = complex manual
   - +1.0 if configuration = multiple changes
   - -0.5 if deployment = automated
   - -0.5 if customer impact = no impact

3. **Normalize final score to a 0–10 scale**. Final score must not exceed 10.

### RISK CATEGORIES:

- **HIGH RISK**: Score ≥ 7.0
- **MODERATE RISK**: 3.5 ≤ Score < 7.0
- **LOW RISK**: Score < 3.5

### SUMMARY FORMAT:

- Summary must begin with: HIGH RISK / MODERATE RISK / LOW RISK
- Write a 200–250 word assessment summary that reflects the numeric score and findings.
- Include a 20–30 word "risk_summary" field: briefly explain why the score was reached (e.g., "No rollback, high duration, customer impact").
- Also add a 20-30 words "close_notes_summary" field: briefly summarize the data and give information from the close_notes data in the object.
- Also add a 20-30 words "comments_summary" field: briefly summarize the data and give information from the comments data in the object.
- Do not contradict score or thresholds. Do not exceed 10 in final_score under any circumstance.

### THEME:

Choose **exactly one** theme that best matches the nature of the change:
- Bug Fixes
- Patches
- Service Upgrade
- Configuration Changes
- Capacity Scaling
- Security Hardening
- Infrastructure migration
- Feature release
- Compliance and regulatory updates
- Performance optimization
- API changes
- Monitoring/observability changes
- Hardware
- Operational change

Use only provided info to infer theme. Do not speculate or choose multiple.

### OUTPUT STRUCTURE:
Return only valid JSON in the following format. Do not include any other text.

{
  "risk_factors": [
    {
      "factor": "string",
      "finding": "string",
      "score": number,
      "reason": "string"
    }
  ],
  "final_score": number,
  "risk_elevators": [
    {
      "factor": "string",
      "impact": number
    }
  ],
  "mitigating_factors": [
    {
      "factor": "string",
      "impact": number
    }
  ],
  "summary": "string",
  "risk_summary":"string",
  "close_notes_summary":"string",
  "comments_summary":"string",
  "theme":"string",
  "recommendations": [
    "string"
  ],
  "approval_level": "string",
  "technical_details": {
    "complexity": "string",
    "impact_scope": "string",
    "implementation_plan": "string",
    "rollback_capability": "string"
  }
}
`;

async function analysePeakHour(changeRequest) {
  const plannedStart = new Date(changeRequest.planned_start);
  let localTime;

  const timeZoneAdjustments = {
    "us-south": -5,
    "us-east": -4,
    "br-sao": -3,
    "ca-tor": -4,
    "ca-mon": -4,
    "au-syd": 10,
    "jp-osa": 9,
    "jp-tok": 9,
    "eu-de": 2,
    "eu-gb": 1,
    "eu-es": 2,
    "eu-fr2": 2,
  };
  const adjustment = timeZoneAdjustments[changeRequest?.regions?.[0]] || 0;
  localTime = new Date(plannedStart.getTime() + adjustment * 60 * 60 * 1000);
  const localHours = localTime.getUTCHours();
  const peakHour = localHours >= 8 && localHours < 18;

  const dayOfWeekNum = localTime.getUTCDay();
  const dayOfWeekStr = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
  ][dayOfWeekNum];

  const analysis = {
    peak_hour_analysis: {
      local_time: localTime.toISOString(),
      peak_hour: localHours,
      is_peak_hour: peakHour,
      region: changeRequest?.regions?.[0],
      day_of_week: dayOfWeekStr,
    },
  };
  return analysis;
}

async function analyzeCopilot(changeRequest) {
  try {
    console.log(`\n=== Analyzing Change Request ${changeRequest.number} ===`);
    console.log("Change Description:", changeRequest.short_description);

    const prompt = `Analyze the following change request:\n${JSON.stringify(
      changeRequest,
      null,
      2
    )}`;
    const getPeakHourAnalysis = await analysePeakHour(changeRequest);
    // const [error, response] = await generate(
    //   prompt,
    //   RISK_ANALYSIS_PROMPT + `${JSON.stringify(getPeakHourAnalysis, null, 2)}`,
    //   0.7
    // );

    // console.log("response", JSON.stringify(response, null, 4));

    // console.log(
    //   "response from copiltService",
    //   response?.choices[0].message.content
    // );

    const [[error, response], [prbError, prbResponse]] = await Promise.all([
      changeAnalysis(changeRequest, getPeakHourAnalysis),
      getPrbCorrelation(changeRequest?.number),
    ]);

    if (error) {
      throw {
        message: error.message,
        stack: error.stack,
      };
    }

    if (prbError) {
      console.error(
        "Error fetching the prb data:",
        prbError?.message
      );
    }

    let analysis = response?.result;
    // If analysis is a string, try to parse it as JSON
    let analysisObj;
    try {
      analysisObj =
        typeof analysis === "object" ? analysis : JSON.parse(analysis);
    } catch (e) {
      analysisObj = analysis;
    }

    // Adding the prb correlation data to the risk_factors
    const riskFactorWeights = {
      "Backout Plan Analysis": 20,
      "Duration Impact": 12,
      "Zone/Region Impact": 12,
      "Customer Impact": 12,
      "Configuration Complexity": 9,
      "Environment Risk": 9,
      "Deployment Impact": 9,
      "Peak Hour Analysis": 5,
      "Problem Correlation": 12,
      "Closure Notes Review": 10,
      "Pre Check Analysis": 20,
      "Post-Checks Validation": 20,
    };

    if (analysisObj?.risk_factors && Array.isArray(analysisObj.risk_factors)) {
      analysisObj.risk_factors.push(prbResponse);

      let weightedScoreSum = 0;
      let totalWeight = 0;

      analysisObj.risk_factors.forEach((item) => {
        const riskScore = item.score || 0;
        const weight = riskFactorWeights[item.factor] || 0;
        weightedScoreSum += riskScore * weight;
        totalWeight += weight;
      });

      analysisObj.final_score = totalWeight
        ? parseFloat((weightedScoreSum / totalWeight).toFixed(1))
        : 0;
    }

    // Set peak_hour_analysis from input
    if (analysisObj && typeof analysisObj === "object") {
      analysisObj.peak_hour_analysis = getPeakHourAnalysis.peak_hour_analysis;
    }

    console.log(`\n=== End of Analysis ${changeRequest.number} ===\n`);

    return analysisObj;
  } catch (error) {
    console.error(
      "Error analyzing change request with Copilot:",
      error?.message
    );
    // Return a default structure instead of throwing the error
    return {
      risk_score: 0,
      risk_factors: [],
      recommendations: [`Error during analysis: ${error.message}`],
      analysis_timestamp: new Date().toISOString(),
      error: error.message,
    };
  }
}

module.exports = { analyzeCopilot };
