const getPrbCorrelation = async (changeNumber) => {
  try {
    const prbAnalysisResult = await fetch(
      `${process.env.COPILOT_PLATFORM_PRB_ANALYSIS_URL}`,
      {
        method: "POST",
        headers: {
          "Content-Type": "text/plain",
          apiKey: `${process.env.COPILOT_PLATFORM_PRB_ANALYSIS_API_KEY}`,
        },
        body: JSON.stringify({
          change_number: changeNumber,
        }),
      }
    );

    if (!prbAnalysisResult.ok) {
      const errorData = await prbAnalysisResult.json();
      return [
        errorData.error ||
          `Request failed with status ${prbAnalysisResult.status}`,
        null,
      ];
    }

    const data = await prbAnalysisResult.json();

    let prbSummary = null;

    if (Array.isArray(data?.result?.anomalyDetectionList)) {
      const list = data?.result?.anomalyDetectionList;
      const avgScore =
        list.reduce((sum, item) => sum + (item.riskScore || 0), 0) /
        list.length;

      console.log("list", list);

      prbSummary = {
        reason: data?.result?.reason,
        score: Math.round(avgScore * 10 * 100) / 100,
        factor: "Problem Correlation",
        finding: data?.result?.findings,
      };
    }

    console.log(
      `Data returned from prb analysis for ${changeNumber}: ${JSON.stringify(
        data,
        null,
        2
      )}`
    );
    return [undefined, prbSummary];
  } catch (err) {
    console.log("Err returned from prb analysis:", err?.message);
    return [err, undefined];
  }
};

module.exports = { getPrbCorrelation };
