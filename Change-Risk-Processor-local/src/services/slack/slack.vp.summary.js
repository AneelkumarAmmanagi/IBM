const axios = require("axios");
const { fetchTodaysManualVpChanges } = require("../vp-reports/vp.24hrs.changes");

const MAX_TEXT_LENGTH = 2900;

const getColorForScore = (score) => {
  if (score >= 8) return "#FF3B30";
  if (score >= 6 && score<8) return "#FF9500";
  if (score >= 4 && score<6) return "#FFD60A";
  if(score>0 && score<4) return "#34C759";
  return "#D3D3D3";
};

const normalizeDescription = (description, fallback = "No description available") => {
  if (!description) return fallback;

  if (typeof description === "string") {
    const trimmed = description.replace(/\s+/g, " ").trim();
    return trimmed.length > 0 ? trimmed : fallback;
  }

  if (typeof description === "object") {
    const segments = Object.entries(description)
      .filter(([, v]) => typeof v === "string" && v.trim().length > 0)
      .map(
        ([key, value]) =>
          `${key.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())}: ${value
            .replace(/\s+/g, " ")
            .trim()}`
      );
    return segments.length > 0 ? segments.join(" | ") : fallback;
  }
  return fallback;
};

const formatChangeRow = (change) => {
  const id = change.number || "N/A";
  const region = change.region || "unknown";

  let description =
    normalizeDescription(change?.description?.business_justification) ||
    normalizeDescription(change?.description) ||
    "No description available";

  if (description.length > MAX_TEXT_LENGTH) {
    description = description.slice(0, MAX_TEXT_LENGTH - 3) + "...";
  }

  const score = change?.score ?? 0;
  const color = getColorForScore(score);
  const url = change?.url;

  return { id, region, description, score, color, url };
};
  
const formatChangeText = (row) => {
  const idText = row.url ? `<${row.url}|${row.id}>` : row.id;
  return `*${idText}* – \`${row.region}\` \n${row.description}`;
};

const createChangeAttachment = (row) => ({
  color: row.color,
  fallback: `${row.id} – ${row.region}`,
  blocks: [
    {
      type: "section",
      text: {
        type: "mrkdwn",
        text: formatChangeText(row),
      },
    },
  ],
});
  
  
  const buildPlatformMessage = (platformSummary, includeDate = false) => {
    const today = new Date();
    const formattedDate = today.toLocaleDateString("en-US", {
      weekday: "long",
      year: "numeric",
      month: "long",
      day: "numeric",
    });
  
    const blocks = [];
    let attachments = [];
  
    if (includeDate) {
      blocks.push(
        {
          type: "header",
          text: { type: "plain_text", text: "📊 Daily Change Summary", emoji: true },
        },
        {
          type: "section",
          text: { type: "mrkdwn", text: `*${formattedDate}*` },
        },
        { type: "divider" }
      );
    }
  
    if (platformSummary.error) {
      blocks.push({
        type: "section",
        text: {
          type: "mrkdwn",
          text: `*:warning: ${platformSummary.platform.toUpperCase()}*\n>${platformSummary.error}`,
        },
      });
    } else {
      const { documents, platform, serviceGroups = [], total } = platformSummary;
  
        blocks.push({
            type: "section",
            text: {
              type: "mrkdwn",
              text: `*${platform.toUpperCase()}* ${ 
                total === 0 
                  ? `*_No manual changes detected_*` 
                  : `*( ${total} manual ${total === 1 ? "change" : "changes"} )*`
              }`,
            },
          });          
  
      if (serviceGroups.length > 0) {
        blocks.push({
          type: "context",
          elements: [
            { type: "mrkdwn", text: `*Service Groups:* ${serviceGroups.map((sg) => `\`${sg}\``).join(" • ")}` },
          ],
        });
      }
  
      if (documents.length > 0) {
        attachments = documents.map((doc) =>
          createChangeAttachment(formatChangeRow(doc))
        );
      }
    }
  
    return { blocks, attachments };
  };
  
  const sendVpDailySummaryToSlack = async (options = {}) => {
    const {
      webhookUrl = process.env.SLACK_VP_WEBHOOK_URL,
      aiRiskScore,
      platforms,
    } = options;
  
    if (!webhookUrl) {
      throw new Error("Slack webhook URL is required to post VP daily summary");
    }
  
    const platformSummaries = await fetchTodaysManualVpChanges({
      aiRiskScore,
      platforms,
    });
  
    const results = [];
  
    for (let i = 0; i < platformSummaries.length; i++) {
      const platformSummary = platformSummaries[i];
       const includeDate = i === 0;

       const { blocks, attachments } = buildPlatformMessage(platformSummary, includeDate);

       try {
         await axios.post(webhookUrl, {
           text: `VP Daily Change Summary - ${platformSummary.platform.toUpperCase()}`,
           blocks,
           attachments,
         });
  
        results.push({ platform: platformSummary.platform, success: true });
      } catch (error) {
        results.push({ platform: platformSummary.platform, success: false, error: error.message });
      }
  
      if (i < platformSummaries.length - 1) {
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    }
  
    return {
      posted: true,
      platformsProcessed: platformSummaries.length,
      results,
    };
  };
  
  module.exports = {
    sendVpDailySummaryToSlack,
  };