const axios = require("axios");

const sendOutageMessageToSlack = async (outages) => {
  console.log("outages", outages);
  const MAX_OUTAGE_HOURS = 4;
  const exceededServices = Object.entries(outages).filter(
    ([, value]) => value?.outage_duration > MAX_OUTAGE_HOURS
  );

  console.log("exceededServices", exceededServices);

  if (exceededServices.length === 0) {
    console.log("✅ No services exceeded the 4-hour outage threshold.");
    return;
  }

  const blocks = [
    {
      type: "section",
      text: {
        type: "mrkdwn",
        text: "🚨 *Outage Alert:* One or more services have exceeded the *monthly outage threshold* (4 hours).",
      },
    },
    { type: "divider" },
  ];

  exceededServices.forEach(([serviceName, duration]) => {
    console.log("serviceName duration", serviceName, duration);
    blocks.push({
      type: "section",
      text: {
        type: "mrkdwn",
        text: `:rotating_light: *${serviceName}* → *${duration.outage_duration} hours* of outages this month.`,
      },
    });
  });

  try {
    const response = await axios.post(process.env.SLACK_WEBHOOK_URL, {
      blocks,
    });
    console.log("🚀 Outage alert sent to Slack:", response.data);
  } catch (error) {
    console.error("❌ Error sending outage alert to Slack:", error.message);
  }
};

module.exports = {
  sendOutageMessageToSlack,
};
