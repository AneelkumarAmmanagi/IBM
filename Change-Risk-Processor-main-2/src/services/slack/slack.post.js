const axios = require("axios");
const { fetchingRiskyCRs } = require("./slack.riskcr.analysis");

const webhookURL = process.env.SLACK_WEBHOOK_URL;

// const message = {
//   text: "Hello, Slack! :wave:",
// };

const sendMessageToSlack = async () => {
  const [errFetchingScores, fetchedScoresData] = await fetchingRiskyCRs();
  if (fetchedScoresData.length === 0) {
    message = "No records found with final_score >= 8.";
    await axios
      .post(webhookURL, message)
      .then((response) => {
        console.log("Message sent to Slack:", response.data);
      })
      .catch((error) => {
        console.error("Error sending message:", error);
      });
  }

  if (errFetchingScores) {
    message = "Error fetching data or sending to Slack:" + errFetchingScores;
    await axios
      .post(webhookURL, message)
      .then((response) => {
        console.log("Message sent to Slack:", response.data);
      })
      .catch((error) => {
        console.error("Error sending message:", error);
      });
  }
  if (fetchedScoresData.length > 0) {
    const blocks = [
      {
        type: "section",
        text: {
          type: "mrkdwn",
          text: "*⚡ High Final Score Results Grouped by Region and Data Center ⚡*",
        },
      },
      { type: "divider" },
    ];

    // Step 1: Group data by region → then by data center, count records & sum risk scores
    const groupedData = {};

    fetchedScoresData.forEach((record) => {
      record.regions.forEach((region) => {
        if (!groupedData[region]) {
          groupedData[region] = {};
        }

        record.dc.forEach((dataCenter) => {
          if (!groupedData[region][dataCenter]) {
            groupedData[region][dataCenter] = { count: 0, totalRisk: 0 };
          }

          groupedData[region][dataCenter].count += 1; // Count the number of records per DC
          groupedData[region][dataCenter].totalRisk += record.final_score; // Sum the risk scores
        });
      });
    });

    // Step 2: Build Slack blocks based on the grouped data
    Object.entries(groupedData).forEach(([region, dcs]) => {
      Object.entries(dcs).forEach(([dc, data]) => {
        const averageRisk =
          data.count > 0 ? (data.totalRisk / data.count).toFixed(2) : 0;

        blocks.push({
          type: "section",
          text: {
            type: "mrkdwn",
            text: `*${region}* → *${dc}* → *${data.count} Changes*, *Cumulative Risk:* ${data.totalRisk}, *Average Risk:* ${averageRisk}`,
          },
        });
      });

      blocks.push({ type: "divider" }); // Separate regions
    });

    // Step 3: Add service-based grouping section
    blocks.push(
      {
        type: "section",
        text: {
          type: "mrkdwn",
          text: "*⚡ High Final Score Results Grouped by Service Name ⚡*",
        },
      },
      { type: "divider" }
    );

    // Group data by service_names → then by region and data center
    const serviceGroupedData = {};

    fetchedScoresData.forEach((record) => {
      // Handle if service_names is a string or array
      const serviceNames = Array.isArray(record.service_names)
        ? record.service_names
        : [record.service_names];

      serviceNames.forEach((serviceName) => {
        if (!serviceGroupedData[serviceName]) {
          serviceGroupedData[serviceName] = {};
        }

        record.regions.forEach((region) => {
          if (!serviceGroupedData[serviceName][region]) {
            serviceGroupedData[serviceName][region] = {};
          }

          record.dc.forEach((dataCenter) => {
            if (!serviceGroupedData[serviceName][region][dataCenter]) {
              serviceGroupedData[serviceName][region][dataCenter] = {
                count: 0,
                totalRisk: 0,
              };
            }

            serviceGroupedData[serviceName][region][dataCenter].count += 1;
            serviceGroupedData[serviceName][region][dataCenter].totalRisk +=
              record.final_score;
          });
        });
      });
    });

    // Build Slack blocks based on the service-grouped data
    Object.entries(serviceGroupedData).forEach(([serviceName, regions]) => {
      Object.entries(regions).forEach(([region, dcs]) => {
        Object.entries(dcs).forEach(([dc, data]) => {
          const averageRisk =
            data.count > 0 ? (data.totalRisk / data.count).toFixed(2) : 0;

          blocks.push({
            type: "section",
            text: {
              type: "mrkdwn",
              text: `*${serviceName}* -> *${region}* -> *${dc}* → *${data.count} Changes*, *Cumulative Risk:* ${data.totalRisk}, *Average Risk:* ${averageRisk}`,
            },
          });
        });
      });

      blocks.push({ type: "divider" }); // Separate services
    });

    console.log(blocks); // Output for debugging

    await axios
      .post(webhookURL, { blocks })
      .then((response) => {
        console.log("Message sent to Slack:", response.data);
      })
      .catch((error) => {
        console.error("Error sending message:", error);
      });
  }
};

module.exports = {
  sendMessageToSlack,
};
