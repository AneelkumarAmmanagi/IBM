const IBM = require("ibm-cos-sdk");
const fs = require("fs");
const path = require("path");

var config = {
  endpoint: process.env.COS_INSTANCE_ENDPOINT_URL,
  apiKeyId: process.env.COS_INSTANCE_APIKEY,
  serviceInstanceId: process.env.COS_INSTANCE_CRN,
  signatureVersion: "iam",
};

var cos = new IBM.S3(config);

const getItem = (bucketName, itemName) => {
  console.log(`Retrieving item from bucket: ${bucketName}, key: ${itemName}`);
  return cos
    .getObject({
      Bucket: bucketName,
      Key: itemName,
    })
    .promise()
    .then(async (data) => {
      if (data != null) {
        const jsonContent = JSON.parse(Buffer.from(data.Body).toString());
        console.log("jsonContent: ", jsonContent);
        const latestFile = jsonContent.latest_60dayssnapshot_file;
        console.log(`Latest fullsnapshot file: ${latestFile}`);
        await downloadTextFile(bucketName, latestFile);
      }
    })
    .catch((e) => {
      console.error(`ERROR: ${e.code} - ${e.message}\n`);
    });
};

function downloadTextFile(bucketName, itemName) {
  console.log(`fetching new item: ${itemName}`);
  return cos
    .getObject({
      Bucket: bucketName,
      Key: itemName,
    })
    .promise()
    .then((data) => {
      if (data != null) {
        const fileName = path.basename(itemName);
        const localPath = path.join(__dirname, "..", "..", fileName);
        console.log('localPath: ', localPath);
        fs.writeFileSync(localPath, data.Body);
        console.log(`Item: ${itemName} downloaded and saved to ${localPath}`);
      }
    })
    .catch((e) => {
      console.error(`ERROR: ${e.code} - ${e.message}\n`);
    });
}

// getBucketContents("obs-snow-change-requests-feed-raw");
// getItem(
//   "obs-snow-change-requests-feed-raw",
//   "obs-snow-changes-60dayssnapshot-latest.json"
// );

module.exports = { getItem };
