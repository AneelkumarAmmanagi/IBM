const duckdb = require("duckdb");
const path = require("path");
const fs = require("fs");
const { configNames, serviceGroupNames } = require("../../constants/constants");

const readParquetFile = async () => {
  const db = new duckdb.Database(":memory:");
  const connection = db.connect();

  try {
    // Get current directory
    const currentDir = path.dirname(__filename);
    const parentDir = path.join(currentDir, "..", "..");

    // Find the most recent parquet file in the directory
    const files = fs
      .readdirSync(parentDir)
      .filter((file) => file.endsWith(".parquet"));

    if (files.length === 0) {
      throw new Error("No parquet files found in the current directory");
    }

    // Sort files by date (assuming filename contains timestamp)
    const latestFile = files.sort().reverse()[0];
    const filePath = path.join(parentDir, latestFile);

    console.log(`Reading parquet file: ${latestFile}`);
    const cleanedNames = serviceGroupNames.map(name => `'${name.trim().replace(/'/g, "''")}'`).join(", ");

    const validServicesQuery = `
  SELECT *
  FROM read_parquet('${filePath}')
  WHERE TRIM(tribe) IN (${cleanedNames})
`;

    console.log("validServicesQuery:", validServicesQuery);

    const records = await new Promise((resolve, reject) => {
      connection.all(validServicesQuery, (err, rows) => {
        if (err) return reject(err);
        resolve(rows);
      });
    });

    // Filter records based on state, service names, and planned start time
    // const validStates = ["Implement", "Scheduled", "Review"];
    // const now = new Date();
    // const oneWeekAgo = new Date();
    // oneWeekAgo.setDate(oneWeekAgo.getDate() - 7);

    const filteredRecords = records.filter((record) => {
      try {
        if (
          !record ||
          !record.state ||
          !record.tribe ||
          !record.service_names ||
          !Array.isArray(record.service_names)
        ) {
          return false;
        }
        // const plannedStartDate = new Date(record.planned_start);
        // if (isNaN(plannedStartDate.getTime())) {
        //   return false;
        // }
        return serviceGroupNames.includes(record.tribe?.trim());
      } catch (error) {
        console.error(`Error filtering record: ${error.message}`);
        return false;
      }
    });

    console.log(
      `Read ${records.length} total records, filtered to ${filteredRecords.length} records with valid states from ${latestFile}`
    );
    return filteredRecords;
  } catch (error) {
    console.error(`Error reading parquet file: ${error.message}`);
    throw error;
  } finally {
    connection.close();
  }
};

module.exports = { readParquetFile };
