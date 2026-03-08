const { Client } = require('@elastic/elasticsearch');

// Add initial configuration logging
console.log('Initializing Elasticsearch service...');

// Log configuration details (excluding sensitive data)
console.log('Creating Elasticsearch client with config:', {
  maxRetries: 5,
  requestTimeout: 60000,
});

const client = new Client({
  node: process.env.ELASTIC_SERVER_HOST,
  auth: {
    username: process.env.ELASTIC_USER,
    password: process.env.ELASTIC_PASSWORD,
  },
  tls: {
    ca: process.env.ELASTIC_CERTIFICATE,
    rejectUnauthorized: false,
  },
});

module.exports = { client };
