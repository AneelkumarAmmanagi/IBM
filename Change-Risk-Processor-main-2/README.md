# Change-Risk-Processor

A service to process and analyze change requests and store results. This service provides risk analysis for change requests by leveraging AI capabilities and maintains a structured storage system for analysis results.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
  - [Standard Setup](#standard-setup)
  - [Docker Deployment](#docker-deployment)
- [Configuration](#configuration)
  - [Environment Variables](#environment-variables)
- [Usage](#usage)
  - [Development](#development)
  - [API Endpoints](#api-endpoints)
- [Scripts](#scripts)
- [Dependencies](#dependencies)

## Overview

Change-Risk-Processor is a service that analyzes change requests for risk assessment using AI capabilities. It integrates with Elasticsearch for data storage and provides a RESTful API for interacting with the system.

## Features

- Automated risk analysis for change requests
- Elasticsearch integration for persistent storage
- Copilot AI integration for intelligent analysis
- RESTful API endpoints
- Docker support for containerized deployment
- Automatic document archival
- Cron job for periodic processing

## Prerequisites

- Node.js (v20 or higher)
- Docker (optional, for containerized deployment)
- Access to Elasticsearch instance
- Change Request API access

## Installation

### Standard Setup

1. Clone the repository:
```bash
git clone https://github.ibm.com/Assistant/Change-Risk-Processor
cd Change-Risk-Processor
```

2. Install dependencies:
```bash
npm install
```

3. Set up environment variables (see [Environment Variables](#environment-variables))

4. Start the service:
```bash
npm run start
```

### Docker Deployment

1. Build the Docker image:
```bash
docker build -t change-risk-processor .
```

2. Run the container:
```bash
docker run -p 8080:8080 -v $(pwd)/certs:/usr/app/certs --env-file .env change-risk-processor
```

## Configuration

### Environment Variables

Create a `.env` file in the root directory with the following variables:

```env
# Server Configuration
PORT=8080
USERNAME=your_username
PASSWORD=your_password

# Change Request API Configuration
CHANGE_REQUEST_API_KEY=your_change_request_api_key
CHANGE_REQUEST_API_URL=your_change_request_api_url

# Elasticsearch Configuration
ELASTIC_USER=your_elastic_user
ELASTIC_CERTIFICATE=your_elastic_certificate
ELASTIC_PASSWORD=your_elastic_password
ELASTIC_SERVER_HOST=your_elastic_server_host
ELASTIC_INDEX=your_elastic_index

# Copilot API Configuration
COPILOT_API_KEY=your_copilot_api_key
COPILOT_HOST=your_copilot_host
CHAT_ID=your_chat_id

# Cloud Object Storage Configuration
COS_INSTANCE_ENDPOINT_URL=your_cos_instance_endpoint_url
COS_INSTANCE_CRN=your_cos_instance_crn
COS_INSTANCE_APIKEY=your_cos_instance_apikey

# Slack Integration
SLACK_WEBHOOK_URL=your_slack_webhook_url
SLACK_TOKEN=your_slack_token
SLACK_CHANNEL_ID=your_slack_channel_id

# Data Sync Configuration
DATASYNC_SERVER_URL=your_datasync_server_url
DATASYNC_SERVER_API_KEY=your_datasync_server_api_key
```

> **Note:** Replace all `your_*` values with your actual credentials. Do not commit sensitive information to version control.

## Usage

### Development

1. Start in development mode with hot-reload:
```bash
npm run dev
```

2. The service will be available at `http://localhost:8080`

### API Endpoints

Below are some of the main API endpoints (see `src/index.js` and service files for full details):

- **GET /analyzed-documents**: Retrieve analyzed documents
- **GET /all-documents**: Retrieve all documents
- **POST /analyze-risk**: Analyze risk for a change request
- **GET /fetch-analysed-result**: Fetch analysis result for a document
- **POST /query-elastic**: Query Elasticsearch data
- **GET /outages**: Fetch current month outages

> **Authentication:** Most endpoints require basic authentication using the `USERNAME` and `PASSWORD` from your `.env` file.

## Scripts

- `npm run start`: Start the server
- `npm run dev`: Start the server with hot-reload (development)

## Dependencies

- **Web Framework**: express, cors
- **Environment**: dotenv
- **Database**: @elastic/elasticsearch, duckdb
- **AI Integration**: @modelcontextprotocol/sdk
- **HTTP Client**: axios
- **Cloud Storage**: ibm-cos-sdk
- **Utilities**: lodash, node-cron, zod