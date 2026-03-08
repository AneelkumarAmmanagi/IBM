const fetch = require('node-fetch');
require("dotenv").config();

/**
 * Fetches the content from given GitHub url & return the content
 * @param {string} url 
 * @returns {string} url content
 */
async function getGithubUrlContent(url) {
  // Converts Github url to Github API url
  // console.log(`url=${url}`);
  apiUrl = convertIBMGitHubUrlToApi(url)
  // console.log(`apiUrl=${apiUrl}`);


  const token = process.env.GITHUB_TOKEN;

  const headers = {
    'Authorization': `Bearer ${token}`,
    'Content-Type': 'application/json',
  };
  try {
    // Read the file
    const response = await fetch(apiUrl, {
      method: 'GET',
      headers: headers,
    });
    if (!response.ok) {
      console.error(`HTTP error! status: ${response.status}`);
      return '';
    }
    const data = await response.json();
    content = data.content;

    // Decode the fetched content in base 64 format
    const decoded = atob(content);
    return decoded;
  } catch (error) {
    console.error("Error fetching github content:", error);
    return '';
  }
}

function convertIBMGitHubUrlToApi(url) {
  const regex = /^https:\/\/github\.ibm\.com\/([^\/]+)\/([^\/]+)\/blob\/([^\/]+)\/(.+)$/;
  const match = url.match(regex);

  if (!match) {
    throw new Error("Invalid GitHub IBM URL format");
  }

  const [, org, repo, branch, path] = match;

  return `https://github.ibm.com/api/v3/repos/${org}/${repo}/contents/${path}?ref=${branch}`;
}

async function getOrCreateDailyIssue() {
  const repo = process.env.GITHUB_REPO;
  const owner = process.env.GITHUB_OWNER;
  const token = process.env.GITHUB_TOKEN;
  const dateStr = new Date().toISOString().slice(0, 10);
  const title = `Change Risk Daily Summary - ${dateStr}`;
  // IBM GitHub Enterprise base URL
  const apiBase = `https://github.ibm.com/api/v3/repos/${owner}/${repo}`;
  const headers = {
    'Authorization': `Bearer ${token}`,
    'Accept': 'application/vnd.github+json',
    'Content-Type': 'application/json'
  };

  const listResp = await fetch(`${apiBase}/issues?state=open&per_page=50`, { headers });
  if (!listResp.ok) throw new Error(`GitHub /issues fetch failed: ${listResp.status}`);
  const issues = await listResp.json();
  let issue = issues.find(i => i.title === title);

  if (issue) {
    return issue.number;
  }

  const createResp = await fetch(`${apiBase}/issues`, {
    method: 'POST',
    headers,
    body: JSON.stringify({ title, body: 'Auto-created for daily change-risk summary.', labels: ['automation', 'daily-summary'] })
  });
  if (!createResp.ok) {
    const data = await createResp.text();
    throw new Error(`GitHub issue creation failed: ${createResp.status} - ${data}`);
  }
  const newIssue = await createResp.json();
  return newIssue.number;
}

async function postCommentToIssue(issueNumber, markdown) {
  const repo = process.env.GITHUB_REPO;
  const owner = process.env.GITHUB_OWNER;
  const token = process.env.GITHUB_TOKEN;
  const apiBase = `https://github.ibm.com/api/v3/repos/${owner}/${repo}`;

  const headers = {
    'Authorization': `Bearer ${token}`,
    'Accept': 'application/vnd.github+json',
    'Content-Type': 'application/json'
  };
  const url = `${apiBase}/issues/${issueNumber}/comments`;
  const resp = await fetch(url, {
    method: 'POST',
    headers,
    body: JSON.stringify({ body: markdown })
  });
  if (!resp.ok) {
    const data = await resp.text();
    throw new Error(`GitHub comment failed: ${resp.status} - ${data}`);
  }
  const comment = await resp.json();
  return comment;
}

async function postDailySummaryToGithub(markdown) {
  try {
    const issueNumber = await getOrCreateDailyIssue();
    const comment = await postCommentToIssue(issueNumber, markdown);
    console.log(`✅ GitHub comment posted: ${comment.html_url}`);
    return comment;
  } catch (err) {
    console.error('❌ GitHub summary post failed:', err.message || err);
    throw err;
  }
}

module.exports = { getGithubUrlContent, postDailySummaryToGithub };
