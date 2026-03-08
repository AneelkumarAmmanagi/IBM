const axios = require("axios");

const axiosConfig = {
  auth: {
    username: "apikey",
    password: process.env.CHANGE_REQUEST_API_KEY,
  },
  headers: {
    Accept: "application/json",
    "Content-Type": "application/json",
  },
};

async function fetchChangeRequestComments(changeRequest) {
  try {
    // Check if the change request has comments URL
    if (!changeRequest?.comments?.href) {
      console.log(
        `No comments URL found for change request ${changeRequest.number}`
      );
      return [];
    }

    // Make API call to fetch comments
    const response = await axios.get(
      changeRequest?.comments?.href,
      axiosConfig
    );

    if (!response.data?.comments) {
      console.log(
        `No comments found for change request ${changeRequest.number}`
      );
      return [];
    }

    console.log(
      `Retrieved ${response.data.comments.length} comments for change request ${changeRequest.number}`
    );
    return response.data?.comments || [];
  } catch (error) {
    console.error(
      `Error fetching comments for change request ${changeRequest?.number}:`,
      error?.message
    );
    throw {
      message: error.message,
      stack: error.stack,
    };
  }
}

module.exports = { fetchChangeRequestComments };
