const authMiddleware = (req, res, next) => {
  // Get credentials from request header
  const authHeader = req.headers.authorization;

  if (!authHeader) {
    return res.status(401).json({
      status: 401,
      error: "Authorization header is required",
      message: "Please provide Basic authentication credentials",
    });
  }

  // Check if it's Basic auth
  if (!authHeader.startsWith("Basic ")) {
    return res.status(401).json({
      status: 401,
      error: "Basic authentication is required",
      message: "Authentication must use Basic auth format",
    });
  }

  // Extract and decode credentials
  try {
    const base64Credentials = authHeader.split(" ")[1];
    const credentials = Buffer.from(base64Credentials, "base64").toString(
      "ascii"
    );
    const [username, password] = credentials.split(":");

    // Check against environment variables
    if (
      username === process.env.USERNAME &&
      password === process.env.PASSWORD
    ) {
      next();
    } else {
      return res.status(401).json({
        status: 401,
        error: "Invalid credentials",
        message: "The provided username or password is incorrect",
      });
    }
  } catch (error) {
    console.error("Authentication error: Invalid format");
    res.status(401).json({
      status: 401,
      error: "Authentication failed",
      message: "Invalid authentication format",
    });
  }
};

const mcpAuthMiddleware = (req, res, next) => {
  const authHeader = req.headers.authorization;

  if (!authHeader) {
    return res.status(401).json({
      status: 401,
      error: "Authorization header is required",
      message: "Please provide Basic authentication credentials",
    });
  }

  try {
    const base64Credentials = authHeader.split(" ")[1];
    const mcpApikey = Buffer.from(base64Credentials, "base64").toString(
      "ascii"
    );

    if (mcpApikey === process.env.CRA_MCP_SERVER_APIKEY) {
      next();
    } else {
      return res.status(401).json({
        status: 401,
        error: "Invalid credentials",
        message: "The provided username or password is incorrect",
      });
    }
  } catch (error) {
    console.error("Authentication error: Invalid format");
    res.status(401).json({
      status: 401,
      error: "Authentication failed",
      message: "Invalid authentication format",
    });
  }
};

module.exports = { authMiddleware, mcpAuthMiddleware };
