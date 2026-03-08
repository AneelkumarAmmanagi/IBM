const fetch = require("node-fetch");

function getDayBounds(daySpecifier) {
  if (!daySpecifier) {
    return null;
  }

  const normalized = daySpecifier.trim().toLowerCase();
  const offsets = {
    today: 0,
    yesterday: -1,
  };

  if (!(normalized in offsets)) {
    return null;
  }

  const targetDate = new Date();
  targetDate.setDate(targetDate.getDate() + offsets[normalized]);

  const yyyy = targetDate.getFullYear();
  const mm = String(targetDate.getMonth() + 1).padStart(2, "0");
  const dd = String(targetDate.getDate()).padStart(2, "0");

  const dateStr = `${yyyy}-${mm}-${dd}`;
  return {
    gte: `${dateStr}T00:00:00`,
    lte: `${dateStr}T23:59:59`,
  };
}

function buildWhereClause(bounds) {
  const clauses = [];
  clauses.push("created_by = 'TIP.SA'");

  if (bounds) {
    clauses.push(`created>='${bounds.gte}' AND created<='${bounds.lte}'`);
  }

  return clauses.length > 0 ? clauses.join(" AND ") : "1=1";
}

async function fetchIncidentsFromDatasync({ day }) {
  const baseUrl = process.env.DATASYNC_SERVER_URL;
  const apiKey = process.env.DATASYNC_SERVER_API_KEY;

  if (!baseUrl || !apiKey) {
    throw new Error(
      "DATASYNC_SERVER_URL and DATASYNC_SERVER_API_KEY environment variables are required"
    );
  }

  const bounds = getDayBounds(day);

  const body = {
    fields: [
      "number",
      "created_by",
      "severity",
      "service_names",
      "created",
      "status",
      "tribe",
      "regions",
      "locations",
      "affected_ci_list",
      "caused_by_change_number",
    ],
    where: buildWhereClause(bounds),
    page: 1,
    page_size: 300,
  };

  const response = await fetch(`${baseUrl}/incidents/customQuery`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": apiKey,
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}));
    throw new Error(
      errorData.error ||
        `Failed to fetch incidents: ${response.status} ${response.statusText}`
    );
  }

  const data = await response.json();
  if (Array.isArray(data)) {
    return data;
  }
  
  if (typeof data === "object" && data !== null && "documents" in data) {
    return data.documents || [];
  }
  
  if (typeof data === "object" && data !== null) {
    if ("number" in data) {
      return [data];
    }
  }
  
  return [];
}

function parseJsonArrayField(value) {
  if (Array.isArray(value)) {
    return value;
  }

  if (typeof value === "string" && value.trim().length > 0) {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [parsed];
    } catch (error) {
      return [value];
    }
  }

  return [];
}

function mapIncidentToResponse(incident) {
  const serviceNames = parseJsonArrayField(incident.service_names);
  const regions = parseJsonArrayField(incident.regions);
  const locations = parseJsonArrayField(incident.locations);
  const affectedCiList = parseJsonArrayField(incident.affected_ci_list);

  return {
    number: incident.number,
    created_by: incident.created_by,
    severity: incident.severity,
    service_names: serviceNames,
    created: incident.created,
    status: incident.status,
    tribe: incident.tribe,
    regions,
    locations,
    affected_ci_list: affectedCiList,
    caused_by_change_number: incident.caused_by_change_number,
  };
}

async function fetchAskIaasTipsReports({ day } = {}) {
  try {
    // Default to 'today' if day is not provided or invalid
    const normalizedDay = day === "yesterday" || day === "today" ? day : "today";
    
    console.log("Selected day for incident tips:", normalizedDay);

    const incidents = await fetchIncidentsFromDatasync({
      day: normalizedDay,
    });

    const documents = incidents.map((incident) => mapIncidentToResponse(incident));

    return {
      success: true,
      total: documents.length,
      documents,
    };
  } catch (error) {
    console.error("Error fetching Ask IaaS TIP reports:", error.message);
    throw error;
  }
}

module.exports = {
  fetchAskIaasTipsReports,
};


