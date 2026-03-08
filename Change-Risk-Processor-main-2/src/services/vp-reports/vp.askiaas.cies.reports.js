const fetch = require("node-fetch");
const {
  normalizeEmail,
  hasFullServiceAccess,
  getServicesForOwner,
} = require("./utils/access");

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

function escapeForQuery(value = "") {
  return value.replace(/'/g, "\\'");
}

function buildServiceFilterClause(ownerServices = []) {
  if (!Array.isArray(ownerServices) || ownerServices.length === 0) {
    return "";
  }

  const clauses = ownerServices.map(
    (service) => `service_names LIKE '%${escapeForQuery(service)}%'`
  );

  return clauses.length > 0 ? ` AND (${clauses.join(" OR ")})` : "";
}

function getSeverityClause(email) {
  if (hasFullServiceAccess(email)) {
    return "(severity=1 OR severity=2 OR severity=3 OR severity=4 OR severity=5)";
  }
  return "(severity=1 OR severity=2 OR severity=3 OR severity=4 OR severity=5)";
}

function buildWhereClause(dayBounds, ownerServices, email) {
  const clauses = [];

  clauses.push(getSeverityClause(email));
  clauses.push("status='confirmed_cie'")

  if (dayBounds) {
    clauses.push(`created>='${dayBounds.gte}' AND created<='${dayBounds.lte}'`);
  }

  const serviceClause = buildServiceFilterClause(ownerServices);
  if (serviceClause) {
    clauses.push(serviceClause.replace(/^ AND /, ""));
  }

  return clauses.length > 0 ? clauses.join(" AND ") : "1=1";
}

async function fetchIncidentsFromDatasync({ platform, email, day, ownerServices }) {
  const baseUrl = process.env.DATASYNC_SERVER_URL;
  const apiKey = process.env.DATASYNC_SERVER_API_KEY;

  if (!baseUrl || !apiKey) {
    throw new Error(
      "DATASYNC_SERVER_URL and DATASYNC_SERVER_API_KEY environment variables are required"
    );
  }

  const bounds = getDayBounds(day);
  const normalizedOwnerServices =
    Array.isArray(ownerServices) && ownerServices.length > 0
      ? ownerServices
      : getServicesForOwner(platform, email);

  const body = {
    fields: [
      "number",
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
    where: buildWhereClause(bounds, normalizedOwnerServices, email),
  };

  const response = await fetch(
    `${baseUrl}/incidents/customQuery`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": apiKey,
      },
      body: JSON.stringify(body),
    }
  );

  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}));
    throw new Error(
      errorData.error ||
        `Failed to fetch incidents: ${response.status} ${response.statusText}`
    );
  }

  const data = await response.json();
  return  data || [];
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

  const response = {
    incident_number: incident.number,
    caused_by_change_number: incident.caused_by_change_number,
    service_names: serviceNames,
    severity: incident.severity,
    status: incident.status,
    tribe: incident.tribe,
    regions,
    locations,
    affected_ci_list: affectedCiList,
  };

  if (incident.service_tribe) {
    response.service_tribe = incident.service_tribe;
  }

  if (incident.region) {
    response.region = incident.region;
  }

  return response;
}

async function fetchAskIaasCiesReports({
  platform,
  email,
  day,
} = {}) {
  try {
    const ownerServices = getServicesForOwner(platform, email);
    const incidents = await fetchIncidentsFromDatasync({
      platform,
      email,
      day,
      ownerServices,
    });

    let filteredIncidents =
      ownerServices.length > 0
        ? incidents.filter((incident) => {
            let serviceNames = [];
            if (Array.isArray(incident.service_names)) {
              serviceNames = incident.service_names;
            } else if (typeof incident.service_names === "string") {
              try {
                serviceNames = JSON.parse(incident.service_names);
              } catch (error) {
                serviceNames = [incident.service_names];
              }
            }

            return serviceNames.some((service) =>
              ownerServices.includes(service)
            );
          })
        : incidents;

    const documents = filteredIncidents.map((incident) =>
      mapIncidentToResponse(incident)
    );

    return {
      success: true,
      total: documents.length,
      documents,
    };
  } catch (error) {
    console.error("Error fetching Ask IaaS CIE reports:", error.message);
    throw error;
  }
}

module.exports = {
  fetchAskIaasCiesReports,
};

