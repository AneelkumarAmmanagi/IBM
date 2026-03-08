const fetch = require('node-fetch');

async function fetchAndGroupCieIncidents() {
  const groupedByCbcNumber = {};
  try {
    const body = {
      fields: [
        "number",
        "severity",
        "service_names",
        "created",
        "tribe",
        "regions",
        "locations",
        "affected_ci_list"
      ],
      where: "(status='confirmed_cie' OR status='potential_cie') AND state!='closed' AND state!='resolved' AND (severity=1 OR severity=2)"
    };

    const fetchRelatedIncidentsResponse = await fetch(
      `${process.env.DATASYNC_SERVER_URL}/incidents/customQuery`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-api-key": `${process.env.DATASYNC_SERVER_API_KEY}`,
        },
        body: JSON.stringify(body)
      }
    );

    if (!fetchRelatedIncidentsResponse.ok) {
      const errorData = await fetchRelatedIncidentsResponse.json();
      throw new Error(
        errorData.error ||
          `Request failed with status ${fetchRelatedIncidentsResponse.status}`
      );
    }

    const data = await fetchRelatedIncidentsResponse.json();
    const incidents = data || [];

    return incidents;
  } catch (err) {
    console.log("Err from grouped incidents", err?.message);
    throw err;
  }
}

module.exports = { fetchAndGroupCieIncidents }; 