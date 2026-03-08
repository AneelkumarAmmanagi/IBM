const { client } = require("../../databaseSetup/elasticsearchConfig");
const _ = require("lodash");

const now = new Date();


function defaultServiceStructure() {
  return {
    "is-management-rack": {},
    "is-fleet": {},
    "iaas-network": {},
    "is-vault": {},
    "power-iaas": {},
    kms: {},
    "secrets-manager": {},
    "block-storage": {},
    "ims-fabric": {},
    "cloud-object-storage": {},
    "is-razee-deploy": {},
    "is-compute": {},
    compliance: {},
    "is-vpc": {},
    "storage-platform-acadia": {},
    "iaas-compute-general": {},
    "is-volume": {},
    "ngdc-network-ncs": {},
    "is-platform-integration": {},
    "is-bare-metal-server": {},
    "is-kube": {},
    "iaas-maintenance": {},
    "is-sdn": {},
    "carbon-calculator": {},
    "dns-svcs": {},
    "file-storage": {},
    "iaas-fabric-classic": {},
    "is-hostos": {},
    "is-rias-ingress": {},
    "is-continuous-deploy": {},
    "is-genctl": {},
    "is-genctl-etcd": {},
    "is-rias-deploy": {},
    "is-share": {},
    "is-snapshot": {},
    "is-volume-acadia": {},
    "ngdc-network-underlay": {}
  };
}

async function updateGroupedServiceDocument(groupedData) {
  let indexName = process.env.OUTAGE_ANALYSIS_INDEX || "change-risk-outages";
  const now = new Date();
  const year = now.getFullYear();
  const month = now.toLocaleString("default", { month: "short" });
  const docId = `${month}${year}`;

  let existingDoc;

  try {
    const res = await client.get({
      index: indexName,
      id: docId,
    });
    existingDoc = res._source;
  } catch (err) {
    if (err.meta?.statusCode === 404) {
      console.warn(`📄 Document ${docId} not found. Creating a new one.`);
      existingDoc = {
        services: defaultServiceStructure(),
      };
    } else {
      console.error("❌ Error fetching document:", err);
      throw err;
    }
  }

  const mergedServices = {
    ...defaultServiceStructure(),
    ...existingDoc.services,
  };

  for (const [serviceName, newData] of Object.entries(groupedData)) {
    if (
      !mergedServices[serviceName] ||
      typeof mergedServices[serviceName] !== "object"
    ) {
      mergedServices[serviceName] = {};
    }

    const existing = mergedServices[serviceName];
    const newOutageDetails = newData.outage_details || {};
    const existingOutageDetails = existing.outage_details || {};

    let additionalOutageDuration = 0;

    for (const [changeNumber, detail] of Object.entries(newOutageDetails)) {
      const existingDetail = existingOutageDetails[changeNumber];

      if (!existingDetail) {
        existingOutageDetails[changeNumber] = detail;
        additionalOutageDuration += newData.outage_duration || 0;
      } else {
        const incidentSet = new Set(existingDetail.incident_numbers || []);
        const newIncidents = (detail.incident_numbers || []).filter(
          (inc) => !incidentSet.has(inc)
        );

        if (newIncidents.length > 0) {
          additionalOutageDuration += newData.outage_duration || 0;
        }

        existingOutageDetails[changeNumber] = {
          time_stamp:
            new Date(detail.time_stamp) > new Date(existingDetail.time_stamp)
              ? detail.time_stamp
              : existingDetail.time_stamp,
          incident_numbers: _.union(
            existingDetail.incident_numbers || [],
            detail.incident_numbers || []
          ),
        };
      }
    }

    mergedServices[serviceName] = {
      outage_duration:
        (existing.outage_duration || 0) + additionalOutageDuration,
      outage_details: existingOutageDetails,
    };
  }

  try {
    await client.update({
      index: indexName,
      id: docId,
      doc: {
        services: mergedServices,
      },
      doc_as_upsert: true,
    });

    console.log(`✅ Successfully updated document: ${docId}`);
  } catch (err) {
    console.error("❌ Failed to update document:", err);
  }
}
  

module.exports = updateGroupedServiceDocument;
