const { client } = require("../../databaseSetup/elasticsearchConfig");

async function createMonthlyServiceDoc() {
  let indexName = process.env.OUTAGE_ANALYSIS_INDEX || "change-risk-outages";
  const now = new Date();
  const year = now.getFullYear();
  const month = now.toLocaleString("default", { month: "short" });
  const docId = `${month}${year}`;

  const startOfMonth = new Date(year, now.getMonth(), 1).toISOString();

  const docBody = {
    services: {
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
      "ngdc-network-underlay": {},
    },
  };

  try {
    // Check if document already exists
    const exists = await client.exists({
      index: indexName,
      id: docId,
    });

    if (!exists) {
      await client.index({
        index: indexName,
        id: docId,
        document: docBody,
      });

      console.log(`✅ Document ${docId} created in index ${indexName}`);
    } else {
      console.log(`ℹ️ Document ${docId} already exists in index ${indexName}`);
    }
  } catch (error) {
    console.error(`❌ Error creating document:`, error);
  }
}

module.exports = createMonthlyServiceDoc;
