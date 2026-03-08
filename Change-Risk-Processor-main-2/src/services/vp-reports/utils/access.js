const { vpMapping } = require("../../../../constants/constants");

const FULL_SERVICE_ACCESS_EMAILS = new Set(["girishdhanak@in.ibm.com"]);

function normalizeEmail(email) {
  return typeof email === "string" ? email.trim().toLowerCase() : "";
}

function hasFullServiceAccess(email) {
  const normalizedEmail = normalizeEmail(email);
  return normalizedEmail && FULL_SERVICE_ACCESS_EMAILS.has(normalizedEmail);
}

let cachedServices = null;

function getAllMappedServices() {
  if (cachedServices) {
    return cachedServices;
  }

  const serviceSet = new Set();
  vpMapping.forEach((item) => {
    if (Array.isArray(item.services)) {
      item.services.forEach((service) => serviceSet.add(service));
    }
  });

  cachedServices = Array.from(serviceSet);
  return cachedServices;
}

function getServicesForOwner(ownerPlatform, ownerEmail) {
  if (hasFullServiceAccess(ownerEmail)) {
    return getAllMappedServices();
  }

  const normalizedEmail = normalizeEmail(ownerEmail);
  let ownerData = null;

  if (normalizedEmail) {
    ownerData = vpMapping.find((item) => {
      if (item.email) {
        const itemEmail = normalizeEmail(item.email);
        return itemEmail === normalizedEmail;
      }
      return false;
    });
  }

  if (!ownerData && ownerPlatform) {
    ownerData = vpMapping.find((item) => item.platform === ownerPlatform);
  }

  return ownerData ? ownerData.services : [];
}

module.exports = {
  FULL_SERVICE_ACCESS_EMAILS,
  normalizeEmail,
  hasFullServiceAccess,
  getAllMappedServices,
  getServicesForOwner,
};

