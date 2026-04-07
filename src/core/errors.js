class PortalScrapeError extends Error {
  constructor(portal, message, options = {}) {
    super(message, options);
    this.name = "PortalScrapeError";
    this.portal = portal;
    this.cause = options.cause;
  }
}

module.exports = {
  PortalScrapeError
};
