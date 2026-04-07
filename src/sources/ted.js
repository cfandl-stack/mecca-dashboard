const { PortalScrapeError } = require("../core/errors");
const { normalizeRecord, pickLocalizedText } = require("../core/normalize");
const { normalizeWhitespace, randomPause, toArray } = require("../core/utils");

function buildTedQuery(searchTerm) {
  if (searchTerm.type === "cpv") {
    return `classification-cpv = ${searchTerm.value.replace(/[^\d]/g, "").slice(0, 8)}`;
  }

  const escapedKeyword = String(searchTerm.value).replace(/\\/g, "\\\\").replace(/"/g, '\\"');
  return `FT ~ "${escapedKeyword}"`;
}

function createTedRequestBody(config, searchTerm, page) {
  return {
    query: buildTedQuery(searchTerm),
    fields: config.ted.fields,
    page,
    limit: config.ted.pageSize,
    scope: config.ted.scope,
    paginationMode: "PAGE_NUMBER"
  };
}

function extractTedUrl(links) {
  if (!links || typeof links !== "object") {
    return "";
  }

  const buckets = ["html", "htmlDirect", "pdf", "xml"];

  for (const bucket of buckets) {
    const values = links[bucket];

    if (!values || typeof values !== "object") {
      continue;
    }

    if (values.DEU) {
      return values.DEU;
    }

    if (values.ENG) {
      return values.ENG;
    }

    const firstValue = Object.values(values).find(Boolean);
    if (firstValue) {
      return firstValue;
    }
  }

  return "";
}

function extractTedDeadline(notice) {
  const directDeadline = pickLocalizedText(notice.deadline);
  if (directDeadline) {
    return directDeadline;
  }

  const datePart = pickLocalizedText(notice["deadline-date-lot"]);
  const timePart = pickLocalizedText(notice["deadline-time-lot"]);

  return normalizeWhitespace([datePart, timePart].filter(Boolean).join(" "));
}

function mapTedNoticeToRecord(notice) {
  return normalizeRecord({
    portal: "TED",
    title: pickLocalizedText(notice["notice-title"]),
    content:
      pickLocalizedText(notice["description-proc"]) ||
      pickLocalizedText(notice["description-lot"]),
    organization: pickLocalizedText(notice["buyer-name"]),
    deadline: extractTedDeadline(notice),
    url: extractTedUrl(notice.links),
    cpvCodes: toArray(notice["classification-cpv"]),
    matchReasons: []
  });
}

async function fetchTedPage(config, searchTerm, page, logger) {
  const controller = new AbortController();
  const timeoutHandle = setTimeout(
    () => controller.abort(),
    config.runtime.requestTimeoutMs
  );

  try {
    const response = await fetch(config.ted.baseUrl, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        accept: "application/json"
      },
      body: JSON.stringify(createTedRequestBody(config, searchTerm, page)),
      signal: controller.signal
    });

    if (!response.ok) {
      throw new PortalScrapeError("TED", `TED API antwortete mit Status ${response.status}`);
    }

    const payload = await response.json();
    return payload.notices || [];
  } catch (error) {
    logger.warn("TED-Abfrage fehlgeschlagen", {
      term: searchTerm.value,
      page,
      message: error.message
    });

    throw new PortalScrapeError("TED", "TED-Suche fehlgeschlagen", { cause: error });
  } finally {
    clearTimeout(timeoutHandle);
  }
}

async function scrapeTed({ config, logger }) {
  const records = [];
  const seenKeys = new Set();
  const searchTerms = [
    ...config.cpvCodes.map((value) => ({ type: "cpv", value })),
    ...config.keywords.map((value) => ({ type: "keyword", value }))
  ];

  for (const searchTerm of searchTerms) {
    logger.info("TED-Suche gestartet", searchTerm);

    for (let page = 1; page <= config.runtime.maxPagesPerSearch; page += 1) {
      let notices = [];

      try {
        notices = await fetchTedPage(config, searchTerm, page, logger);
      } catch (error) {
        logger.warn("TED-Suchseite wird übersprungen", {
          term: searchTerm.value,
          page,
          message: error.message
        });
        break;
      }

      if (notices.length === 0) {
        break;
      }

      for (const notice of notices) {
        const record = mapTedNoticeToRecord(notice);

        if (seenKeys.has(record._recordKey)) {
          continue;
        }

        seenKeys.add(record._recordKey);
        records.push(record);
      }

      if (notices.length < config.ted.pageSize) {
        break;
      }

      await randomPause(config.runtime, logger, `TED Pagination nach Seite ${page}`);
    }

    await randomPause(config.runtime, logger, `TED Suche abgeschlossen für ${searchTerm.value}`);
  }

  return records.slice(0, config.runtime.maxRecordsPerPortal);
}

module.exports = {
  buildTedQuery,
  createTedRequestBody,
  scrapeTed
};
