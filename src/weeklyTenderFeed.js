const fs = require("node:fs/promises");
const path = require("node:path");

const { chromium } = require("playwright");

const { createLogger } = require("./core/logger");
const { pickLocalizedText } = require("./core/normalize");
const { absolutizeUrl, createStableHash, ensureDirectoryPath, normalizeWhitespace, sleep, toArray, unique } = require("./core/utils");
const { buildTedQuery } = require("./sources/ted");

const CSV_HEADERS = [
  "portal",
  "suchbegriff",
  "titel",
  "auftraggeber",
  "frist",
  "link",
  "cpvCodes",
  "beschreibung",
  "veroeffentlichungsdatum",
  "organisationLand",
  "scrapedAt"
];

const COUNTRY_NAMES = {
  AUT: "Österreich",
  DEU: "Deutschland",
  CHE: "Schweiz",
  FRA: "Frankreich",
  ITA: "Italien",
  NLD: "Niederlande",
  BEL: "Belgien",
  ESP: "Spanien",
  POL: "Polen",
  CZE: "Tschechien",
  SVK: "Slowakei",
  SVN: "Slowenien",
  HUN: "Ungarn",
  HRV: "Kroatien",
  GBR: "Vereinigtes Königreich",
  IRL: "Irland"
};

function mergeConfig(baseConfig, overrideConfig = {}) {
  const merged = { ...baseConfig };

  for (const [key, value] of Object.entries(overrideConfig)) {
    if (
      value &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      merged[key] &&
      typeof merged[key] === "object" &&
      !Array.isArray(merged[key])
    ) {
      merged[key] = mergeConfig(merged[key], value);
      continue;
    }

    merged[key] = value;
  }

  return merged;
}

function parseArgs(argv) {
  const options = {
    configPath: "config/weekly-dashboard.json",
    days: null,
    sample: false,
    headless: null
  };

  for (const argument of argv) {
    if (argument.startsWith("--config=")) {
      options.configPath = argument.split("=")[1];
      continue;
    }

    if (argument.startsWith("--days=")) {
      options.days = Number(argument.split("=")[1]);
      continue;
    }

    if (argument === "--sample") {
      options.sample = true;
      continue;
    }

    if (argument === "--headful") {
      options.headless = false;
      continue;
    }

    if (argument === "--headless") {
      options.headless = true;
    }
  }

  return options;
}

function parseDate(value) {
  if (!value) {
    return null;
  }

  const text = normalizeWhitespace(value).replace(/Z$/, "");
  const dateOnly = text.match(/\d{4}-\d{2}-\d{2}/)?.[0];

  if (dateOnly) {
    return new Date(`${dateOnly}T00:00:00.000Z`);
  }

  const germanDate = text.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);
  if (germanDate) {
    const [, day, month, year] = germanDate;
    return new Date(`${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}T00:00:00.000Z`);
  }

  const parsed = new Date(text);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function toIsoDate(value) {
  const date = parseDate(value);
  return date ? date.toISOString().slice(0, 10) : "";
}

function formatTedDate(date) {
  return date.toISOString().slice(0, 10).replace(/-/g, "");
}

function calculateCutoffDate(maxAgeDays) {
  const cutoff = new Date();
  cutoff.setUTCHours(0, 0, 0, 0);
  cutoff.setUTCDate(cutoff.getUTCDate() - maxAgeDays);
  return cutoff;
}

function isOnOrAfter(value, cutoffDate) {
  const parsed = parseDate(value);
  return parsed ? parsed.getTime() >= cutoffDate.getTime() : false;
}

function countryLabel(codes) {
  return unique(toArray(codes).flat().map(normalizeWhitespace))
    .map((code) => COUNTRY_NAMES[code] || code)
    .join("; ");
}

function extractTedUrl(links) {
  if (!links || typeof links !== "object") {
    return "";
  }

  for (const bucket of ["html", "htmlDirect", "pdf", "xml"]) {
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
  return (
    pickLocalizedText(notice.deadline) ||
    normalizeWhitespace(
      [
        pickLocalizedText(notice["deadline-date-lot"]),
        pickLocalizedText(notice["deadline-time-lot"])
      ]
        .filter(Boolean)
        .join(" ")
    )
  );
}

function normalizeFeedRecord(record) {
  const normalized = {
    portal: normalizeWhitespace(record.portal),
    suchbegriff: normalizeWhitespace(record.suchbegriff),
    titel: normalizeWhitespace(record.titel),
    auftraggeber: normalizeWhitespace(record.auftraggeber),
    frist: normalizeWhitespace(record.frist),
    link: normalizeWhitespace(record.link),
    cpvCodes: unique(toArray(record.cpvCodes).flat().map(normalizeWhitespace)),
    beschreibung: normalizeWhitespace(record.beschreibung),
    veroeffentlichungsdatum: toIsoDate(record.veroeffentlichungsdatum),
    organisationLand: normalizeWhitespace(record.organisationLand),
    scrapedAt: record.scrapedAt || new Date().toISOString()
  };

  normalized._recordKey =
    normalized.link ||
    createStableHash(
      [
        normalized.portal,
        normalized.suchbegriff,
        normalized.titel,
        normalized.auftraggeber,
        normalized.veroeffentlichungsdatum
      ].join("|")
    );

  return normalized;
}

function buildTedSearchTerms(config) {
  const keywordTerms = config.searchTerms.map((value) => ({ type: "keyword", value }));

  if (!config.includeCpvSearches) {
    return keywordTerms;
  }

  return [
    ...config.cpvCodes.map((value) => ({ type: "cpv", value })),
    ...keywordTerms
  ];
}

function buildTedWeeklyQuery(searchTerm, cutoffDate) {
  const dateFilter = `publication-date = (${formatTedDate(cutoffDate)} <> ${formatTedDate(new Date())})`;
  return `${dateFilter} AND ${buildTedQuery(searchTerm)}`;
}

async function fetchTedPage(config, searchTerm, page, cutoffDate) {
  const maxRetries = config.runtime.tedMaxRetries || 3;

  for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
    const controller = new AbortController();
    const timeoutHandle = setTimeout(() => controller.abort(), config.runtime.requestTimeoutMs);

    try {
      const response = await fetch(config.ted.baseUrl, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          accept: "application/json"
        },
        body: JSON.stringify({
          query: buildTedWeeklyQuery(searchTerm, cutoffDate),
          fields: config.ted.fields,
          page,
          limit: config.runtime.pageSize,
          scope: config.ted.scope,
          paginationMode: "PAGE_NUMBER"
        }),
        signal: controller.signal
      });

      if (response.status === 429 && attempt < maxRetries) {
        const retryAfterSeconds = Number(response.headers.get("retry-after")) || attempt + 1;
        await sleep(retryAfterSeconds * 1500);
        continue;
      }

      if (!response.ok) {
        throw new Error(`TED API status ${response.status}`);
      }

      const payload = await response.json();
      return payload.notices || [];
    } finally {
      clearTimeout(timeoutHandle);
    }
  }

  return [];
}

async function scrapeTed(config, logger, cutoffDate) {
  const records = [];
  const seen = new Set();
  const searchTerms = buildTedSearchTerms(config);
  const maxRecordsPerSearchTerm = Math.max(
    1,
    Math.ceil(config.runtime.maxRecordsPerPortal / Math.max(1, searchTerms.length))
  );

  for (const searchTerm of searchTerms) {
    logger.info("Weekly TED Suche", { searchTerm: searchTerm.value, type: searchTerm.type });
    let recordsForSearchTerm = 0;

    for (let page = 1; page <= config.runtime.maxPagesPerSearch; page += 1) {
      const notices = await fetchTedPage(config, searchTerm, page, cutoffDate);

      if (notices.length === 0) {
        break;
      }

      for (const notice of notices) {
        if (!isOnOrAfter(notice["publication-date"], cutoffDate)) {
          continue;
        }

        const record = normalizeFeedRecord({
          portal: "TED",
          suchbegriff: searchTerm.value,
          titel: pickLocalizedText(notice["notice-title"]),
          auftraggeber: pickLocalizedText(notice["buyer-name"]),
          frist: extractTedDeadline(notice),
          link: extractTedUrl(notice.links),
          cpvCodes: notice["classification-cpv"],
          beschreibung:
            pickLocalizedText(notice["description-proc"]) ||
            pickLocalizedText(notice["description-lot"]),
          veroeffentlichungsdatum: notice["publication-date"],
          organisationLand: countryLabel(
            notice["buyer-country"] || notice["organisation-country-buyer"]
          )
        });

        if (seen.has(record._recordKey)) {
          continue;
        }

        seen.add(record._recordKey);
        records.push(record);
        recordsForSearchTerm += 1;

        if (
          records.length >= config.runtime.maxRecordsPerPortal ||
          recordsForSearchTerm >= maxRecordsPerSearchTerm
        ) {
          break;
        }
      }

      if (
        records.length >= config.runtime.maxRecordsPerPortal ||
        recordsForSearchTerm >= maxRecordsPerSearchTerm
      ) {
        break;
      }

      if (notices.length < config.runtime.pageSize) {
        break;
      }
    }

    await sleep(config.runtime.tedDelayMs || 500);

    if (records.length >= config.runtime.maxRecordsPerPortal) {
          return records;
      }
  }

  return records;
}

async function createBrowserContext(config) {
  const browser = await chromium.launch({
    headless: config.runtime.headless
  });
  const context = await browser.newContext({
    ignoreHTTPSErrors: true,
    locale: "de-AT",
    timezoneId: "Europe/Vienna",
    userAgent: config.runtime.userAgent,
    viewport: {
      width: 1440,
      height: 1200
    }
  });

  return { browser, context };
}

async function extractUspDetail(page, url, config) {
  if (!url) {
    return { cpvCodes: [], beschreibung: "", organisationLand: "Österreich" };
  }

  try {
    await page.goto(url, {
      waitUntil: "domcontentloaded",
      timeout: config.runtime.navigationTimeoutMs
    });

    return page.evaluate(() => {
      const normalize = (value) =>
        String(value || "")
          .replace(/\u00a0/g, " ")
          .replace(/\s+/g, " ")
          .trim();
      const bodyText = normalize(document.body.innerText);
      const cpvCodes = [...new Set(bodyText.match(/\b\d{8}(?:-\d)?\b/g) || [])];
      const descriptionLabels = [
        "Beschreibung",
        "Kurzbeschreibung",
        "Bezeichnung des Auftrags",
        "Auftragsbezeichnung",
        "Gegenstand"
      ];
      const description = descriptionLabels
        .map((label) => {
          const match = bodyText.match(new RegExp(`${label}[:\\s]+(.{20,700})`, "i"));
          return normalize(match?.[1]);
        })
        .find(Boolean);

      return {
        cpvCodes,
        beschreibung: description || "",
        organisationLand: bodyText.includes("Österreich") ? "Österreich" : "Österreich"
      };
    });
  } catch {
    return { cpvCodes: [], beschreibung: "", organisationLand: "Österreich" };
  }
}

async function scrapeUsp(config, logger, cutoffDate) {
  const { browser, context } = await createBrowserContext(config);
  const listPage = await context.newPage();
  const detailPage = await context.newPage();
  const records = [];
  const seen = new Set();
  let detailCount = 0;

  try {
    for (const searchTerm of config.searchTerms) {
      const searchUrl = `${config.usp.url}?q=${encodeURIComponent(searchTerm)}&loaded=true`;
      logger.info("Weekly USP Suche", { searchTerm, searchUrl });

      try {
        await listPage.goto(searchUrl, {
          waitUntil: "domcontentloaded",
          timeout: config.runtime.navigationTimeoutMs
        });
        await listPage.waitForSelector("tbody tr", { timeout: config.runtime.navigationTimeoutMs });
        await listPage.waitForFunction(
          () => Array.from(document.querySelectorAll("tbody tr")).some((row) => row.querySelectorAll("td").length >= 4),
          { timeout: config.runtime.navigationTimeoutMs }
        );
      } catch (error) {
        logger.warn("USP Suche übersprungen", { searchTerm, message: error.message });
        continue;
      }

      for (let pageNumber = 1; pageNumber <= config.runtime.maxPagesPerSearch; pageNumber += 1) {
        const rows = await listPage.$$eval(
          "tbody tr",
          (tableRows, baseUrl) => {
            const normalize = (value) =>
              String(value || "")
                .replace(/\u00a0/g, " ")
                .replace(/\s+/g, " ")
                .trim();

            return tableRows
              .map((row) => {
                const cells = Array.from(row.querySelectorAll("td"));
                const link = cells[0]?.querySelector("a[href]");
                const href = link?.getAttribute("href") || "";

                return {
                  titel: normalize(link?.textContent || cells[0]?.textContent),
                  auftraggeber: normalize(cells[1]?.textContent),
                  veroeffentlichungsdatum: normalize(cells[2]?.textContent),
                  frist: normalize(cells[3]?.textContent),
                  link: href ? new URL(href, baseUrl).href : "",
                  beschreibung: ""
                };
              })
              .filter((row) => row.titel);
          },
          config.usp.detailBaseUrl
        );

        for (const row of rows) {
          if (!isOnOrAfter(row.veroeffentlichungsdatum, cutoffDate)) {
            continue;
          }

          let detail = { cpvCodes: [], beschreibung: "", organisationLand: "Österreich" };

          if (detailCount < config.runtime.maxDetailsPerPortal) {
            detail = await extractUspDetail(detailPage, row.link, config);
            detailCount += 1;
          }

          const record = normalizeFeedRecord({
            ...row,
            portal: "USP Bund",
            suchbegriff: searchTerm,
            cpvCodes: detail.cpvCodes,
            beschreibung: detail.beschreibung || row.beschreibung,
            organisationLand: detail.organisationLand
          });

          if (seen.has(record._recordKey)) {
            continue;
          }

          seen.add(record._recordKey);
          records.push(record);

          if (records.length >= config.runtime.maxRecordsPerPortal) {
            return records;
          }
        }

        const nextButton = await listPage.$(
          "#tenderlist_next:not(.disabled), a[aria-label='Next']:not(.disabled), .paginate_button.next:not(.disabled)"
        );

        if (!nextButton) {
          break;
        }

        await nextButton.click();
        await listPage.waitForTimeout(1000);
      }
    }
  } finally {
    await detailPage.close();
    await listPage.close();
    await context.close();
    await browser.close();
  }

  return records;
}

async function ensureParentDirectory(filePath) {
  await fs.mkdir(ensureDirectoryPath(filePath), { recursive: true });
}

function escapeCsvValue(value) {
  const text = Array.isArray(value) ? value.join("; ") : String(value ?? "");
  return `"${text.replace(/"/g, '""')}"`;
}

function toCsv(records) {
  const lines = records.map((record) =>
    CSV_HEADERS.map((header) => escapeCsvValue(record[header])).join(",")
  );

  return [CSV_HEADERS.join(","), ...lines].join("\n");
}

async function writeOutputs(records, output) {
  const publicRecords = records.map(({ _recordKey, ...record }) => record);

  for (const filePath of [output.csvPath, output.jsonPath, output.dataJsPath]) {
    await ensureParentDirectory(filePath);
  }

  await fs.writeFile(output.csvPath, `\uFEFF${toCsv(publicRecords)}\n`, "utf8");
  await fs.writeFile(output.jsonPath, JSON.stringify(publicRecords, null, 2), "utf8");
  await fs.writeFile(
    output.dataJsPath,
    `const DATA = ${JSON.stringify(publicRecords, null, 2)};\n`,
    "utf8"
  );
}

async function loadWeeklyConfig(argv) {
  const options = parseArgs(argv);
  const defaultConfigPath = path.resolve(process.cwd(), "config", "default.json");
  const weeklyConfigPath = path.resolve(process.cwd(), options.configPath);
  const defaultConfig = JSON.parse(await fs.readFile(defaultConfigPath, "utf8"));
  const weeklyConfig = JSON.parse(await fs.readFile(weeklyConfigPath, "utf8"));
  const config = mergeConfig(weeklyConfig, {});

  config.cpvCodes = defaultConfig.cpvCodes || [];
  config.searchTerms = unique([
    ...(config.searchTerms || []),
    ...(defaultConfig.keywords || [])
  ]);

  if (options.days) {
    config.maxAgeDays = options.days;
  }

  if (options.sample) {
    config.runtime.maxPagesPerSearch = 1;
    config.runtime.maxRecordsPerPortal = 10;
    config.runtime.maxDetailsPerPortal = 5;
    config.searchTerms = config.searchTerms.slice(0, 3);
    config.cpvCodes = config.cpvCodes.slice(0, 2);
  }

  if (typeof options.headless === "boolean") {
    config.runtime.headless = options.headless;
  }

  config.output = {
    csvPath: path.resolve(process.cwd(), config.output.csvPath),
    jsonPath: path.resolve(process.cwd(), config.output.jsonPath),
    dataJsPath: path.resolve(process.cwd(), config.output.dataJsPath)
  };

  return config;
}

async function main() {
  const logger = createLogger();
  const config = await loadWeeklyConfig(process.argv.slice(2));
  const cutoffDate = calculateCutoffDate(config.maxAgeDays);

  logger.info("Weekly Tender Feed startet", {
    cutoffDate: cutoffDate.toISOString().slice(0, 10),
    searchTerms: config.searchTerms,
    includeCpvSearches: config.includeCpvSearches
  });

  const tedRecords = await scrapeTed(config, logger, cutoffDate);
  const uspRecords = await scrapeUsp(config, logger, cutoffDate);
  const records = [...tedRecords, ...uspRecords]
    .sort((a, b) => b.veroeffentlichungsdatum.localeCompare(a.veroeffentlichungsdatum));

  await writeOutputs(records, config.output);

  logger.info("Weekly Tender Feed abgeschlossen", {
    records: records.length,
    ted: tedRecords.length,
    usp: uspRecords.length,
    csvPath: config.output.csvPath,
    jsonPath: config.output.jsonPath,
    dataJsPath: config.output.dataJsPath
  });
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}

module.exports = {
  calculateCutoffDate,
  countryLabel,
  normalizeFeedRecord,
  parseDate,
  toIsoDate
};
