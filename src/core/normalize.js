const { createStableHash, normalizeWhitespace, toArray, unique } = require("./utils");

const PREFERRED_LANGUAGES = ["deu", "de", "eng", "en", "nld", "fra"];

/**
 * TED liefert Mehrsprachfelder als Objekt zurück.
 * Diese Funktion bevorzugt deutsche Inhalte, fällt aber sauber auf
 * andere verfügbare Sprachwerte zurück.
 */
function pickLocalizedText(value) {
  if (!value) {
    return "";
  }

  if (typeof value === "string") {
    return normalizeWhitespace(value);
  }

  if (Array.isArray(value)) {
    return normalizeWhitespace(value.map(pickLocalizedText).find(Boolean) || "");
  }

  if (typeof value === "object") {
    for (const language of PREFERRED_LANGUAGES) {
      if (value[language]) {
        return pickLocalizedText(value[language]);
      }
    }

    return pickLocalizedText(Object.values(value).find(Boolean));
  }

  return normalizeWhitespace(String(value));
}

function normalizeCpvCode(code) {
  const cleaned = normalizeWhitespace(code).replace(/[^\d-]/g, "");
  const digitsOnly = cleaned.replace(/-/g, "");

  if (/^\d{8}-\d$/.test(cleaned)) {
    return cleaned;
  }

  if (/^\d{8}$/.test(cleaned)) {
    return cleaned;
  }

  if (/^\d{9}$/.test(digitsOnly)) {
    return `${digitsOnly.slice(0, 8)}-${digitsOnly.slice(8)}`;
  }

  return cleaned;
}

function stripCpvCheckDigit(code) {
  return normalizeCpvCode(code).split("-")[0];
}

function buildRecordKey(record) {
  if (record.url) {
    return record.url;
  }

  return createStableHash(
    [record.portal, record.title, record.organization, record.deadline].join("|")
  );
}

function normalizeRecord(record) {
  const normalized = {
    portal: normalizeWhitespace(record.portal),
    title: normalizeWhitespace(record.title),
    content: normalizeWhitespace(record.content),
    organization: normalizeWhitespace(record.organization),
    deadline: normalizeWhitespace(record.deadline),
    url: normalizeWhitespace(record.url),
    cpvCodes: unique(toArray(record.cpvCodes).map(normalizeCpvCode)),
    matchReasons: unique(toArray(record.matchReasons).map(normalizeWhitespace)),
    scrapedAt: record.scrapedAt || new Date().toISOString()
  };

  normalized._recordKey = buildRecordKey(normalized);
  return normalized;
}

module.exports = {
  buildRecordKey,
  normalizeCpvCode,
  normalizeRecord,
  pickLocalizedText,
  stripCpvCheckDigit
};
