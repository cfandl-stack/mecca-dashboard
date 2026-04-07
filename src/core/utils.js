const crypto = require("node:crypto");
const path = require("node:path");

function normalizeWhitespace(value) {
  return String(value || "")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function stripHtmlTags(value) {
  return normalizeWhitespace(String(value || "").replace(/<[^>]+>/g, " "));
}

function toArray(value) {
  if (Array.isArray(value)) {
    return value;
  }

  if (value === undefined || value === null || value === "") {
    return [];
  }

  return [value];
}

function unique(values) {
  return [...new Set(values.filter(Boolean))];
}

function createStableHash(input) {
  return crypto.createHash("sha1").update(String(input)).digest("hex");
}

function slugify(value) {
  return normalizeWhitespace(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function randomBetween(minimum, maximum) {
  const min = Math.min(minimum, maximum);
  const max = Math.max(minimum, maximum);
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

async function sleep(milliseconds) {
  await new Promise((resolve) => setTimeout(resolve, milliseconds));
}

/**
 * Kapselt die zufälligen Pausen, damit alle Quellen dieselbe Logik verwenden.
 */
async function randomPause(runtimeConfig, logger, reason) {
  const milliseconds = randomBetween(
    runtimeConfig.minDelayMs,
    runtimeConfig.maxDelayMs
  );

  if (logger) {
    logger.info(`Zufällige Pause: ${milliseconds} ms (${reason})`);
  }

  await sleep(milliseconds);
}

function absolutizeUrl(baseUrl, href) {
  if (!href) {
    return "";
  }

  try {
    return new URL(href, baseUrl).href;
  } catch {
    return href;
  }
}

function ensureDirectoryPath(filePath) {
  return path.dirname(filePath);
}

module.exports = {
  absolutizeUrl,
  createStableHash,
  ensureDirectoryPath,
  normalizeWhitespace,
  randomBetween,
  randomPause,
  slugify,
  sleep,
  stripHtmlTags,
  toArray,
  unique
};
