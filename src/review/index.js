const { normalizeWhitespace } = require("../core/utils");
const { createMinimaxReview, isMinimaxConfigured } = require("./providers/minimax");

const REVIEW_LABELS = new Set(["passt gut", "pruefen", "eher unpassend", "ungeprueft"]);

function getReviewRuntimeConfig() {
  return {
    enabled: String(process.env.LLM_ENABLED || "true").trim().toLowerCase() !== "false",
    provider: String(process.env.LLM_PROVIDER || "minimax").trim().toLowerCase(),
    timeoutMs: Number(process.env.LLM_TIMEOUT_MS || 30000),
    maxRecords: Number(process.env.LLM_MAX_RECORDS || 0)
  };
}

function summarizeRecord(record) {
  return {
    portal: record.portal,
    suchbegriff: record.suchbegriff,
    titel: record.titel,
    auftraggeber: record.auftraggeber,
    beschreibung: record.beschreibung,
    cpvCodes: record.cpvCodes,
    organisationLand: record.organisationLand,
    frist: record.frist,
    veroeffentlichungsdatum: record.veroeffentlichungsdatum
  };
}

function buildPrompt(record) {
  const summarized = summarizeRecord(record);

  return [
    "Bewerte die Ausschreibung fuer ein oesterreichisches Raumplanungsbuero.",
    "Das Buero arbeitet in Raumplanung, Orts- und Stadtentwicklung, Regionalentwicklung, Strategie, Machbarkeitsstudien, Evaluation, Moderation, Interreg-Programmen und Projektmanagement ausserhalb von Hoch- und Tiefbau.",
    "Typisch passend sind Planungs-, Strategie-, Beteiligungs-, Studien-, Forschungs-, Standortentwicklungs- und Beratungsleistungen im oeffentlichen oder regionalen Kontext.",
    "Typisch eher unpassend sind Hochbau, Tiefbau, Strassen- oder Schienenbau, Bauausfuehrung, TGA/Haustechnik, OeBA, reine Lieferleistungen, IT-only-Projekte, Werbeagenturleistungen, Reinigung oder medizintechnische Beschaffung.",
    "Waehle genau ein Label: passt gut | pruefen | eher unpassend.",
    "Verwende pruefen nur bei gemischten Signalen oder unklarer Relevanz.",
    'Antworte ausschliesslich als JSON mit diesem Schema: {"label":"passt gut|pruefen|eher unpassend","score":0-100,"reason":"kurzer deutscher Satz, maximal 160 Zeichen"}',
    `Datensatz: ${JSON.stringify(summarized)}`
  ].join("\n");
}

function extractJsonObject(value) {
  const text = String(value || "").trim();

  if (!text) {
    throw new Error("Leere LLM-Antwort");
  }

  const fencedMatch = text.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fencedMatch) {
    return JSON.parse(fencedMatch[1]);
  }

  const objectMatch = text.match(/\{[\s\S]*\}/);
  if (objectMatch) {
    return JSON.parse(objectMatch[0]);
  }

  return JSON.parse(text);
}

function normalizeReviewPayload(payload, provider, model) {
  const label = normalizeWhitespace(payload?.label).toLowerCase();
  const normalizedLabel = REVIEW_LABELS.has(label) ? label : "ungeprueft";
  const score = Number(payload?.score);
  const normalizedScore = Number.isFinite(score)
    ? Math.max(0, Math.min(100, Math.round(score)))
    : normalizedLabel === "passt gut"
      ? 85
      : normalizedLabel === "pruefen"
        ? 55
        : normalizedLabel === "eher unpassend"
          ? 20
          : null;

  return {
    reviewLabel: normalizedLabel,
    reviewScore: normalizedScore,
    reviewReason: normalizeWhitespace(payload?.reason).slice(0, 160),
    reviewProvider: provider,
    reviewModel: model || "",
    reviewedAt: new Date().toISOString()
  };
}

async function requestReview(record, logger) {
  const runtimeConfig = getReviewRuntimeConfig();

  if (!runtimeConfig.enabled) {
    return {
      reviewLabel: "ungeprueft",
      reviewScore: null,
      reviewReason: "",
      reviewProvider: "",
      reviewModel: "",
      reviewedAt: ""
    };
  }

  if (runtimeConfig.provider === "minimax") {
    if (!isMinimaxConfigured()) {
      logger?.info("LLM Review uebersprungen", {
        reason: "MiniMax nicht konfiguriert"
      });

      return {
        reviewLabel: "ungeprueft",
        reviewScore: null,
        reviewReason: "",
        reviewProvider: "",
        reviewModel: "",
        reviewedAt: ""
      };
    }

    const controller = new AbortController();
    const timeoutHandle = setTimeout(() => controller.abort(), runtimeConfig.timeoutMs);

    try {
      const prompt = buildPrompt(record);
      const review = await createMinimaxReview(prompt, { signal: controller.signal });
      const parsed = extractJsonObject(review.rawContent);
      return normalizeReviewPayload(parsed, review.provider, review.model);
    } finally {
      clearTimeout(timeoutHandle);
    }
  }

  throw new Error(`Unbekannter LLM Provider: ${runtimeConfig.provider}`);
}

async function enrichRecordsWithReview(records, logger) {
  const runtimeConfig = getReviewRuntimeConfig();

  if (!runtimeConfig.enabled) {
    return records.map((record) => ({
      ...record,
      reviewLabel: record.reviewLabel || "ungeprueft",
      reviewScore: record.reviewScore ?? null,
      reviewReason: record.reviewReason || "",
      reviewProvider: record.reviewProvider || "",
      reviewModel: record.reviewModel || "",
      reviewedAt: record.reviewedAt || ""
    }));
  }

  if (runtimeConfig.provider === "minimax" && !isMinimaxConfigured()) {
    logger?.info("LLM Review global deaktiviert", {
      reason: "MiniMax Konfiguration fehlt"
    });

    return records.map((record) => ({
      ...record,
      reviewLabel: "ungeprueft",
      reviewScore: null,
      reviewReason: "",
      reviewProvider: "",
      reviewModel: "",
      reviewedAt: ""
    }));
  }

  const maxRecords = runtimeConfig.maxRecords > 0 ? runtimeConfig.maxRecords : records.length;
  const reviewed = [];

  for (const [index, record] of records.entries()) {
    if (index >= maxRecords) {
      reviewed.push({
        ...record,
        reviewLabel: "ungeprueft",
        reviewScore: null,
        reviewReason: "",
        reviewProvider: "",
        reviewModel: "",
        reviewedAt: ""
      });
      continue;
    }

    try {
      const review = await requestReview(record, logger);
      reviewed.push({
        ...record,
        ...review
      });
    } catch (error) {
      logger?.warn("LLM Review fehlgeschlagen", {
        recordKey: record._recordKey || record.recordKey || record.link || record.titel,
        message: error.message
      });

      reviewed.push({
        ...record,
        reviewLabel: "ungeprueft",
        reviewScore: null,
        reviewReason: "",
        reviewProvider: "",
        reviewModel: "",
        reviewedAt: ""
      });
    }
  }

  return reviewed;
}

module.exports = {
  enrichRecordsWithReview,
  extractJsonObject,
  getReviewRuntimeConfig,
  normalizeReviewPayload
};
