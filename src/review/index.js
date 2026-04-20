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
    "WICHTIG: Gib ausschliesslich ein JSON-Objekt zurueck. Kein Markdown. Keine Erklaerung. Kein Vorspann. Kein Nachspann.",
    'Antwortformat exakt: {"label":"passt gut|pruefen|eher unpassend","score":0-100,"reason":"kurzer deutscher Satz, maximal 160 Zeichen"}',
    'Beispiel: {"label":"pruefen","score":54,"reason":"Projektmanagement ist enthalten, aber der Bau- und Infrastrukturanteil wirkt fuer das Buero zu stark."}',
    `Datensatz: ${JSON.stringify(summarized)}`
  ].join("\n");
}

function stripThinkingArtifacts(value) {
  return String(value || "")
    .replace(/<think>[\s\S]*?<\/think>/gi, " ")
    .replace(/```json/gi, "```")
    .trim();
}

function extractFirstJsonObject(text) {
  const start = text.indexOf("{");
  if (start < 0) {
    return "";
  }

  let depth = 0;
  let inString = false;
  let escaped = false;

  for (let index = start; index < text.length; index += 1) {
    const character = text[index];

    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (character === "\\") {
        escaped = true;
      } else if (character === "\"") {
        inString = false;
      }
      continue;
    }

    if (character === "\"") {
      inString = true;
      continue;
    }

    if (character === "{") {
      depth += 1;
      continue;
    }

    if (character === "}") {
      depth -= 1;
      if (depth === 0) {
        return text.slice(start, index + 1);
      }
    }
  }

  return "";
}

function inferLabelFromNarrative(text) {
  const normalized = normalizeWhitespace(text).toLowerCase();

  if (
    normalized.includes("eher unpassend") ||
    normalized.includes("not a good fit") ||
    normalized.includes("not suitable") ||
    normalized.includes("less suitable")
  ) {
    return "eher unpassend";
  }

  if (
    normalized.includes("passt gut") ||
    normalized.includes("good fit") ||
    normalized.includes("strong fit") ||
    normalized.includes("well aligned")
  ) {
    return "passt gut";
  }

  if (
    normalized.includes("pruefen") ||
    normalized.includes("prüfen") ||
    normalized.includes("needs review") ||
    normalized.includes("mixed signals") ||
    normalized.includes("unclear")
  ) {
    return "pruefen";
  }

  return "";
}

function parseNarrativeFallback(value) {
  const text = normalizeWhitespace(stripThinkingArtifacts(value));
  const label = inferLabelFromNarrative(text);

  if (!label) {
    throw new Error(text ? `Leere JSON-Antwort, Freitext beginnt mit: ${text.slice(0, 140)}` : "Leere LLM-Antwort");
  }

  const scoreMatch = text.match(/\bscore\b[:\s-]*(\d{1,3})/i) || text.match(/\b(\d{1,3})\s*\/\s*100\b/);
  const score = scoreMatch ? Number(scoreMatch[1]) : null;
  const firstSentence = text.split(/(?<=[.!?])\s+/)[0] || text;

  return {
    label,
    score,
    reason: firstSentence.slice(0, 160)
  };
}

function extractJsonObject(value) {
  const text = stripThinkingArtifacts(value);

  if (!text) {
    throw new Error("Leere LLM-Antwort");
  }

  const fencedMatch = text.match(/```+\s*([\s\S]*?)```+/i);
  if (fencedMatch) {
    return JSON.parse(fencedMatch[1]);
  }

  const objectCandidate = extractFirstJsonObject(text);
  if (objectCandidate) {
    return JSON.parse(objectCandidate);
  }

  try {
    return JSON.parse(text);
  } catch {
    return parseNarrativeFallback(text);
  }
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
