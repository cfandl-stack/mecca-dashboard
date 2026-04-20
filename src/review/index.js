const { normalizeWhitespace } = require("../core/utils");
const { createMinimaxReview, isMinimaxConfigured } = require("./providers/minimax");

const REVIEW_LABELS = new Set(["passt gut", "pruefen", "eher unpassend", "ungeprueft"]);
const TOKEN_TO_LABEL = {
  PASS: "passt gut",
  CHECK: "pruefen",
  NO: "eher unpassend"
};

const POSITIVE_HINTS = [
  "raumplanung",
  "spatial planning",
  "stadtentwicklung",
  "urban development",
  "ortsentwicklung",
  "regionalentwicklung",
  "regional development",
  "strategie",
  "strategy",
  "machbarkeitsstudie",
  "feasibility study",
  "studie",
  "evaluation",
  "evaluierung",
  "beteiligung",
  "moderation",
  "interreg",
  "smart village",
  "projektmanagement",
  "project management",
  "governance",
  "kommune",
  "municipalities",
  "municipality",
  "research",
  "forschung",
  "beratung",
  "advisory",
  "technical assistance",
  "standortentwicklung",
  "regional policy"
];

const NEGATIVE_HINTS = [
  "hochbau",
  "tiefbau",
  "bauausfuehrung",
  "construction works",
  "road infrastructure",
  "road maintenance",
  "maintenance",
  "street",
  "strassenbau",
  "schienenbau",
  "railway",
  "bridge",
  "tunnel",
  "citytunnel",
  "asphalt",
  "earthworks",
  "oberflaeche herrichten",
  "gelaendeoberflaeche",
  "hvac",
  "haustechnik",
  "tga",
  "oeba",
  "lieferung",
  "supply",
  "equipment",
  "medical",
  "medizintechnik",
  "reinigung",
  "cleaning",
  "schuldner",
  "insolvenzberatung",
  "sortieranlage",
  "general designer",
  "generalplaner",
  "design office"
];

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
    "PASS = passt gut fuer Raumplanung, Regionalentwicklung, Strategie, Studien, Evaluation, Moderation, Interreg oder projektnahe Beratungsleistungen.",
    "CHECK = gemischte Signale oder unklare Passung.",
    "NO = eher unpassend, vor allem Hochbau, Tiefbau, Strassenbau, Schienenbau, Bauausfuehrung, TGA, OeBA, Lieferleistungen, Reinigung, Medizintechnik oder fachfremde Leistungen.",
    "Antworte nur mit genau einem Token: PASS oder CHECK oder NO.",
    "Kein JSON. Kein Markdown. Kein Satz. Kein Vorspann. Kein Nachspann. Keine Begruendung.",
    `Datensatz: ${JSON.stringify(summarized)}`,
    "Antwort:"
  ].join("\n");
}

function asciiFold(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\u00df/g, "ss")
    .toLowerCase();
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

function scoreForLabel(label) {
  return label === "passt gut" ? 85 : label === "pruefen" ? 55 : label === "eher unpassend" ? 20 : null;
}

function defaultReasonForLabel(label) {
  return label === "passt gut"
    ? "MiniMax stuft den Datensatz als passend ein."
    : label === "pruefen"
      ? "MiniMax stuft den Datensatz als Grenzfall ein."
      : label === "eher unpassend"
        ? "MiniMax stuft den Datensatz als eher unpassend ein."
        : "";
}

function normalizeToken(value) {
  const text = stripThinkingArtifacts(value);
  if (!text) {
    return "";
  }

  const compact = normalizeWhitespace(text)
    .replace(/^[`"'([{<\s]+/, "")
    .replace(/[`"')\]}>.\s]+$/, "");

  if (/^(PASS|CHECK|NO)$/i.test(compact)) {
    return compact.toUpperCase();
  }

  const firstLine = compact.split(/\r?\n/)[0].trim();
  const firstLineMatch = firstLine.match(/^(PASS|CHECK|NO)\b/i);
  if (firstLineMatch) {
    return firstLineMatch[1].toUpperCase();
  }

  return "";
}

function tokenPayload(token) {
  const label = TOKEN_TO_LABEL[token] || "ungeprueft";

  return {
    label,
    score: scoreForLabel(label),
    reason: defaultReasonForLabel(label)
  };
}

function inferLabelFromNarrative(text) {
  const normalized = asciiFold(normalizeWhitespace(text));

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
    normalized.includes("prufen") ||
    normalized.includes("needs review") ||
    normalized.includes("mixed signals") ||
    normalized.includes("unclear")
  ) {
    return "pruefen";
  }

  return "";
}

function collectHintMatches(text, hints) {
  return hints.filter((hint) => text.includes(asciiFold(hint)));
}

function inferLabelFromKeywords(text, record) {
  const recordText = normalizeWhitespace(
    [
      text,
      record?.titel,
      record?.beschreibung,
      record?.auftraggeber,
      Array.isArray(record?.cpvCodes) ? record.cpvCodes.join(" ") : record?.cpvCodes,
      record?.organisationLand
    ]
      .filter(Boolean)
      .join(" ")
  );
  const normalized = asciiFold(recordText);
  const positiveMatches = collectHintMatches(normalized, POSITIVE_HINTS);
  const negativeMatches = collectHintMatches(normalized, NEGATIVE_HINTS);

  if (negativeMatches.length >= 2 && positiveMatches.length === 0) {
    return {
      label: "eher unpassend",
      reason: `Eher Bau- oder Lieferfokus (${negativeMatches.slice(0, 2).join(", ")}).`
    };
  }

  if (negativeMatches.length >= 1 && positiveMatches.length >= 1) {
    return {
      label: "pruefen",
      reason: `Gemischte Signale zwischen Planung (${positiveMatches[0]}) und Ausschlusskriterium (${negativeMatches[0]}).`
    };
  }

  if (positiveMatches.length >= 2 && negativeMatches.length === 0) {
    return {
      label: "passt gut",
      reason: `Wirkt passend wegen ${positiveMatches.slice(0, 2).join(" und ")}.`
    };
  }

  if (positiveMatches.length >= 1) {
    return {
      label: "pruefen",
      reason: `Teilweise passend wegen ${positiveMatches[0]}, aber nicht eindeutig genug.`
    };
  }

  if (negativeMatches.length >= 1) {
    return {
      label: "eher unpassend",
      reason: `Wirkt fachfremd wegen ${negativeMatches[0]}.`
    };
  }

  return null;
}

function parseNarrativeFallback(value, record) {
  const text = normalizeWhitespace(stripThinkingArtifacts(value));
  const label = inferLabelFromNarrative(text);

  if (label) {
    const scoreMatch = text.match(/\bscore\b[:\s-]*(\d{1,3})/i) || text.match(/\b(\d{1,3})\s*\/\s*100\b/);
    const score = scoreMatch ? Number(scoreMatch[1]) : scoreForLabel(label);
    const firstSentence = text.split(/(?<=[.!?])\s+/)[0] || text;

    return {
      label,
      score,
      reason: firstSentence.slice(0, 160)
    };
  }

  const heuristic = inferLabelFromKeywords(text, record);
  if (heuristic) {
    return {
      label: heuristic.label,
      score: scoreForLabel(heuristic.label),
      reason: heuristic.reason
    };
  }

  return {
    label: "pruefen",
    score: scoreForLabel("pruefen"),
    reason: text
      ? "MiniMax antwortete ohne verwertbares Token; Datensatz vorsichtshalber pruefen."
      : "Leere LLM-Antwort; Datensatz vorsichtshalber pruefen."
  };
}

function extractJsonObject(value, record) {
  const text = stripThinkingArtifacts(value);

  if (!text) {
    throw new Error("Leere LLM-Antwort");
  }

  const token = normalizeToken(text);
  if (token) {
    return tokenPayload(token);
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
    return parseNarrativeFallback(text, record);
  }
}

function normalizeReviewPayload(payload, provider, model) {
  const label = normalizeWhitespace(payload?.label).toLowerCase();
  const normalizedLabel = REVIEW_LABELS.has(label) ? label : "ungeprueft";
  const score = Number(payload?.score);
  const normalizedScore = Number.isFinite(score)
    ? Math.max(0, Math.min(100, Math.round(score)))
    : scoreForLabel(normalizedLabel);

  return {
    reviewLabel: normalizedLabel,
    reviewScore: normalizedScore,
    reviewReason: normalizeWhitespace(payload?.reason || defaultReasonForLabel(normalizedLabel)).slice(0, 160),
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
      const parsed = extractJsonObject(review.rawContent, record);
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
  buildPrompt,
  enrichRecordsWithReview,
  extractJsonObject,
  getReviewRuntimeConfig,
  normalizeReviewPayload
};
