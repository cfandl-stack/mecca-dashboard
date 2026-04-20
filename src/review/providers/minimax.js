const DEFAULT_MINIMAX_BASE_URL = "https://api.minimax.io";
const DEFAULT_MINIMAX_MODEL = "MiniMax-M2.5";

function normalizeBaseUrl(value) {
  return String(value || "").trim().replace(/\/+$/, "");
}

function getMinimaxConfig() {
  return {
    apiKey: String(process.env.LLM_API_KEY || process.env.MINIMAX_API_KEY || "").trim(),
    baseUrl: normalizeBaseUrl(
      process.env.LLM_BASE_URL || process.env.MINIMAX_BASE_URL || DEFAULT_MINIMAX_BASE_URL
    ),
    model: String(process.env.LLM_MODEL || process.env.MINIMAX_MODEL || DEFAULT_MINIMAX_MODEL).trim()
  };
}

function isMinimaxConfigured() {
  const config = getMinimaxConfig();
  return Boolean(config.apiKey && config.baseUrl && config.model);
}

function flattenContentParts(value) {
  if (!value) {
    return "";
  }

  if (typeof value === "string") {
    return value.trim();
  }

  if (Array.isArray(value)) {
    return value
      .map((part) => {
        if (typeof part === "string") {
          return part;
        }

        if (part && typeof part === "object") {
          return String(part.text || part.content || "").trim();
        }

        return "";
      })
      .filter(Boolean)
      .join("\n")
      .trim();
  }

  if (typeof value === "object") {
    return String(value.text || value.content || "").trim();
  }

  return String(value).trim();
}

function summarizePayload(payload) {
  return {
    model: payload?.model,
    finishReason: payload?.choices?.[0]?.finish_reason || "",
    baseStatusCode: payload?.base_resp?.status_code,
    baseStatusMessage: payload?.base_resp?.status_msg || "",
    inputSensitive: payload?.input_sensitive,
    inputSensitiveType: payload?.input_sensitive_type,
    outputSensitive: payload?.output_sensitive,
    outputSensitiveType: payload?.output_sensitive_type
  };
}

function extractResponseContent(payload) {
  const message = payload?.choices?.[0]?.message;

  const content = flattenContentParts(message?.content);
  if (content) {
    return content;
  }

  const reasoningContent = flattenContentParts(message?.reasoning_content);
  if (reasoningContent) {
    return reasoningContent;
  }

  const deltaContent = flattenContentParts(payload?.choices?.[0]?.delta?.content);
  if (deltaContent) {
    return deltaContent;
  }

  return "";
}

async function createMinimaxReview(prompt, options = {}) {
  const config = getMinimaxConfig();

  if (!config.apiKey) {
    throw new Error("MiniMax API key fehlt");
  }

  const response = await fetch(`${config.baseUrl}/v1/text/chatcompletion_v2`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${config.apiKey}`,
      "Content-Type": "application/json"
    },
    signal: options.signal,
    body: JSON.stringify({
      model: config.model,
      temperature: 0,
      max_tokens: 12,
      messages: [
        {
          role: "system",
          name: "MiniMax AI",
          content:
            "Du klassifizierst Ausschreibungen fuer ein Raumplanungsbuero. Antworte nur mit genau einem Token: PASS oder CHECK oder NO. Kein JSON. Kein weiterer Text."
        },
        {
          role: "user",
          name: "reviewer",
          content: prompt
        }
      ]
    })
  });

  if (!response.ok) {
    throw new Error(`MiniMax request failed with status ${response.status}`);
  }

  const payload = await response.json();
  const content = extractResponseContent(payload);

  if (!content) {
    const summary = summarizePayload(payload);
    throw new Error(
      `MiniMax response enthaelt keinen Inhalt (${JSON.stringify(summary)})`
    );
  }

  return {
    provider: "minimax",
    model: config.model,
    rawContent: content,
    usage: payload?.usage || null,
    summary: summarizePayload(payload)
  };
}

module.exports = {
  createMinimaxReview,
  extractResponseContent,
  getMinimaxConfig,
  isMinimaxConfigured,
  summarizePayload
};
