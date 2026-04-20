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
      temperature: 0.1,
      max_tokens: 220,
      messages: [
        {
          role: "system",
          name: "MiniMax AI",
          content:
            "Du klassifizierst Ausschreibungen fuer ein Raumplanungsbuero. Antworte ausschliesslich mit JSON."
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
  const content = payload?.choices?.[0]?.message?.content;

  if (!content) {
    throw new Error("MiniMax response enthaelt keinen Inhalt");
  }

  return {
    provider: "minimax",
    model: config.model,
    rawContent: content,
    usage: payload?.usage || null
  };
}

module.exports = {
  createMinimaxReview,
  getMinimaxConfig,
  isMinimaxConfigured
};
