const DEFAULT_HIDDEN_RECORDS_TABLE = "dashboard_hidden_records";

function normalizeBaseUrl(value) {
  return String(value || "").trim().replace(/\/+$/, "");
}

function getSupabaseServerConfig() {
  const supabaseUrl = normalizeBaseUrl(
    process.env.SUPABASE_URL || process.env.PUBLIC_SUPABASE_URL
  );
  const serviceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY || "").trim();
  const hiddenRecordsTable = String(
    process.env.SUPABASE_HIDDEN_RECORDS_TABLE || DEFAULT_HIDDEN_RECORDS_TABLE
  ).trim();

  return {
    enabled: Boolean(supabaseUrl && serviceRoleKey),
    supabaseUrl,
    serviceRoleKey,
    hiddenRecordsTable
  };
}

async function fetchHiddenRecords(logger) {
  const config = getSupabaseServerConfig();

  if (!config.enabled) {
    logger?.info("Supabase Hidden Records deaktiviert", {
      reason: "SUPABASE_URL oder SUPABASE_SERVICE_ROLE_KEY fehlt"
    });
    return [];
  }

  const url = new URL(`${config.supabaseUrl}/rest/v1/${config.hiddenRecordsTable}`);
  url.searchParams.set("select", "record_key");

  const response = await fetch(url, {
    headers: {
      apikey: config.serviceRoleKey,
      Authorization: `Bearer ${config.serviceRoleKey}`,
      Accept: "application/json"
    }
  });

  if (!response.ok) {
    throw new Error(`Supabase hidden records request failed with status ${response.status}`);
  }

  const payload = await response.json();
  return Array.isArray(payload) ? payload : [];
}

async function fetchHiddenRecordKeys(logger) {
  try {
    const rows = await fetchHiddenRecords(logger);
    return new Set(
      rows
        .map((row) => String(row.record_key || "").trim())
        .filter(Boolean)
    );
  } catch (error) {
    logger?.warn("Supabase Hidden Records konnten nicht geladen werden", {
      message: error instanceof Error ? error.message : String(error)
    });
    return new Set();
  }
}

module.exports = {
  DEFAULT_HIDDEN_RECORDS_TABLE,
  fetchHiddenRecordKeys,
  getSupabaseServerConfig
};
