const fs = require("node:fs/promises");
const path = require("node:path");

function normalizeValue(value) {
  return String(value || "").trim();
}

async function main() {
  const config = {
    supabaseUrl: normalizeValue(process.env.PUBLIC_SUPABASE_URL || process.env.SUPABASE_URL),
    supabaseAnonKey: normalizeValue(
      process.env.PUBLIC_SUPABASE_ANON_KEY || process.env.SUPABASE_ANON_KEY
    ),
    hiddenRecordsTable: normalizeValue(process.env.SUPABASE_HIDDEN_RECORDS_TABLE) || "dashboard_hidden_records"
  };

  const outputPath = path.resolve(process.cwd(), "dash", "runtime-config.js");
  const content = `window.DASHBOARD_CONFIG = ${JSON.stringify(config, null, 2)};\n`;
  await fs.writeFile(outputPath, content, "utf8");
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
