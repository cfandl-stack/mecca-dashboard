const fs = require("node:fs/promises");

const {
  loadWeeklyConfig,
  normalizeFeedRecord,
  writeOutputs
} = require("./weeklyTenderFeed");

async function main() {
  const config = await loadWeeklyConfig(process.argv.slice(2));
  const records = JSON.parse(await fs.readFile(config.output.jsonPath, "utf8"));
  const normalizedRecords = records.map((record) => normalizeFeedRecord(record));

  await writeOutputs(normalizedRecords, config.output);
  console.log(`Dashboard-Daten neu geschrieben: ${normalizedRecords.length} Datensaetze`);
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
