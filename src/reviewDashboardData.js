const fs = require("node:fs/promises");

const { extractJsonObject, normalizeReviewPayload } = require("./review");
const { loadWeeklyConfig, normalizeFeedRecord, writeOutputs } = require("./weeklyTenderFeed");

function createLocalReview(record) {
  const parsed = extractJsonObject("Codex local review", record);
  const review = normalizeReviewPayload(parsed, "codex-automation", "codex");

  if (/minimax|llm-antwort/i.test(review.reviewReason)) {
    review.reviewReason =
      review.reviewLabel === "passt gut"
        ? "Codex stuft den Datensatz anhand der fachlichen Signale als passend ein."
        : review.reviewLabel === "eher unpassend"
          ? "Codex erkennt fachfremde oder ausgeschlossene Schwerpunkte."
          : "Codex findet gemischte oder unklare Signale; bitte bei Bedarf manuell pruefen.";
  }

  return review;
}

async function main() {
  const config = await loadWeeklyConfig(process.argv.slice(2));
  const records = JSON.parse(await fs.readFile(config.output.jsonPath, "utf8"));
  const reviewedAt = new Date().toISOString();
  const reviewedRecords = records.map((record) =>
    normalizeFeedRecord({
      ...record,
      ...createLocalReview(record),
      reviewedAt
    })
  );

  await writeOutputs(reviewedRecords, config.output);

  const summary = reviewedRecords.reduce((counts, record) => {
    counts[record.reviewLabel] = (counts[record.reviewLabel] || 0) + 1;
    return counts;
  }, {});

  console.log(
    `Dashboard-Daten bewertet: ${reviewedRecords.length} Datensaetze (${Object.entries(summary)
      .map(([label, count]) => `${label}: ${count}`)
      .join(", ")})`
  );
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
