const test = require("node:test");
const assert = require("node:assert/strict");

const {
  countryLabel,
  formatCpvSearchTerm,
  normalizeFeedRecord,
  parseDate,
  toIsoDate
} = require("../src/weeklyTenderFeed");

test("toIsoDate normalisiert TED- und USP-Datumsformate", () => {
  assert.equal(toIsoDate("2026-04-02+02:00"), "2026-04-02");
  assert.equal(toIsoDate("02.04.2026"), "2026-04-02");
});

test("parseDate gibt null für leere Werte zurück", () => {
  assert.equal(parseDate(""), null);
});

test("countryLabel übersetzt wichtige TED-Ländercodes", () => {
  assert.equal(countryLabel(["AUT", "DEU"]), "Österreich; Deutschland");
});

test("formatCpvSearchTerm ergänzt lesbare CPV-Kurzlabels", () => {
  assert.equal(
    formatCpvSearchTerm("714100005", { "71410000-5": "Raumplanung" }),
    "CPV 71410000-5 - Raumplanung"
  );
});

test("normalizeFeedRecord setzt Dashboard-Spalten", () => {
  const record = normalizeFeedRecord({
    portal: "TED",
    suchbegriff: "Raumplanung",
    titel: " Test ",
    auftraggeber: " Auftraggeber ",
    frist: "2026-04-30",
    link: "https://example.test",
    cpvCodes: ["71410000", "71410000"],
    beschreibung: " Beschreibung ",
    veroeffentlichungsdatum: "2026-04-02+02:00",
    organisationLand: "DEU"
  });

  assert.equal(record.titel, "Test");
  assert.deepEqual(record.cpvCodes, ["71410000"]);
  assert.equal(record.veroeffentlichungsdatum, "2026-04-02");
  assert.equal(record._recordKey, "https://example.test");
});
