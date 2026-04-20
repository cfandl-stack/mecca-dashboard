const test = require("node:test");
const assert = require("node:assert/strict");

const {
  buildTedCountryFilter,
  buildTedWeeklyQuery,
  countryLabel,
  filterExpiredRecords,
  filterHiddenRecords,
  formatCpvSearchTerm,
  isExpiredDeadline,
  normalizeFeedRecord,
  tedNoticeMatchesAllowedCountries,
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
  assert.equal(countryLabel(["AUT", "DEU", "LIE"]), "Österreich; Deutschland; Liechtenstein");
});

test("formatCpvSearchTerm ergänzt lesbare CPV-Kurzlabels", () => {
  assert.equal(
    formatCpvSearchTerm("714100005", { "71410000-5": "Raumplanung" }),
    "CPV 71410000-5 - Raumplanung"
  );
});

test("TED-Länderfilter beschränkt die Suche auf erlaubte Käuferländer", () => {
  assert.equal(
    buildTedCountryFilter(["AUT", "DEU", "HUN"]),
    "buyer-country IN (AUT DEU HUN)"
  );

  assert.equal(
    buildTedWeeklyQuery(
      { type: "keyword", value: "Raumplanung", allowedBuyerCountries: ["AUT"] },
      new Date("2026-04-01")
    ).includes("buyer-country IN (AUT)"),
    true
  );

  assert.equal(
    tedNoticeMatchesAllowedCountries({ "buyer-country": ["HUN"] }, ["AUT", "HUN"]),
    true
  );
  assert.equal(
    tedNoticeMatchesAllowedCountries({ "buyer-country": ["FRA"] }, ["AUT", "HUN"]),
    false
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
  assert.equal(record.recordKey, "https://example.test");
  assert.equal(record.reviewLabel, "ungeprueft");
});

test("abgelaufene Fristen werden serverseitig erkannt", () => {
  assert.equal(isExpiredDeadline("2026-04-01", new Date("2026-04-20T00:00:00.000Z")), true);
  assert.equal(isExpiredDeadline("2026-04-20", new Date("2026-04-20T00:00:00.000Z")), false);
  assert.equal(isExpiredDeadline("", new Date("2026-04-20T00:00:00.000Z")), false);
});

test("Feed kann abgelaufene und manuell ausgeblendete Datensaetze herausfiltern", () => {
  const records = [
    normalizeFeedRecord({
      portal: "TED",
      suchbegriff: "Raumplanung",
      titel: "Aktiv",
      auftraggeber: "A",
      frist: "2026-04-22",
      link: "https://example.test/a",
      veroeffentlichungsdatum: "2026-04-10"
    }),
    normalizeFeedRecord({
      portal: "TED",
      suchbegriff: "Raumplanung",
      titel: "Alt",
      auftraggeber: "B",
      frist: "2026-04-01",
      link: "https://example.test/b",
      veroeffentlichungsdatum: "2026-04-10"
    })
  ];

  const visible = filterExpiredRecords(records, new Date("2026-04-20T00:00:00.000Z"));
  assert.equal(visible.length, 1);
  assert.equal(visible[0].link, "https://example.test/a");

  const hidden = filterHiddenRecords(visible, new Set(["https://example.test/a"]));
  assert.equal(hidden.length, 0);
});
