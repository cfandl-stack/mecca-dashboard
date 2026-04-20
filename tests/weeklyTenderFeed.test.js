const test = require("node:test");
const assert = require("node:assert/strict");
const { extractResponseContent, summarizePayload } = require("../src/review/providers/minimax");
const { buildPrompt, extractJsonObject } = require("../src/review");

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

test("MiniMax Adapter kann String- und Array-Content lesen", () => {
  assert.equal(
    extractResponseContent({
      choices: [{ message: { content: "Hallo" } }]
    }),
    "Hallo"
  );

  assert.equal(
    extractResponseContent({
      choices: [
        {
          message: {
            content: [
              { text: "Teil 1" },
              { content: "Teil 2" }
            ]
          }
        }
      ]
    }),
    "Teil 1\nTeil 2"
  );
});

test("MiniMax Payload Summary extrahiert relevante Debug-Felder", () => {
  const summary = summarizePayload({
    model: "MiniMax-M2.7",
    choices: [{ finish_reason: "stop" }],
    base_resp: { status_code: 1027, status_msg: "output new_sensitive" },
    input_sensitive: false,
    output_sensitive: true,
    output_sensitive_type: 4
  });

  assert.deepEqual(summary, {
    model: "MiniMax-M2.7",
    finishReason: "stop",
    baseStatusCode: 1027,
    baseStatusMessage: "output new_sensitive",
    inputSensitive: false,
    inputSensitiveType: undefined,
    outputSensitive: true,
    outputSensitiveType: 4
  });
});

test("Review Parser kann JSON aus Markdown-Codefences extrahieren", () => {
  const parsed = extractJsonObject("```json\n{\"label\":\"pruefen\",\"score\":58,\"reason\":\"Grenzfall wegen Bauanteil.\"}\n```");

  assert.deepEqual(parsed, {
    label: "pruefen",
    score: 58,
    reason: "Grenzfall wegen Bauanteil."
  });
});

test("Review Parser kann Freitext-Fallback auf Label pruefen abbilden", () => {
  const parsed = extractJsonObject("Let me analyze this. The tender has mixed signals and needs review because planning and construction aspects overlap.");

  assert.equal(parsed.label, "pruefen");
  assert.equal(parsed.score, 55);
  assert.match(parsed.reason, /Let me analyze this/i);
});

test("Review Parser kann PASS Token direkt zuordnen", () => {
  const parsed = extractJsonObject("PASS");

  assert.equal(parsed.label, "passt gut");
  assert.equal(parsed.score, 85);
});

test("Review Parser kann CHECK Token in erster Zeile zuordnen", () => {
  const parsed = extractJsonObject("CHECK\nThis looks borderline.");

  assert.equal(parsed.label, "pruefen");
  assert.equal(parsed.score, 55);
});

test("Review Parser kann NO Token mit Satzzeichen zuordnen", () => {
  const parsed = extractJsonObject("NO.");

  assert.equal(parsed.label, "eher unpassend");
  assert.equal(parsed.score, 20);
});

test("Review Parser kann Bau-Freitext heuristisch als eher unpassend werten", () => {
  const parsed = extractJsonObject(
    "Let me analyze this tender. The tender is about road infrastructure maintenance on a tunnel section.",
    {
      titel: "Road infrastructure maintenance",
      beschreibung: "Tunnel section and asphalt works",
      suchbegriff: "Projektmanagement"
    }
  );

  assert.equal(parsed.label, "eher unpassend");
  assert.match(parsed.reason, /Bau|fachfremd/i);
});

test("Review Parser kann Planungs-Freitext heuristisch als passend werten", () => {
  const parsed = extractJsonObject(
    "Let me analyze this tender. The tender concerns a feasibility study for regional development and strategy.",
    {
      titel: "Feasibility study for regional development",
      beschreibung: "Strategy and evaluation support",
      suchbegriff: "Strategie"
    }
  );

  assert.equal(parsed.label, "passt gut");
  assert.match(parsed.reason, /passend/i);
});

test("Review Parser faellt bei unverwertbarem MiniMax-Freitext konservativ auf pruefen zurueck", () => {
  const parsed = extractJsonObject(
    "Let me analyze this procurement notice in more detail before deciding.",
    {
      titel: "Rahmenvertrag externe Leistungen",
      beschreibung: "Diverse Unterstuetzungsleistungen",
      auftraggeber: "Teststelle"
    }
  );

  assert.equal(parsed.label, "pruefen");
  assert.equal(parsed.score, 55);
  assert.match(parsed.reason, /vorsichtshalber pruefen/i);
});

test("Review Prompt fordert nur PASS CHECK NO an", () => {
  const prompt = buildPrompt({
    portal: "TED",
    suchbegriff: "Strategie",
    titel: "Machbarkeitsstudie",
    auftraggeber: "Stadt Test",
    beschreibung: "Regionalentwicklung",
    cpvCodes: ["71410000-5"],
    organisationLand: "AUT",
    frist: "2026-04-30",
    veroeffentlichungsdatum: "2026-04-20"
  });

  assert.match(prompt, /PASS oder CHECK oder NO/);
  assert.match(prompt, /Kein JSON/);
  assert.doesNotMatch(prompt, /"label":"passt gut\|pruefen\|eher unpassend"/);
});
