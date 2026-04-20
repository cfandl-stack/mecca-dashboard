const today = new Date();
today.setHours(0, 0, 0, 0);

const DASHBOARD_CONFIG = window.DASHBOARD_CONFIG || {};
const SUPABASE_ENABLED = Boolean(
  DASHBOARD_CONFIG.supabaseUrl &&
  DASHBOARD_CONFIG.supabaseAnonKey &&
  window.supabase &&
  typeof window.supabase.createClient === "function"
);
const HIDDEN_RECORDS_TABLE = DASHBOARD_CONFIG.hiddenRecordsTable || "dashboard_hidden_records";

let sortCol = "frist";
let sortDir = 1;
let portalChart = null;
let fristChart = null;
let supabaseClient = null;
let currentSession = null;
let hiddenRows = [];
let hiddenRecordKeys = new Set();
let dataRecords = [];

function simpleHash(value) {
  const text = String(value || "");
  let hash = 0;

  for (let index = 0; index < text.length; index += 1) {
    hash = (hash << 5) - hash + text.charCodeAt(index);
    hash |= 0;
  }

  return `rk-${Math.abs(hash)}`;
}

function normalizeRecord(record, extra = {}) {
  const normalized = {
    ...record,
    ...extra
  };

  normalized.recordKey =
    normalized.recordKey ||
    normalized.link ||
    simpleHash(
      [
        normalized.portal,
        normalized.suchbegriff,
        normalized.titel,
        normalized.auftraggeber,
        normalized.veroeffentlichungsdatum
      ].join("|")
    );
  normalized.reviewLabel = String(normalized.reviewLabel || "ungeprueft").trim().toLowerCase();
  normalized.reviewReason = String(normalized.reviewReason || "").trim();
  normalized.reviewProvider = String(normalized.reviewProvider || "").trim();
  normalized.reviewModel = String(normalized.reviewModel || "").trim();
  normalized.reviewScore = Number.isFinite(Number(normalized.reviewScore))
    ? Number(normalized.reviewScore)
    : null;
  normalized.reviewedAt = String(normalized.reviewedAt || "").trim();
  normalized.cpvCodes = Array.isArray(normalized.cpvCodes)
    ? normalized.cpvCodes
    : String(normalized.cpvCodes || "")
        .split(";")
        .map((value) => value.trim())
        .filter(Boolean);
  normalized._isHidden = Boolean(normalized._isHidden);

  return normalized;
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function prettifyGermanText(value) {
  return String(value || "")
    .replace(/\bFuer\b/g, "Für")
    .replace(/\bfuer\b/g, "für")
    .replace(/\bOeffnen\b/g, "Öffnen")
    .replace(/\boeffnen\b/g, "öffnen")
    .replace(/\bnoetig\b/g, "nötig")
    .replace(/\bNoetig\b/g, "Nötig")
    .replace(/\bEintraege\b/g, "Einträge")
    .replace(/\beintraege\b/g, "einträge")
    .replace(/\bVeroeffentlicht\b/g, "Veröffentlicht")
    .replace(/\bVeroeffentlichung\b/g, "Veröffentlichung")
    .replace(/\bveroeffentlicht\b/g, "veröffentlicht")
    .replace(/\bveroeffentlichung\b/g, "veröffentlichung")
    .replace(/\bPruefen\b/g, "Prüfen")
    .replace(/\bpruefen\b/g, "prüfen")
    .replace(/\bUngeprueft\b/g, "Ungeprüft")
    .replace(/\bungeprueft\b/g, "ungeprüft")
    .replace(/\bgeprueft\b/g, "geprüft")
    .replace(/\bbestaetigen\b/g, "bestätigen")
    .replace(/\bBestaetigen\b/g, "Bestätigen")
    .replace(/\bEinschaetzung\b/g, "Einschätzung")
    .replace(/\beinschaetzung\b/g, "einschätzung");
}

function parseFrist(value) {
  if (!value || !String(value).trim()) {
    return null;
  }

  if (String(value).includes("-")) {
    const parsedIso = new Date(value);
    return Number.isNaN(parsedIso.getTime()) ? null : parsedIso;
  }

  const months = {
    Jan: 0,
    Feb: 1,
    Mar: 2,
    Apr: 3,
    Mai: 4,
    Jun: 5,
    Jul: 6,
    Aug: 7,
    Sep: 8,
    Okt: 9,
    Nov: 10,
    Dez: 11
  };
  const match = String(value).match(/(\d+)\s+(\w+)\.?\s+(\d{4})/);

  if (!match) {
    return null;
  }

  const month = months[match[2]] ?? null;
  if (month === null) {
    return null;
  }

  return new Date(Number(match[3]), month, Number(match[1]));
}

function fristDays(value) {
  const parsed = parseFrist(value);
  if (!parsed) {
    return null;
  }

  return Math.round((parsed - today) / 86400000);
}

function isExpiredDeadline(value) {
  const parsed = parseFrist(value);
  return parsed ? parsed.getTime() < today.getTime() : false;
}

function formatReviewLabel(label) {
  if (label === "passt gut") {
    return "Passt gut";
  }

  if (label === "pruefen") {
    return "Prüfen";
  }

  if (label === "eher unpassend") {
    return "Eher unpassend";
  }

  return "Ungeprüft";
}

function updateReviewFilterPills(records = getVisibleBaseRecords()) {
  const currentValue = document.getElementById("f-review").value;
  const counts = {
    "": records.length,
    "passt gut": 0,
    pruefen: 0,
    "eher unpassend": 0,
    ungeprueft: 0
  };

  records.forEach((record) => {
    const label = record.reviewLabel || "ungeprueft";
    if (Object.prototype.hasOwnProperty.call(counts, label)) {
      counts[label] += 1;
    }
  });

  document.querySelectorAll("#review-filter-pills .review-pill").forEach((button) => {
    const value = button.dataset.value || "";
    const baseLabel = formatReviewLabel(value || "ungeprueft");
    const visibleLabel = value ? baseLabel : "Alle";
    button.textContent = `${visibleLabel} (${counts[value] ?? 0})`;
    button.classList.toggle("is-active", value === currentValue);
  });
}

function reviewBadge(record) {
  const label = record.reviewLabel || "ungeprueft";
  const className = {
    "passt gut": "badge-review-good",
    pruefen: "badge-review-check",
    "eher unpassend": "badge-review-bad",
    ungeprueft: "badge-review-unknown"
  }[label] || "badge-review-unknown";
  const reason = record.reviewReason ? ` title="${escapeHtml(prettifyGermanText(record.reviewReason))}"` : "";

  return `<span class="badge ${className}"${reason}>${escapeHtml(formatReviewLabel(label))}</span>`;
}

function portalBadge(portal) {
  if (portal === "TED") {
    return '<span class="badge badge-ted">TED</span>';
  }

  if (portal === "USP Bund") {
    return '<span class="badge badge-usp">USP Bund</span>';
  }

  if (portal === "Burgenland") {
    return '<span class="badge badge-burgenland">Burgenland</span>';
  }

  return `<span class="badge">${escapeHtml(portal || "Unbekannt")}</span>`;
}

function suchBadge(value) {
  if (value === "Raumplanung") {
    return '<span class="badge badge-raumplanung">Raumplanung</span>';
  }

  if (value === "Regionalentwicklung") {
    return '<span class="badge badge-regional">Regionalentwicklung</span>';
  }

  if (/^(CPV\s+)?\d{8}(-\d)?/.test(String(value || "")) || String(value || "").startsWith("CPV ")) {
    return `<span class="badge badge-cpv">${escapeHtml(value)}</span>`;
  }

  return `<span class="badge badge-projekt">${escapeHtml(value || "Projektmanagement")}</span>`;
}

function fristLabel(value) {
  const days = fristDays(value);

  if (days === null) {
    return '<span class="no-frist">-</span>';
  }

  if (days < 0) {
    return '<span class="no-frist">abgelaufen</span>';
  }

  if (days <= 14) {
    return `<span class="frist-soon">${escapeHtml(value)} (${days}d)</span>`;
  }

  return `<span class="frist-ok">${escapeHtml(value)}</span>`;
}

function buildLink(record) {
  let url = record.link;

  if (!url) {
    return "-";
  }

  if (!url.startsWith("http")) {
    if (record.portal === "Burgenland") {
      url = `https://burgenland.vergabeportal.at/${url}`;
    } else {
      url = `https://ausschreibungen.usp.gv.at/at.gv.bmdw.eproc-p/public/${url}`;
    }
  }

  return `<a class="link-btn" href="${escapeHtml(url)}" target="_blank" rel="noopener">Öffnen</a>`;
}

function cpvLabel(record) {
  const values = Array.isArray(record.cpvCodes) ? record.cpvCodes : [];
  return values.length ? escapeHtml(values.join("; ")) : '<span class="no-frist">-</span>';
}

function descriptionLabel(record) {
  const description = String(record.beschreibung || "").trim();
  const reviewReason = String(record.reviewReason || "").trim();
  const parts = [];

  if (description) {
    const shortDescription =
      description.length > 190 ? `${description.slice(0, 190)}...` : description;
    parts.push(`<div class="description-snippet">${escapeHtml(shortDescription)}</div>`);
  }

  parts.push(`<div class="review-meta">${reviewBadge(record)}</div>`);

  if (reviewReason) {
    parts.push(`<div class="small-meta">${escapeHtml(prettifyGermanText(reviewReason))}</div>`);
  }

  if (record._isHidden) {
    const hiddenMeta = [
      record.hiddenAt ? `Ausgeblendet: ${record.hiddenAt.slice(0, 10)}` : "",
      record.hiddenBy ? `von ${record.hiddenBy}` : ""
    ]
      .filter(Boolean)
      .join(" ");
    parts.push(`<div class="small-meta hidden-meta">${escapeHtml(hiddenMeta || "Ausgeblendet")}</div>`);
  }

  return parts.join("");
}

function organizationLabel(record) {
  const country = record.organisationLand
    ? `<div class="small-meta">${escapeHtml(record.organisationLand)}</div>`
    : "";
  return `${escapeHtml(record.auftraggeber || "")}${country}`;
}

function publicationLabel(record) {
  return record.veroeffentlichungsdatum
    ? escapeHtml(record.veroeffentlichungsdatum)
    : '<span class="no-frist">-</span>';
}

function canManageHiddenRecords() {
  return Boolean(SUPABASE_ENABLED && currentSession?.user);
}

function actionButton(record) {
  if (!SUPABASE_ENABLED) {
    return '<span class="action-hint">Setup ausstehend</span>';
  }

  if (record._isHidden) {
    if (!canManageHiddenRecords()) {
      return '<span class="action-hint">Login nötig</span>';
    }

    return `<button class="table-action secondary" data-action="restore" data-record-key="${escapeHtml(record.recordKey)}">Wiederherstellen</button>`;
  }

  if (!canManageHiddenRecords()) {
    return '<span class="action-hint">Login nötig</span>';
  }

  return `<button class="table-action" data-action="hide" data-record-key="${escapeHtml(record.recordKey)}">Ausblenden</button>`;
}

function getVisibleBaseRecords() {
  return dataRecords.filter((record) => !hiddenRecordKeys.has(record.recordKey));
}

function getHiddenDisplayRecords() {
  return hiddenRows.map((row) =>
    normalizeRecord(row.payload || {}, {
      recordKey: row.record_key,
      _isHidden: true,
      hiddenAt: row.hidden_at || "",
      hiddenBy: row.hidden_by || ""
    })
  );
}

function shouldShowHiddenRecords() {
  const toggle = document.getElementById("toggle-show-hidden");
  return Boolean(toggle?.checked && canManageHiddenRecords());
}

function getDisplayRecords() {
  const records = [...getVisibleBaseRecords()];

  if (shouldShowHiddenRecords()) {
    records.push(...getHiddenDisplayRecords());
  }

  return records;
}

function buildKPIs(records) {
  const soon = records.filter((record) => {
    const days = fristDays(record.frist);
    return days !== null && days >= 0 && days <= 14;
  });
  const withFrist = records.filter((record) => record.frist && String(record.frist).trim());
  const portals = [...new Set(records.map((record) => record.portal))];
  const latestDate = records.map((record) => record.veroeffentlichungsdatum).filter(Boolean).sort().pop() || "-";
  const reviewGood = records.filter((record) => record.reviewLabel === "passt gut").length;

  document.getElementById("kpi-row").innerHTML = `
    <div class="kpi"><div class="label">Gesamt</div><div class="value">${records.length}</div><div class="sub">aktive Ausschreibungen</div></div>
    <div class="kpi"><div class="label">Portale</div><div class="value">${portals.length}</div><div class="sub">${escapeHtml(portals.join(", "))}</div></div>
    <div class="kpi"><div class="label">Mit Frist</div><div class="value">${withFrist.length}</div><div class="sub">von ${records.length} Einträgen</div></div>
    <div class="kpi"><div class="label">Frist &lt; 14 Tage</div><div class="value urgent">${soon.length}</div><div class="sub">dringend</div></div>
    <div class="kpi"><div class="label">Passt gut</div><div class="value">${reviewGood}</div><div class="sub">AI-Einschätzung</div></div>
    <div class="kpi"><div class="label">Datenstand</div><div class="value" style="font-size:1.2rem">${escapeHtml(latestDate)}</div><div class="sub">Veröffentlichung</div></div>
  `;
}

function destroyCharts() {
  if (portalChart) {
    portalChart.destroy();
    portalChart = null;
  }

  if (fristChart) {
    fristChart.destroy();
    fristChart = null;
  }
}

function renderTermSummary(records) {
  const termSummary = document.getElementById("termSummary");
  const counts = {};

  records.forEach((record) => {
    counts[record.suchbegriff] = (counts[record.suchbegriff] || 0) + 1;
  });

  const entries = Object.entries(counts).sort((left, right) => right[1] - left[1]);
  const max = Math.max(...entries.map((entry) => entry[1]), 1);

  termSummary.innerHTML = entries
    .slice(0, 5)
    .map(
      ([label, count]) => `
        <div class="term-row" title="${escapeHtml(label)}">
          <div class="term-label">${escapeHtml(label)}</div>
          <div class="term-count">${count}</div>
          <div class="term-bar"><span style="width:${Math.max(8, Math.round((count / max) * 100))}%"></span></div>
        </div>
      `
    )
    .join("");
}

function buildCharts(records) {
  destroyCharts();

  const palette = ["#5ba872", "#c9b896", "#a89370", "#4a78be", "#8f6fb8", "#c0392b", "#e8dcc8"];
  const portalCounts = {};

  records.forEach((record) => {
    portalCounts[record.portal] = (portalCounts[record.portal] || 0) + 1;
  });

  portalChart = new Chart(document.getElementById("chartPortal"), {
    type: "doughnut",
    data: {
      labels: Object.keys(portalCounts),
      datasets: [
        {
          data: Object.values(portalCounts),
          backgroundColor: Object.keys(portalCounts).map((_, index) => palette[index % palette.length]),
          borderWidth: 0
        }
      ]
    },
    options: {
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: "bottom",
          labels: {
            font: {
              size: 11
            }
          }
        }
      }
    }
  });

  renderTermSummary(records);

  let hasFrist = 0;
  let soon = 0;
  let none = 0;

  records.forEach((record) => {
    const days = fristDays(record.frist);
    if (days === null) {
      none += 1;
    } else if (days <= 14) {
      soon += 1;
    } else {
      hasFrist += 1;
    }
  });

  fristChart = new Chart(document.getElementById("chartFrist"), {
    type: "doughnut",
    data: {
      labels: ["Frist > 14d", "Frist <= 14d", "Keine Frist"],
      datasets: [
        {
          data: [hasFrist, soon, none],
          backgroundColor: ["#5ba872", "#c0392b", "#e8dcc8"],
          borderWidth: 0
        }
      ]
    },
    options: {
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: "bottom",
          labels: {
            font: {
              size: 11
            }
          }
        }
      }
    }
  });
}

function scoreEntry(record) {
  let score = 0;
  const haystack = `${record.titel} ${record.beschreibung || ""}`.toLowerCase();

  if (record.reviewLabel === "passt gut") {
    score += 40;
  } else if (record.reviewLabel === "pruefen") {
    score += 12;
  } else if (record.reviewLabel === "eher unpassend") {
    score -= 30;
  }

  if (Number.isFinite(record.reviewScore)) {
    score += Math.round((record.reviewScore - 50) / 3);
  }

  if (haystack.includes("interreg")) score += 6;
  if (haystack.includes("regional")) score += 4;
  if (haystack.includes("raumplanung")) score += 6;
  if (haystack.includes("strategie")) score += 4;
  if (haystack.includes("studie") || haystack.includes("machbarkeit")) score += 4;
  if (haystack.includes("projektsteuerung") || haystack.includes("projektleitung")) score += 2;
  if (haystack.includes("hochbau") || haystack.includes("tiefbau")) score -= 8;
  if (haystack.includes("autobahn") || haystack.includes("schiene")) score -= 8;
  if (haystack.includes("werbeagentur") || haystack.includes("marketing")) score -= 7;
  if (haystack.includes("software") || haystack.includes("sap")) score -= 7;

  return score;
}

function buildAdvisor(records) {
  const scored = records
    .map((record) => ({ ...record, _score: scoreEntry(record) }))
    .sort((left, right) => right._score - left._score);
  const top = scored.filter((record) => record._score > 0).slice(0, 3);
  const good = records.filter((record) => record.reviewLabel === "passt gut").length;
  const check = records.filter((record) => record.reviewLabel === "pruefen").length;
  const bad = records.filter((record) => record.reviewLabel === "eher unpassend").length;

  let summary = `<strong>Stefan:</strong> ${records.length} aktive Ausschreibungen geprüft. `;
  summary += `<strong>${good}</strong> passen gut, <strong>${check}</strong> bitte prüfen, <strong>${bad}</strong> eher unpassend.`;
  document.getElementById("advisor-summary").innerHTML = summary;

  const picksElement = document.getElementById("advisor-inline-picks");
  if (!top.length) {
    picksElement.innerHTML = "";
    return;
  }

  picksElement.innerHTML = top
    .map((record) => {
      const days = fristDays(record.frist);
      const urgent = days !== null && days >= 0 && days <= 14;
      const fristInfo =
        days === null
          ? "Keine Frist angegeben"
          : urgent
            ? `Frist in ${days} Tagen`
            : `Frist: ${record.frist}`;

      return `
        <div class="advisor-inline-pick${urgent ? " warn" : ""}" title="${escapeHtml(prettifyGermanText(record.reviewReason || "AI-Einschätzung ohne Zusatzgrund"))}">
          <div class="advisor-inline-title">${escapeHtml(record.titel)}</div>
          <div class="advisor-inline-meta">${escapeHtml(record.auftraggeber)} · ${escapeHtml(fristInfo)}</div>
          <div class="advisor-inline-meta">${reviewBadge(record)}</div>
        </div>
      `;
    })
    .join("");
}

function getFiltered() {
  const portal = document.getElementById("f-portal").value;
  const searchTerm = document.getElementById("f-such").value;
  const fristFilter = document.getElementById("f-frist").value;
  const reviewFilter = document.getElementById("f-review").value;
  const query = document.getElementById("f-search").value.toLowerCase();

  return getDisplayRecords().filter((record) => {
    if (portal && record.portal !== portal) return false;
    if (searchTerm && record.suchbegriff !== searchTerm) return false;
    if (reviewFilter && record.reviewLabel !== reviewFilter) return false;
    if (fristFilter === "yes" && !(record.frist || "").trim()) return false;
    if (fristFilter === "no" && (record.frist || "").trim()) return false;
    if (fristFilter === "soon") {
      const days = fristDays(record.frist);
      if (days === null || days < 0 || days > 14) return false;
    }

    const haystack = [
      record.titel,
      record.auftraggeber,
      record.beschreibung,
      record.organisationLand,
      record.veroeffentlichungsdatum,
      record.reviewReason,
      record.reviewLabel,
      Array.isArray(record.cpvCodes) ? record.cpvCodes.join(" ") : record.cpvCodes
    ]
      .join(" ")
      .toLowerCase();

    if (query && !haystack.includes(query)) return false;
    return true;
  });
}

function getSorted(records) {
  return [...records].sort((left, right) => {
    let leftValue = left[sortCol] || "";
    let rightValue = right[sortCol] || "";

    if (Array.isArray(leftValue)) leftValue = leftValue.join("; ");
    if (Array.isArray(rightValue)) rightValue = rightValue.join("; ");

    if (sortCol === "frist") {
      const leftDate = parseFrist(leftValue);
      const rightDate = parseFrist(rightValue);

      if (!leftDate && !rightDate) return 0;
      if (!leftDate) return 1;
      if (!rightDate) return -1;

      return (leftDate - rightDate) * sortDir;
    }

    return String(leftValue).localeCompare(String(rightValue)) * sortDir;
  });
}

function renderTable(records) {
  const tbody = document.getElementById("table-body");

  if (!records.length) {
    tbody.innerHTML = '<tr><td colspan="9" class="no-data">Keine Einträge gefunden.</td></tr>';
    document.getElementById("result-count").textContent = "0 Einträge";
    return;
  }

  tbody.innerHTML = records
    .map(
      (record) => `
        <tr class="${record._isHidden ? "row-hidden" : ""}">
          <td>${portalBadge(record.portal)}</td>
          <td>${suchBadge(record.suchbegriff)}</td>
          <td class="titel-cell"><strong>${escapeHtml(record.titel)}</strong>${descriptionLabel(record)}</td>
          <td class="ag-cell">${organizationLabel(record)}</td>
          <td class="date-cell">${publicationLabel(record)}</td>
          <td style="white-space:nowrap">${fristLabel(record.frist)}</td>
          <td class="cpv-list">${cpvLabel(record)}</td>
          <td>${buildLink(record)}</td>
          <td class="action-cell">${actionButton(record)}</td>
        </tr>
      `
    )
    .join("");
  document.getElementById("result-count").textContent = `${records.length} Einträge`;
}

function updateTable() {
  renderTable(getSorted(getFiltered()));
  updateReviewFilterPills();
}

function populateFilters() {
  const visibleRecords = getVisibleBaseRecords();
  const portals = [...new Set(visibleRecords.map((record) => record.portal))].sort();
  const searchTerms = [...new Set(visibleRecords.map((record) => record.suchbegriff))].sort();
  const portalSelect = document.getElementById("f-portal");
  const suchSelect = document.getElementById("f-such");

  portalSelect.innerHTML = '<option value="">Alle</option>';
  suchSelect.innerHTML = '<option value="">Alle</option>';

  portals.forEach((portal) => {
    const option = document.createElement("option");
    option.value = portal;
    option.textContent = portal;
    portalSelect.appendChild(option);
  });

  searchTerms.forEach((term) => {
    const option = document.createElement("option");
    option.value = term;
    option.textContent = term;
    suchSelect.appendChild(option);
  });

  updateReviewFilterPills(visibleRecords);
}

function setAdminMessage(message, tone = "neutral") {
  const note = document.getElementById("admin-note");
  note.textContent = message;
  note.dataset.tone = tone;
}

function updateAuthUi() {
  const card = document.getElementById("admin-card");
  const loginRow = document.getElementById("admin-login");
  const actionsRow = document.getElementById("admin-actions");
  const status = document.getElementById("auth-status");

  card.hidden = false;

  if (!SUPABASE_ENABLED) {
    loginRow.hidden = true;
    actionsRow.hidden = true;
    status.textContent = "Supabase nicht konfiguriert";
    setAdminMessage(
      "Für Ausblenden/Wiederherstellen bitte PUBLIC_SUPABASE_URL und PUBLIC_SUPABASE_ANON_KEY konfigurieren.",
      "warning"
    );
    return;
  }

  if (currentSession?.user) {
    loginRow.hidden = true;
    actionsRow.hidden = false;
    status.textContent = `Angemeldet: ${currentSession.user.email || "Benutzer"}`;
    setAdminMessage("Ausblendungen werden sofort gespeichert und im Wochenlauf erneut ausgeschlossen.", "success");
  } else {
    loginRow.hidden = false;
    actionsRow.hidden = true;
    status.textContent = "Nicht angemeldet";
    setAdminMessage("Mit Magic Link anmelden, um Treffer auszublenden oder wiederherzustellen.", "neutral");
  }
}

function refreshOverview() {
  const visibleRecords = getVisibleBaseRecords();
  populateFilters();
  buildKPIs(visibleRecords);
  buildCharts(visibleRecords);
  buildAdvisor(visibleRecords);
  updateTable();
}

async function loadHiddenRecords() {
  if (!SUPABASE_ENABLED) {
    hiddenRows = [];
    hiddenRecordKeys = new Set();
    refreshOverview();
    return;
  }

  const { data, error } = await supabaseClient
    .from(HIDDEN_RECORDS_TABLE)
    .select("record_key,payload,hidden_at,hidden_by")
    .order("hidden_at", { ascending: false });

  if (error) {
    setAdminMessage(`Hidden Records konnten nicht geladen werden: ${error.message}`, "warning");
    hiddenRows = [];
    hiddenRecordKeys = new Set();
  } else {
    hiddenRows = Array.isArray(data) ? data : [];
    hiddenRecordKeys = new Set(hiddenRows.map((row) => String(row.record_key || "").trim()).filter(Boolean));
  }

  refreshOverview();
}

async function hideRecord(recordKey) {
  if (!canManageHiddenRecords()) {
    setAdminMessage("Bitte zuerst anmelden.", "warning");
    return;
  }

  const record = dataRecords.find((entry) => entry.recordKey === recordKey);
  if (!record) {
    setAdminMessage("Datensatz wurde lokal nicht gefunden.", "warning");
    return;
  }

  const payload = {
    recordKey: record.recordKey,
    portal: record.portal,
    suchbegriff: record.suchbegriff,
    titel: record.titel,
    auftraggeber: record.auftraggeber,
    frist: record.frist,
    link: record.link,
    cpvCodes: record.cpvCodes,
    beschreibung: record.beschreibung,
    veroeffentlichungsdatum: record.veroeffentlichungsdatum,
    organisationLand: record.organisationLand,
    scrapedAt: record.scrapedAt,
    reviewLabel: record.reviewLabel,
    reviewScore: record.reviewScore,
    reviewReason: record.reviewReason,
    reviewProvider: record.reviewProvider,
    reviewModel: record.reviewModel,
    reviewedAt: record.reviewedAt
  };

  const { error } = await supabaseClient.from(HIDDEN_RECORDS_TABLE).upsert(
    {
      record_key: recordKey,
      payload,
      hidden_by: currentSession.user.email || ""
    },
    {
      onConflict: "record_key"
    }
  );

  if (error) {
    setAdminMessage(`Ausblenden fehlgeschlagen: ${error.message}`, "warning");
    return;
  }

  setAdminMessage("Treffer wurde ausgeblendet.", "success");
  await loadHiddenRecords();
}

async function restoreRecord(recordKey) {
  if (!canManageHiddenRecords()) {
    setAdminMessage("Bitte zuerst anmelden.", "warning");
    return;
  }

  const { error } = await supabaseClient.from(HIDDEN_RECORDS_TABLE).delete().eq("record_key", recordKey);

  if (error) {
    setAdminMessage(`Wiederherstellen fehlgeschlagen: ${error.message}`, "warning");
    return;
  }

  setAdminMessage("Treffer wurde wiederhergestellt.", "success");
  await loadHiddenRecords();
}

async function sendMagicLink() {
  const email = document.getElementById("auth-email").value.trim();

  if (!email) {
    setAdminMessage("Bitte eine E-Mail-Adresse eingeben.", "warning");
    return;
  }

  const { error } = await supabaseClient.auth.signInWithOtp({
    email,
    options: {
      emailRedirectTo: window.location.href.split("#")[0]
    }
  });

  if (error) {
    setAdminMessage(`Magic Link konnte nicht gesendet werden: ${error.message}`, "warning");
    return;
  }

  setAdminMessage("Magic Link versendet. Bitte E-Mail öffnen und den Link bestätigen.", "success");
}

async function signOut() {
  const { error } = await supabaseClient.auth.signOut();

  if (error) {
    setAdminMessage(`Abmelden fehlgeschlagen: ${error.message}`, "warning");
    return;
  }

  setAdminMessage("Abgemeldet.", "neutral");
}

function registerTableActions() {
  document.getElementById("table-body").addEventListener("click", async (event) => {
    const button = event.target.closest("button[data-action]");
    if (!button) {
      return;
    }

    const recordKey = button.dataset.recordKey;
    const action = button.dataset.action;

    if (action === "hide") {
      await hideRecord(recordKey);
    } else if (action === "restore") {
      await restoreRecord(recordKey);
    }
  });
}

function registerFilters() {
  ["f-portal", "f-such", "f-frist", "f-review"].forEach((id) => {
    document.getElementById(id).addEventListener("change", updateTable);
  });
  document.getElementById("f-search").addEventListener("input", updateTable);
  document.getElementById("toggle-show-hidden").addEventListener("change", updateTable);
  document.getElementById("review-filter-pills").addEventListener("click", (event) => {
    const button = event.target.closest("button.review-pill");
    if (!button) {
      return;
    }

    document.getElementById("f-review").value = button.dataset.value || "";
    updateTable();
  });
}

function registerSorting() {
  document.querySelectorAll("thead th[data-col]").forEach((header) => {
    header.addEventListener("click", () => {
      const column = header.dataset.col;
      if (sortCol === column) {
        sortDir *= -1;
      } else {
        sortCol = column;
        sortDir = 1;
      }

      document.querySelectorAll("thead th .sort-icon").forEach((icon) => {
        icon.textContent = "↕";
      });
      header.querySelector(".sort-icon").textContent = sortDir === 1 ? "↑" : "↓";
      updateTable();
    });
  });
}

async function initSupabase() {
  if (!SUPABASE_ENABLED) {
    updateAuthUi();
    refreshOverview();
    return;
  }

  supabaseClient = window.supabase.createClient(
    DASHBOARD_CONFIG.supabaseUrl,
    DASHBOARD_CONFIG.supabaseAnonKey
  );
  const {
    data: { session }
  } = await supabaseClient.auth.getSession();
  currentSession = session;
  updateAuthUi();

  supabaseClient.auth.onAuthStateChange((_event, sessionValue) => {
    currentSession = sessionValue;
    updateAuthUi();
    updateTable();
  });

  document.getElementById("auth-send-link").addEventListener("click", sendMagicLink);
  document.getElementById("auth-signout").addEventListener("click", signOut);

  await loadHiddenRecords();
}

async function main() {
  const initialData = typeof DATA !== "undefined" ? DATA : window.DATA || [];

  dataRecords = initialData
    .map((record) => normalizeRecord(record))
    .filter((record) => !isExpiredDeadline(record.frist));

  registerSorting();
  registerFilters();
  registerTableActions();
  updateAuthUi();
  await initSupabase();
}

main().catch((error) => {
  console.error(error);
  setAdminMessage(`Dashboard-Initialisierung fehlgeschlagen: ${error.message}`, "warning");
  refreshOverview();
});
