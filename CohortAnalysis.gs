// ─────────────────────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────────────────────

const SPREADSHEET_ID = "1Vvt1nNZymnl5uczAmKrdbJOAarLZBL7quRo6-Wo0SfE";

const SHEET_EVENTS  = "raw_events";
const SHEET_COHORTS = "cohort_summary";
const SHEET_FUNNEL  = "funnel_summary";

// MUST match backend normalizeEvent()
const EVENT_HEADERS = [
  "timestamp", "cohort_date", "user_id", "session_id",
  "event_type", "buyer_type", "monthly_budget",
  "safe_emi_min", "safe_emi_max",
  "car_id", "car_name", "fit_status", "extra_emi",
  "delivery_hours", "trust_score", "dealer_reliability",
  "cta", "page", "source"
];

// ─────────────────────────────────────────────────────────────
// CORE HELPERS
// ─────────────────────────────────────────────────────────────

function getSpreadsheet() {
  return SpreadsheetApp.openById(SPREADSHEET_ID);
}

function getOrCreateSheet(ss, name, headers) {
  let sheet = ss.getSheetByName(name);

  if (!sheet) {
    sheet = ss.insertSheet(name);
    sheet.appendRow(headers);

    sheet.getRange(1, 1, 1, headers.length)
      .setFontWeight("bold")
      .setBackground("#1a1a2e")
      .setFontColor("#ffffff");
  }

  return sheet;
}

// ─────────────────────────────────────────────────────────────
// MAIN WEBHOOK
// ─────────────────────────────────────────────────────────────

function doPost(e) {
  try {
    const raw = JSON.parse(e.postData.contents);

    const ss = getSpreadsheet();

    const eventsSheet = getOrCreateSheet(ss, SHEET_EVENTS, EVENT_HEADERS);

    const row = EVENT_HEADERS.map(h => raw[h] ?? "");
    eventsSheet.appendRow(row);

    // rebuild summaries
    rebuildCohortSummary(ss);
    rebuildFunnelSummary(ss);

    return ContentService
      .createTextOutput(JSON.stringify({ ok: true }))
      .setMimeType(ContentService.MimeType.JSON);

  } catch (err) {
    console.error("doPost error:", err);

    return ContentService
      .createTextOutput(JSON.stringify({
        ok: false,
        error: err.message
      }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

// ─────────────────────────────────────────────────────────────
// COHORT SUMMARY (REAL LOGIC)
// ─────────────────────────────────────────────────────────────

function rebuildCohortSummary(ss) {
  const sheet = ss.getSheetByName(SHEET_EVENTS);
  if (!sheet) return;

  const data = sheet.getDataRange().getValues();
  if (data.length < 2) return;

  const headers = data[0];

  const rows = data.slice(1).map(r => {
    const obj = {};
    headers.forEach((h, i) => obj[h] = r[i]);
    return obj;
  });

  // user → first seen date
  const userFirstSeen = {};

  rows.forEach(r => {
    const uid = r.user_id;
    const date = r.cohort_date || String(r.timestamp).slice(0, 10);

    if (!userFirstSeen[uid] || date < userFirstSeen[uid]) {
      userFirstSeen[uid] = date;
    }
  });

  const cohorts = {};

  rows.forEach(r => {
    const cohortDate = userFirstSeen[r.user_id];

    if (!cohorts[cohortDate]) {
      cohorts[cohortDate] = {
        cohort_date: cohortDate,
        users: new Set(),
        page_views: 0,
        eligibility_checks: 0,
        car_clicks: 0,
        dealer_gates: 0,
        first_time: 0,
        urgent: 0,
        emi_sensitive: 0,
        returning: 0
      };
    }

    const c = cohorts[cohortDate];

    c.users.add(r.user_id);

    if (r.event_type === "page_view") c.page_views++;
    if (r.event_type === "eligibility_check") c.eligibility_checks++;
    if (r.event_type === "car_click") c.car_clicks++;
    if (r.event_type === "dealer_gate") c.dealer_gates++;

    if (r.buyer_type === "first_time") c.first_time++;
    if (r.buyer_type === "urgent") c.urgent++;
    if (r.buyer_type === "emi_sensitive") c.emi_sensitive++;
    if (r.buyer_type === "returning_high_intent") c.returning++;
  });

  const headersOut = [
    "cohort_date", "unique_users",
    "page_views", "eligibility_checks", "car_clicks", "dealer_gates",
    "eligibility_rate_%", "dealer_conversion_rate_%",
    "first_time_buyers", "urgent_buyers", "emi_sensitive", "returning_buyers"
  ];

  const sheetOut = getOrCreateSheet(ss, SHEET_COHORTS, headersOut);

  if (sheetOut.getLastRow() > 1) {
    sheetOut.getRange(2, 1, sheetOut.getLastRow() - 1, headersOut.length).clearContent();
  }

  Object.values(cohorts).forEach(c => {
    const eligRate = c.page_views > 0
      ? (c.eligibility_checks / c.page_views * 100).toFixed(1)
      : 0;

    const dealerRate = c.eligibility_checks > 0
      ? (c.dealer_gates / c.eligibility_checks * 100).toFixed(1)
      : 0;

    sheetOut.appendRow([
      c.cohort_date,
      c.users.size,
      c.page_views,
      c.eligibility_checks,
      c.car_clicks,
      c.dealer_gates,
      eligRate,
      dealerRate,
      c.first_time,
      c.urgent,
      c.emi_sensitive,
      c.returning
    ]);
  });
}

// ─────────────────────────────────────────────────────────────
// FUNNEL SUMMARY
// ─────────────────────────────────────────────────────────────

function rebuildFunnelSummary(ss) {
  const sheet = ss.getSheetByName(SHEET_EVENTS);
  if (!sheet) return;

  const data = sheet.getDataRange().getValues();
  if (data.length < 2) return;

  const headers = data[0];
  const idx = headers.indexOf("event_type");

  const counts = {
    page_view: 0,
    car_click: 0,
    eligibility_check: 0,
    dealer_gate: 0
  };

  data.slice(1).forEach(r => {
    const type = r[idx];
    if (counts[type] !== undefined) counts[type]++;
  });

  const funnelHeaders = ["stage", "event_count", "drop_off_%"];

  const sheetOut = getOrCreateSheet(ss, SHEET_FUNNEL, funnelHeaders);

  if (sheetOut.getLastRow() > 1) {
    sheetOut.getRange(2, 1, sheetOut.getLastRow() - 1, 3).clearContent();
  }

  const stages = [
    ["Page View", counts.page_view],
    ["Car Click", counts.car_click],
    ["Eligibility Check", counts.eligibility_check],
    ["Dealer Gate", counts.dealer_gate]
  ];

  stages.forEach(([stage, count], i) => {
    const prev = i === 0 ? count : stages[i - 1][1];

    const drop = prev > 0
      ? ((prev - count) / prev * 100).toFixed(1)
      : 0;

    sheetOut.appendRow([stage, count, drop]);
  });
}

// ─────────────────────────────────────────────────────────────
// MANUAL TEST
// ─────────────────────────────────────────────────────────────

function testRebuild() {
  const ss = getSpreadsheet();
  rebuildCohortSummary(ss);
  rebuildFunnelSummary(ss);
}
