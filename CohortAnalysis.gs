// ─── CohortAnalysis.gs ────────────────────────────────────────────────────────
// Google Apps Script webhook that receives normalized events from the backend.
// Schema matches server.js normalizeEvent() — the backend is the single source of truth.
//
// DEPLOY: Extensions > Apps Script > Deploy > New Deployment > Web App
//         Execute as: Me | Who has access: Anyone
// Then paste the Web App URL into Render env: GOOGLE_SHEETS_WEBHOOK_URL

const SHEET_EVENTS  = "raw_events";
const SHEET_COHORTS = "cohort_summary";
const SHEET_FUNNEL  = "funnel_summary";

// Schema: must match normalizeEvent() in server.js exactly
const EVENT_HEADERS = [
  "timestamp", "cohort_date", "user_id", "session_id",
  "event_type", "buyer_type", "monthly_budget",
  "safe_emi_min", "safe_emi_max",
  "car_id", "car_name", "fit_status", "extra_emi",
  "delivery_hours", "trust_score", "dealer_reliability",
  "cta", "page", "source"
];

// ─── Main webhook entry point ─────────────────────────────────────────────────
function doPost(e) {
  try {
    const raw = JSON.parse(e.postData.contents);
    const ss = SpreadsheetApp.getActiveSpreadsheet();

    // Write to raw_events
    const eventsSheet = getOrCreateSheet(ss, SHEET_EVENTS, EVENT_HEADERS);
    const row = EVENT_HEADERS.map(h => raw[h] ?? "");
    eventsSheet.appendRow(row);

    // Rebuild summaries
    rebuildCohortSummary(ss);
    rebuildFunnelSummary(ss);

    return ContentService
      .createTextOutput(JSON.stringify({ ok: true, event: raw.event_type }))
      .setMimeType(ContentService.MimeType.JSON);

  } catch (err) {
    console.error("doPost error:", err);
    return ContentService
      .createTextOutput(JSON.stringify({ ok: false, error: err.message }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

// ─── Cohort Summary: group by first_seen_date ─────────────────────────────────
function rebuildCohortSummary(ss) {
  const eventsSheet = ss.getSheetByName(SHEET_EVENTS);
  if (!eventsSheet) return;

  const data = eventsSheet.getDataRange().getValues();
  if (data.length < 2) return;

  const headers = data[0];
  const rows = data.slice(1).map(r => {
    const obj = {};
    headers.forEach((h, i) => obj[h] = r[i]);
    return obj;
  });

  // Build user → first seen date map
  const userFirstSeen = {};
  rows.forEach(r => {
    const uid = r.user_id;
    const date = r.cohort_date || String(r.timestamp).slice(0, 10);
    if (!userFirstSeen[uid] || date < userFirstSeen[uid]) {
      userFirstSeen[uid] = date;
    }
  });

  // Aggregate per cohort
  const cohorts = {};
  rows.forEach(r => {
    const cohortDate = userFirstSeen[r.user_id] || r.cohort_date;
    if (!cohorts[cohortDate]) cohorts[cohortDate] = {
      cohort_date: cohortDate,
      users: new Set(),
      page_views: 0, eligibility_checks: 0, car_clicks: 0, dealer_gates: 0,
      first_time: 0, urgent: 0, emi_sensitive: 0, returning: 0
    };
    const c = cohorts[cohortDate];
    c.users.add(r.user_id);
    if (r.event_type === "page_view")         c.page_views++;
    if (r.event_type === "eligibility_check") c.eligibility_checks++;
    if (r.event_type === "car_click")         c.car_clicks++;
    if (r.event_type === "dealer_gate")       c.dealer_gates++;
    if (r.buyer_type === "first_time")           c.first_time++;
    if (r.buyer_type === "urgent")               c.urgent++;
    if (r.buyer_type === "emi_sensitive")         c.emi_sensitive++;
    if (r.buyer_type === "returning_high_intent") c.returning++;
  });

  const cohortHeaders = [
    "cohort_date", "unique_users",
    "page_views", "eligibility_checks", "car_clicks", "dealer_gates",
    "eligibility_rate_%", "dealer_conversion_rate_%",
    "first_time_buyers", "urgent_buyers", "emi_sensitive", "returning_buyers"
  ];

  const cohortSheet = getOrCreateSheet(ss, SHEET_COHORTS, cohortHeaders);
  // Clear old data (keep header)
  if (cohortSheet.getLastRow() > 1) {
    cohortSheet.getRange(2, 1, cohortSheet.getLastRow() - 1, cohortHeaders.length).clearContent();
  }

  Object.values(cohorts).sort((a, b) => a.cohort_date.localeCompare(b.cohort_date)).forEach(c => {
    const eligRate = c.page_views > 0 ? (c.eligibility_checks / c.page_views * 100).toFixed(1) : 0;
    const dealerRate = c.eligibility_checks > 0 ? (c.dealer_gates / c.eligibility_checks * 100).toFixed(1) : 0;
    cohortSheet.appendRow([
      c.cohort_date, c.users.size,
      c.page_views, c.eligibility_checks, c.car_clicks, c.dealer_gates,
      eligRate, dealerRate,
      c.first_time, c.urgent, c.emi_sensitive, c.returning
    ]);
  });
}

// ─── Funnel Summary: overall conversion stages ────────────────────────────────
function rebuildFunnelSummary(ss) {
  const eventsSheet = ss.getSheetByName(SHEET_EVENTS);
  if (!eventsSheet) return;

  const data = eventsSheet.getDataRange().getValues();
  if (data.length < 2) return;

  const headers = data[0];
  const etIdx = headers.indexOf("event_type");
  const rows = data.slice(1);

  const counts = { page_view: 0, car_click: 0, eligibility_check: 0, dealer_gate: 0 };
  rows.forEach(r => { if (counts[r[etIdx]] !== undefined) counts[r[etIdx]]++; });

  const funnelHeaders = ["stage", "event_count", "drop_off_%"];
  const funnelSheet = getOrCreateSheet(ss, SHEET_FUNNEL, funnelHeaders);
  if (funnelSheet.getLastRow() > 1) {
    funnelSheet.getRange(2, 1, funnelSheet.getLastRow() - 1, 3).clearContent();
  }

  const stages = [
    ["1. Page View",        counts.page_view],
    ["2. Car Click",        counts.car_click],
    ["3. Eligibility Check",counts.eligibility_check],
    ["4. Dealer Gate",      counts.dealer_gate]
  ];

  stages.forEach(([stage, count], i) => {
    const prev = i === 0 ? count : stages[i-1][1];
    const dropOff = prev > 0 ? ((prev - count) / prev * 100).toFixed(1) : 0;
    funnelSheet.appendRow([stage, count, dropOff]);
  });
}

// ─── Sheet helper ─────────────────────────────────────────────────────────────
function getOrCreateSheet(ss, name, headers) {
  let sheet = ss.getSheetByName(name);
  if (!sheet) {
    sheet = ss.insertSheet(name);
    sheet.appendRow(headers);
    sheet.getRange(1, 1, 1, headers.length).setFontWeight("bold").setBackground("#1a1a2e").setFontColor("#ffffff");
  }
  return sheet;
}

// ─── Manual trigger (run from Apps Script editor to test) ─────────────────────
function testRebuild() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  rebuildCohortSummary(ss);
  rebuildFunnelSummary(ss);
  console.log("Rebuild complete.");
}