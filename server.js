const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const PORT = Number(process.env.PORT || 5050);
const DATA_DIR = path.join(__dirname, "data");
const EVENTS_FILE = path.join(DATA_DIR, "cohort_events.jsonl");

// ─── Google Sheets Webhook (set in environment) ───────────────────────────────
const SHEETS_WEBHOOK_URL = process.env.GOOGLE_SHEETS_WEBHOOK_URL || "";

fs.mkdirSync(DATA_DIR, { recursive: true });

// ─── Inventory ────────────────────────────────────────────────────────────────
const inventory = [
  {
    id: "cd-i20-2019",
    name: "Hyundai i20 Sportz",
    year: 2019,
    fuel: "Petrol",
    transmission: "Manual",
    km: 32000,
    price: 510000,
    baseEmi: 7800,
    downPaymentEmi: 6900,
    downPayment: 50000,
    city: "Delhi NCR",
    deliveryHours: 48,
    trustScore: 91,
    dealerReliability: 88,
    vahanLastCheckedHours: 2,
    accidentHistory: "none",
    ownerCount: 1,
    insurancePack: 4999,
    warrantySavings: 12000,
    intentTags: ["first_time", "emi_sensitive", "urgent", "best_value"]
  },
  {
    id: "cd-swift-2020",
    name: "Maruti Swift VXI",
    year: 2020,
    fuel: "Petrol",
    transmission: "Manual",
    km: 28500,
    price: 455000,
    baseEmi: 6950,
    downPaymentEmi: 6250,
    downPayment: 45000,
    city: "Delhi NCR",
    deliveryHours: 48,
    trustScore: 86,
    dealerReliability: 91,
    vahanLastCheckedHours: 4,
    accidentHistory: "none",
    ownerCount: 1,
    insurancePack: 4999,
    warrantySavings: 9000,
    intentTags: ["urgent", "emi_sensitive", "lowest_emi"]
  },
  {
    id: "cd-nexon-2021",
    name: "Tata Nexon XZ+",
    year: 2021,
    fuel: "Petrol",
    transmission: "Manual",
    km: 41000,
    price: 635000,
    baseEmi: 9400,
    downPaymentEmi: 8300,
    downPayment: 70000,
    city: "Delhi NCR",
    deliveryHours: 96,
    trustScore: 82,
    dealerReliability: 79,
    vahanLastCheckedHours: 8,
    accidentHistory: "none",
    ownerCount: 2,
    insurancePack: 5499,
    warrantySavings: 15000,
    intentTags: ["upgrade", "stretch", "suv"]
  },
  {
    id: "cd-city-2018",
    name: "Honda City VX",
    year: 2018,
    fuel: "Petrol",
    transmission: "Automatic",
    km: 39800,
    price: 585000,
    baseEmi: 8700,
    downPaymentEmi: 7650,
    downPayment: 60000,
    city: "Delhi NCR",
    deliveryHours: 72,
    trustScore: 84,
    dealerReliability: 83,
    vahanLastCheckedHours: 6,
    accidentHistory: "none",
    ownerCount: 1,
    insurancePack: 4999,
    warrantySavings: 11000,
    intentTags: ["automatic", "stretch", "upgrade"]
  }
];

// ─── Helpers ──────────────────────────────────────────────────────────────────
function send(res, status, data) {
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type"
  });
  res.end(JSON.stringify(data, null, 2));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", chunk => {
      body += chunk;
      if (body.length > 1_000_000) reject(new Error("Body too large"));
    });
    req.on("end", () => {
      if (!body) return resolve({});
      try { resolve(JSON.parse(body)); }
      catch { reject(new Error("Invalid JSON")); }
    });
  });
}

function pmtLoanAmount(emi, annualRate = 0.105, months = 72) {
  const r = annualRate / 12;
  return Math.round(emi * ((1 - Math.pow(1 + r, -months)) / r));
}

function emiForLoan(principal, annualRate = 0.105, months = 72) {
  const r = annualRate / 12;
  return Math.round((principal * r * Math.pow(1 + r, months)) / (Math.pow(1 + r, months) - 1));
}

// ─── Business Engines ─────────────────────────────────────────────────────────
function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function bureauScoreFromInput(input) {
  const explicit = Number(input.bureauScore || input.creditScore || input.cibilScore);
  if (explicit >= 300 && explicit <= 900) return explicit;

  const range = String(input.creditRange || "650-750");
  if (range.includes("750")) return 775;
  if (range.includes("650")) return 700;
  if (range.includes("550")) return 610;
  return 680;
}

function riskProfileEngine(input = {}) {
  const income = Math.max(0, Number(input.monthlyIncome || 55000));
  const existingEmis = Math.max(0, Number(input.existingEmis || 0));
  const requestedBudget = Math.max(3500, Number(input.monthlyBudget || 7800));
  const bureauScore = bureauScoreFromInput(input);
  const age = clamp(Number(input.age || 31), 21, 65);
  const jobStabilityMonths = clamp(Number(input.jobStabilityMonths || input.jobTenureMonths || 24), 0, 240);
  const employerCategory = input.employerCategory || input.employmentType || "salaried";
  const city = input.city || "Delhi NCR";
  const cityRisk = /tier\s*2|jaipur|lucknow|indore|bhopal|patna|kanpur/i.test(city) ? 0.95 : 1;

  const bureauFactor = clamp((bureauScore - 600) / 250, 0.35, 1.18);
  const ageFactor = age < 24 ? 0.88 : age > 55 ? 0.9 : 1;
  const stabilityFactor = jobStabilityMonths < 6 ? 0.82 : jobStabilityMonths < 18 ? 0.94 : 1.04;
  const employerFactor = /gov|psu|mnc|listed/i.test(employerCategory) ? 1.06 : /self|gig|freelance/i.test(employerCategory) ? 0.9 : 1;
  const obligationRatio = income > 0 ? existingEmis / income : 1;
  const riskScore = clamp(
    Math.round((bureauFactor * 42 + stabilityFactor * 20 + employerFactor * 16 + ageFactor * 10 + cityRisk * 12 - obligationRatio * 35) * 10),
    300,
    900
  );

  const foirLimit = riskScore >= 760 ? 0.46 : riskScore >= 700 ? 0.4 : riskScore >= 640 ? 0.34 : 0.28;
  const disposableEmi = Math.max(2500, Math.round(income * foirLimit - existingEmis));
  const riskCategory = riskScore >= 760 ? "LOW" : riskScore >= 690 ? "MEDIUM" : "HIGH";

  return {
    city,
    income,
    existingEmis,
    requestedBudget,
    bureauScore,
    age,
    jobStabilityMonths,
    employerCategory,
    foirLimit,
    disposableEmi,
    riskScore,
    riskCategory,
    signals: {
      bureau: bureauScore >= 740 ? "strong" : bureauScore >= 680 ? "moderate" : "thin_or_risky",
      stability: jobStabilityMonths >= 18 ? "stable" : "needs_more_proof",
      obligations: obligationRatio <= 0.18 ? "low" : obligationRatio <= 0.32 ? "manageable" : "high",
      cityRisk: cityRisk === 1 ? "standard" : "watch"
    }
  };
}

const lenderModels = [
  { lender: "Rupyy Prime NBFC", appetite: 1.02, baseRate: 10.4, tenureMonths: 72, minRisk: 610, riskBuffer: 0.03 },
  { lender: "Partner Bank Select", appetite: 0.94, baseRate: 10.9, tenureMonths: 72, minRisk: 675, riskBuffer: 0.08 },
  { lender: "Fast Approval NBFC", appetite: 0.86, baseRate: 12.2, tenureMonths: 60, minRisk: 585, riskBuffer: 0.13 },
  { lender: "Used Car Credit Co", appetite: 0.78, baseRate: 13.5, tenureMonths: 60, minRisk: 560, riskBuffer: 0.18 }
];

function probabilityForAmount(amount, maxAmount, riskScore, lenderBuffer = 0) {
  if (amount <= 0 || maxAmount <= 0) return 5;
  const utilization = amount / maxAmount;
  const riskBoost = (riskScore - 650) / 5;
  return clamp(Math.round(96 - utilization * 42 + riskBoost - lenderBuffer * 100), 12, 96);
}

function lenderSimulationEngine(input, profile = riskProfileEngine(input)) {
  return lenderModels.map(model => {
    const riskHaircut = clamp((profile.riskScore - model.minRisk) / 650, -0.22, 0.18);
    const buyerAnchoredEmi = Math.min(profile.disposableEmi, profile.requestedBudget * 1.35);
    const approvalEmi = Math.max(2500, Math.round(buyerAnchoredEmi * model.appetite * (1 + riskHaircut)));
    const interestRate = Number((model.baseRate + clamp((700 - profile.riskScore) / 80, -0.7, 2.4)).toFixed(2));
    const approvedAmount = pmtLoanAmount(approvalEmi, interestRate / 100, model.tenureMonths);
    const approvalProbability = probabilityForAmount(approvedAmount, approvedAmount * (1 + model.riskBuffer), profile.riskScore, model.riskBuffer);
    return {
      lender: model.lender,
      interestRate,
      tenureMonths: model.tenureMonths,
      approvedAmount,
      emi: approvalEmi,
      approvalProbability,
      decision: profile.riskScore >= model.minRisk ? "LIKELY" : "CONDITIONAL",
      reason: profile.riskScore >= model.minRisk ? "Profile fits lender policy" : "Needs stronger bureau or income proof",
      softCheck: true
    };
  }).sort((a, b) => b.approvalProbability - a.approvalProbability);
}

function affordabilityEngine(input) {
  const profile = riskProfileEngine(input);
  const offers = lenderSimulationEngine(input, profile);
  const amounts = offers.map(offer => offer.approvedAmount).sort((a, b) => a - b);
  const emis = offers.map(offer => offer.emi).sort((a, b) => a - b);
  const worstCase = amounts[0] || 0;
  const bestCase = amounts[amounts.length - 1] || 0;
  const safeMax = Math.min(profile.requestedBudget + 600, emis[emis.length - 1] || profile.requestedBudget);
  const safeMin = Math.max(3000, Math.min(safeMax - 500, emis[0] || 3500));
  const targetAmount = pmtLoanAmount(profile.requestedBudget);
  const approvalProbability = probabilityForAmount(targetAmount, bestCase, profile.riskScore);
  const confidenceScore = clamp(
    Math.round(58 + offers.filter(o => o.decision === "LIKELY").length * 8 + (profile.bureauScore - 650) / 8 + Math.min(profile.jobStabilityMonths, 36) / 3),
    35,
    92
  );
  const confidence = confidenceScore >= 76 ? "HIGH" : confidenceScore >= 58 ? "MEDIUM" : "LOW";
  const tokenSeed = `${input.userId || "anon"}:${profile.riskScore}:${bestCase}:${Date.now()}`;
  const lockToken = `CD-${crypto.createHash("sha1").update(tokenSeed).digest("hex").slice(0, 8).toUpperCase()}`;

  return {
    userId: input.userId || crypto.randomUUID(),
    city: profile.city,
    monthlyIncome: profile.income,
    existingEmis: profile.existingEmis,
    requestedBudget: profile.requestedBudget,
    bureauScore: profile.bureauScore,
    riskProfile: profile,
    safeEmiRange: { min: safeMin, max: safeMax },
    loanRange: { min: worstCase, max: bestCase },
    maxLoan: bestCase,
    bestCase,
    worstCase,
    approvalProbability,
    confidence,
    confidenceScore,
    riskCategory: profile.riskCategory,
    offers,
    lockToken,
    tokenValidHours: 24,
    message: `Estimated approval range: Rs ${worstCase.toLocaleString("en-IN")}-${bestCase.toLocaleString("en-IN")} with ${confidence.toLowerCase()} confidence.`
  };
}

function pricingEngine(car) {
  const rto = Math.round(car.price * 0.035);
  const insurance = car.price < 500000 ? 11000 : 12000;
  const processing = 3500;
  return {
    car: car.price, rto, insurance, processing,
    totalCost: car.price + rto + insurance + processing,
    emi: car.baseEmi,
    downPaymentEmi: car.downPaymentEmi,
    insuranceWarrantyPack: car.insurancePack
  };
}

function trustEngine(car) {
  const verified = car.vahanLastCheckedHours <= 12 && car.accidentHistory === "none";
  return {
    score: car.trustScore,
    label: car.trustScore >= 88 ? "High confidence" : car.trustScore >= 80 ? "Verified" : "Needs review",
    rcStatus: verified ? "Verified via Vahan" : "Manual review required",
    lastChecked: `${car.vahanLastCheckedHours} hrs ago`,
    chargeProtectionEligible: car.trustScore >= 85 && car.dealerReliability >= 85,
    dealerReliability: car.dealerReliability
  };
}

function intentEngine(input = {}) {
  const visits = Number(input.visits || 1);
  const budgetChanges = Number(input.budgetChanges || 0);
  const selectedSegment = input.segment || "first_time";
  const needsFastDelivery = selectedSegment === "urgent" || input.urgent === true;
  const buyerType = needsFastDelivery
    ? "urgent"
    : visits >= 2 ? "returning_high_intent"
    : budgetChanges >= 2 ? "emi_sensitive"
    : selectedSegment;

  return {
    buyerType,
    priority: needsFastDelivery ? "delivery_speed" : buyerType === "emi_sensitive" ? "lowest_emi" : "best_fit",
    message: buyerType === "returning_high_intent"
      ? "Returning buyer: highlight price drops and saved cars."
      : buyerType === "urgent" ? "Urgent buyer: prioritize 48-hour delivery inventory."
      : "First-time buyer: prioritize verification and cost clarity."
  };
}

function fitForCar(car, affordability) {
  const max = affordability.safeEmiRange.max;
  if (car.baseEmi <= max) return { status: "FIT", extraEmi: 0, label: "Fits your budget" };
  const extra = car.baseEmi - max;
  if (extra <= 1500) return { status: "STRETCH", extraEmi: extra, label: `Stretch: +Rs ${extra.toLocaleString("en-IN")}/month` };
  return { status: "OVER", extraEmi: extra, label: `Over budget: +Rs ${extra.toLocaleString("en-IN")}/month` };
}

function inventoryMatchingEngine(input) {
  const affordability = input.affordability || affordabilityEngine(input);
  const intent = intentEngine(input.intent || {});
  const matched = inventory.map(car => {
    const fit = fitForCar(car, affordability);
    const pricing = pricingEngine(car);
    const trust = trustEngine(car);
    let rank = 100 - Math.abs(car.baseEmi - affordability.requestedBudget) / 100 + trust.score / 10;
    if (intent.priority === "delivery_speed" && car.deliveryHours <= 48) rank += 20;
    if (fit.status === "FIT") rank += 30;
    if (fit.status === "OVER") rank -= 30;
    return { ...car, fit, pricing, trust, rank: Math.round(rank) };
  }).sort((a, b) => b.rank - a.rank);

  return { affordability, intent, cars: matched };
}

function loanOrchestrationEngine(input) {
  const affordability = affordabilityEngine(input);
  return {
    affordability,
    offers: affordability.offers,
    approvalCurve: [
      { amount: affordability.bestCase, probability: affordability.approvalProbability },
      { amount: Math.round(affordability.bestCase * 0.9), probability: clamp(affordability.approvalProbability + 10, 1, 96) },
      { amount: Math.round(affordability.bestCase * 0.8), probability: clamp(affordability.approvalProbability + 18, 1, 98) }
    ],
    capture: {
      lockToken: affordability.lockToken,
      validHours: affordability.tokenValidHours,
      nextStep: "Lock rate and route buyer to matching inventory before dealer contact"
    }
  };
}

function swapEngine(input) {
  const oldCarValue = Number(input.oldCarValue || 300000);
  const targetCarPrice = Number(input.targetCarPrice || 600000);
  const loanNeeded = Math.max(0, targetCarPrice - oldCarValue);
  return {
    oldCarValue, targetCarPrice, loanNeeded,
    netEmi: emiForLoan(loanNeeded),
    executionPromise: [
      "Pick old car on delivery day",
      "Transfer RC",
      "Activate insurance",
      "Avoid double EMI"
    ]
  };
}

// ─── Event Schema (Single Source of Truth) ───────────────────────────────────
// ALL event normalization happens HERE. Frontend and Sheets both obey this shape.
function normalizeEvent(raw) {
  return {
    timestamp: new Date().toISOString(),
    cohort_date: new Date().toISOString().slice(0, 10), // YYYY-MM-DD for cohort grouping
    user_id: raw.userId || raw.user_id || "anonymous",
    session_id: raw.sessionId || raw.session_id || "unknown",
    event_type: raw.eventType || raw.event_type || "unknown",
    buyer_type: raw.buyerType || raw.buyer_type || "",
    monthly_budget: raw.monthlyBudget || raw.monthly_budget || "",
    safe_emi_min: raw.safeEmiMin || raw.safe_emi_min || "",
    safe_emi_max: raw.safeEmiMax || raw.safe_emi_max || "",
    approval_probability: raw.approvalProbability || raw.approval_probability || "",
    confidence: raw.confidence || "",
    lock_token: raw.lockToken || raw.lock_token || "",
    car_id: raw.carId || raw.car_id || "",
    car_name: raw.carName || raw.car_name || "",
    fit_status: raw.fitStatus || raw.fit_status || "",
    extra_emi: raw.extraEmi || raw.extra_emi || "",
    delivery_hours: raw.deliveryHours || raw.delivery_hours || "",
    trust_score: raw.trustScore || raw.trust_score || "",
    dealer_reliability: raw.dealerReliability || raw.dealer_reliability || "",
    cta: raw.cta || "",
    page: raw.page || "",
    source: "render_backend"
  };
}

// ─── Local JSONL Storage ──────────────────────────────────────────────────────
function appendEventLocal(normalized) {
  fs.appendFileSync(EVENTS_FILE, JSON.stringify(normalized) + "\n", "utf8");
}

function readEvents() {
  if (!fs.existsSync(EVENTS_FILE)) return [];
  return fs.readFileSync(EVENTS_FILE, "utf8")
    .split(/\r?\n/).filter(Boolean)
    .map(line => { try { return JSON.parse(line); } catch { return null; } })
    .filter(Boolean);
}

// ─── Google Sheets Webhook (with retry) ──────────────────────────────────────
async function sendToSheets(normalized, retries = 3) {
  if (!SHEETS_WEBHOOK_URL) return; // No webhook configured — skip silently

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(SHEETS_WEBHOOK_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(normalized),
        signal: AbortSignal.timeout(5000) // 5s timeout
      });
      if (res.ok) {
        console.log(`[Sheets] Event sent: ${normalized.event_type}`);
        return;
      }
      console.warn(`[Sheets] Attempt ${attempt} failed: HTTP ${res.status}`);
    } catch (err) {
      console.warn(`[Sheets] Attempt ${attempt} error: ${err.message}`);
    }
    if (attempt < retries) await new Promise(r => setTimeout(r, 500 * attempt)); // backoff
  }
  console.error(`[Sheets] All ${retries} attempts failed for event: ${normalized.event_type}. Saved locally only.`);
}

// ─── Unified Event Handler ────────────────────────────────────────────────────
async function handleEvent(raw) {
  const normalized = normalizeEvent(raw);
  appendEventLocal(normalized);              // Always write locally first
  await sendToSheets(normalized);            // Then try Sheets (non-blocking failure)
  return normalized;
}

// ─── Cohort Analytics ─────────────────────────────────────────────────────────
function buildCohortSummary(events) {
  // Real cohort = group by FIRST SEEN DATE, then track behavior over time
  const userFirstSeen = {};
  events.forEach(e => {
    if (!userFirstSeen[e.user_id] || e.timestamp < userFirstSeen[e.user_id]) {
      userFirstSeen[e.user_id] = e.cohort_date || e.timestamp?.slice(0, 10);
    }
  });

  // Funnel counts per cohort_date
  const cohorts = {};
  events.forEach(e => {
    const cohortDate = userFirstSeen[e.user_id] || e.cohort_date;
    if (!cohorts[cohortDate]) cohorts[cohortDate] = {
      cohort_date: cohortDate,
      users: new Set(),
      page_views: 0,
      eligibility_checks: 0,
      car_clicks: 0,
      dealer_gates: 0,
      buyer_types: {},
      fit_statuses: {}
    };
    const c = cohorts[cohortDate];
    c.users.add(e.user_id);

    // Funnel stages
    if (e.event_type === "page_view") c.page_views++;
    if (e.event_type === "eligibility_check") c.eligibility_checks++;
    if (e.event_type === "car_click") c.car_clicks++;
    if (e.event_type === "dealer_gate") c.dealer_gates++;

    // Segmentation
    if (e.buyer_type) c.buyer_types[e.buyer_type] = (c.buyer_types[e.buyer_type] || 0) + 1;
    if (e.fit_status) c.fit_statuses[e.fit_status] = (c.fit_statuses[e.fit_status] || 0) + 1;
  });

  return Object.values(cohorts).map(c => ({
    ...c,
    unique_users: c.users.size,
    eligibility_rate: c.page_views > 0 ? (c.eligibility_checks / c.page_views * 100).toFixed(1) + "%" : "N/A",
    dealer_conversion_rate: c.eligibility_checks > 0 ? (c.dealer_gates / c.eligibility_checks * 100).toFixed(1) + "%" : "N/A",
    users: undefined // remove Set from JSON output
  }));
}

// ─── Router ───────────────────────────────────────────────────────────────────
async function route(req, res) {
  if (req.method === "OPTIONS") return send(res, 200, { ok: true });

  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
  const pathname = url.pathname;

  try {
    if (req.method === "GET" && pathname === "/api/health") {
      return send(res, 200, {
        ok: true,
        service: "cardekho-finance-first-backend",
        port: PORT,
        sheetsConnected: !!SHEETS_WEBHOOK_URL,
        timestamp: new Date().toISOString()
      });
    }

    if (req.method === "GET" && pathname === "/api/inventory") {
      return send(res, 200, {
        cars: inventory.map(car => ({
          ...car,
          pricing: pricingEngine(car),
          trust: trustEngine(car)
        }))
      });
    }

    if (req.method === "POST" && pathname === "/api/affordability") {
      return send(res, 200, affordabilityEngine(await readBody(req)));
    }

    if (req.method === "POST" && pathname === "/api/match") {
      return send(res, 200, inventoryMatchingEngine(await readBody(req)));
    }

    if (req.method === "POST" && pathname === "/api/loan/precheck") {
      return send(res, 200, loanOrchestrationEngine(await readBody(req)));
    }

    if (req.method === "POST" && pathname === "/api/swap") {
      return send(res, 200, swapEngine(await readBody(req)));
    }

    if (req.method === "POST" && pathname === "/api/events") {
      const raw = await readBody(req);
      const row = await handleEvent(raw);
      return send(res, 200, { ok: true, row });
    }

    if (req.method === "GET" && pathname === "/api/cohort/export") {
      const events = readEvents();
      return send(res, 200, {
        rows: events,
        summary: buildCohortSummary(events),
        total: events.length
      });
    }

    send(res, 404, { error: "Route not found", path: pathname });
  } catch (err) {
    console.error("[Route error]", err.message);
    send(res, 400, { error: err.message });
  }
}

http.createServer(route).listen(PORT, "0.0.0.0", () => {
  console.log(`✅ Cardekho backend running at http://0.0.0.0:${PORT}`);
  console.log(`   Health: http://localhost:${PORT}/api/health`);
  console.log(`   Sheets: ${SHEETS_WEBHOOK_URL ? "CONNECTED" : "NOT configured (set GOOGLE_SHEETS_WEBHOOK_URL)"}`);
});
