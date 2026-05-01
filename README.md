# Cardekho Finance-First Backend

This is a local mock backend that turns the frontend from a brochure into a working product prototype.

## Run

```powershell
cd "C:\Users\SAHIL RATHOUR\Documents\Cardekho\cardekho-finance-first-frontend\backend"
node server.js
```

Then open:

```text
C:\Users\SAHIL RATHOUR\Documents\Cardekho\cardekho-finance-first-frontend\index.html
```

## Systems Included

- Affordability Engine: income, existing EMI, credit range -> safe EMI range and max loan.
- Inventory Matching Engine: ranks cars by fit, delivery, trust, and intent.
- Loan Orchestration Engine: mock lender offers before dealer contact.
- Pricing Engine: car price, RTO, insurance, processing fee, total cost.
- Trust Engine: Vahan-style status, trust score, dealer reliability, charge protection eligibility.
- Intent Engine: first-time, urgent, EMI-sensitive, returning buyer logic.
- Swap Engine: old car value, new car price, loan needed, net EMI.
- Cohort Export: writes local events to `backend/data/cohort_events.jsonl`.

## Useful API Tests

```powershell
Invoke-RestMethod http://localhost:5050/api/health
Invoke-RestMethod http://localhost:5050/api/inventory
```

