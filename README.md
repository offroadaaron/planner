# Calendar Planner App

Dockerized web + database application for the Excel planner workflow (customers, products, CVM monthly planner, and calendar).

## Stack

- FastAPI (web UI + JSON API)
- PostgreSQL 16
- Docker Compose

## Start

```bash
docker compose up --build
```

Then open:

- Web UI: `http://localhost:8000`
- Health: `http://localhost:8000/health`
- API: `http://localhost:8000/api/customers`

## What is included

- Database schema for:
  - `territories`
  - `customers`
  - `products`
  - `cvm_month_entries`
  - `calendar_settings`
  - `public_holidays`
  - `annual_leaves`
  - `reference_values`
- Seeded lookup values (`action`, `status`, etc.)
- UI pages:
  - Dashboard
  - Customers CRUD (create/list)
  - Products CRUD (create/list/filter/update)
  - CVM View (monthly date/done grid + notes)
  - Month Calendar view (planned/completed + holidays/leave + week-start toggle)
  - Workbook Import (.xlsm/.xlsx) with Preview (dry-run), row-level issue reporting, upsert policy controls, strict validation mode, and duplicate-handling policy
- JSON endpoints:
  - `GET /api/customers`
  - `GET /api/products`

## Notes for importing from your old workbook

1. Load customer/territory data into `customers` + `territories`.
2. Load product tracking rows into `products`.
3. Use CVM monthly cells (`planned_date` + `completed_manual`) in `cvm_month_entries` to drive calendar rendering.
