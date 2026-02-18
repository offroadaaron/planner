# Calendar Planner App

Dockerized web + database application for the Excel planner workflow (customers, stores, visit events, monthly planning calendar).

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
  - `stores`
  - `visit_events`
  - `calendar_settings`
  - `public_holidays`
  - `annual_leaves`
  - `reference_values`
- Seeded lookup values (`action`, `status`, etc.)
- UI pages:
  - Dashboard
  - Customers CRUD (create/list)
  - Stores CRUD (create/list)
  - Visit Events CRUD (create/list)
  - Month Calendar view (planned/completed counters per day)
- JSON endpoints:
  - `GET /api/customers`
  - `GET /api/events?start=YYYY-MM-DD&end=YYYY-MM-DD`

## Notes for importing from your old workbook

1. Load customer/territory data into `customers` + `territories`.
2. Load store details into `stores`.
3. Unpivot repeated action/status/next-action columns into multiple rows in `visit_events`.
4. Use `event_type` values (`planned`, `completed`, `annual_leave`, `public_holiday`, `note`) to drive calendar rendering.
