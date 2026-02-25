# Calendar Planner

A self-hosted web app for managing sales territory customers, products, CVM monthly planning, and a visit calendar. Imports data from your existing Excel workbook (.xlsm/.xlsx).

## Prerequisites

- [Docker Desktop](https://docs.docker.com/get-docker/) (includes Docker Compose)

That's it. No Python, Node, or database setup required.

## Quick Start

```bash
git clone git@github.com:offroadaaron/planner.git
cd planner
./install.sh
```

Then open **http://localhost:8001** in your browser.

> **Manual steps (if you prefer):**
> ```bash
> cp .env.example .env        # only needed if you want to customise settings
> docker compose up --build -d
> ```

## Pages

| Page | Description |
|------|-------------|
| **Dashboard** | Visits trend chart with completion rate, territory summary |
| **Customers** | Create, search, inline-edit, paginate, export CSV/XLSX |
| **Products** | Track product actions, status, follow-ups, export CSV/XLSX |
| **CVM** | Monthly planned/completed grid with bulk actions and print view |
| **Calendar** | Week view with holidays, annual leave, and jump-to-today |
| **Import** | Upload .xlsm/.xlsx — preview dry run, then apply |

## Configuration

The default configuration works out of the box. To change the port or database credentials, edit `.env` (copied from `.env.example`) and `docker-compose.yml`.

| Setting | Default | Description |
|---------|---------|-------------|
| Port | `8001` | Change `8001:8000` in `docker-compose.yml` |
| Database | `planner / planner` | Postgres credentials in `docker-compose.yml` |

## Importing from your Excel workbook

1. Go to **Import** in the nav
2. Select your `.xlsm` or `.xlsx` file and choose options
3. Click **Preview** — reviews all changes without committing
4. If the preview looks correct, click **Apply Import**

Supported sheets: `Get Data`, `Customer Details`, `CVM`, monthly sheets (e.g. `JANUARY`), and `Database`.

## Common commands

```bash
# Stop
docker compose down

# View logs
docker compose logs -f web

# Restart after code changes
docker compose up --build -d

# Run tests (requires Python 3.12+)
python3 -m venv .venv-test
. .venv-test/bin/activate
pip install -r requirements-dev.txt
python -m pytest tests/ -q
```

## Stack

- **Backend**: FastAPI + SQLAlchemy (PostgreSQL 16)
- **Frontend**: Jinja2 server-rendered templates; React (dashboard chart only, CDN)
- **Infrastructure**: Docker Compose (web + db services)
