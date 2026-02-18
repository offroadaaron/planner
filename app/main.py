import calendar
from collections import defaultdict
from datetime import date, datetime

from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import SessionLocal, engine

app = FastAPI(title="Calendar Planner")
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

VALID_EVENT_TYPES = {"planned", "completed", "annual_leave", "public_holiday", "note"}
MONTH_SHORT = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]


@app.on_event("startup")
def ensure_cvm_tables():
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS cvm_month_entries (
                  id BIGSERIAL PRIMARY KEY,
                  customer_id BIGINT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
                  year INT NOT NULL,
                  month INT NOT NULL CHECK (month BETWEEN 1 AND 12),
                  planned_date DATE,
                  completed_manual BOOLEAN NOT NULL DEFAULT FALSE,
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  UNIQUE (customer_id, year, month)
                )
                """
            )
        )


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def month_window(year: int, month: int) -> tuple[date, date]:
    start = date(year, month, 1)
    end_day = calendar.monthrange(year, month)[1]
    return start, date(year, month, end_day)


def parse_optional_int(value: str | None) -> int | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid integer value") from exc


@app.get("/health")
def health(db: Session = Depends(get_db)):
    db.execute(text("SELECT 1"))
    return {"ok": True}


@app.get("/")
def dashboard(request: Request, db: Session = Depends(get_db)):
    counts = {
        "customers": db.execute(text("SELECT COUNT(*) FROM customers")).scalar_one(),
        "stores": db.execute(text("SELECT COUNT(*) FROM stores")).scalar_one(),
        "events": db.execute(text("SELECT COUNT(*) FROM visit_events")).scalar_one(),
    }
    upcoming = db.execute(
        text(
            """
            SELECT ve.event_date, ve.event_type, COALESCE(c.name, 'N/A') AS customer_name, ve.status
            FROM visit_events ve
            LEFT JOIN customers c ON c.id = ve.customer_id
            WHERE ve.event_date >= CURRENT_DATE
            ORDER BY ve.event_date ASC
            LIMIT 15
            """
        )
    ).mappings().all()
    settings = db.execute(
        text("SELECT calendar_year, week_start_day FROM calendar_settings WHERE id = 1")
    ).mappings().first()
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "counts": counts,
            "upcoming": upcoming,
            "settings": settings,
            "today": date.today(),
        },
    )


@app.get("/customers")
def customers_page(request: Request, db: Session = Depends(get_db)):
    territories = db.execute(text("SELECT id, name FROM territories ORDER BY name")).mappings().all()
    customers = db.execute(
        text(
            """
            SELECT c.id, c.cust_code, c.name, c.trade_name, c.group_name, c.iws_code,
                   COALESCE(t.name, '') AS territory
            FROM customers c
            LEFT JOIN territories t ON t.id = c.territory_id
            ORDER BY c.cust_code
            """
        )
    ).mappings().all()
    return templates.TemplateResponse(
        "customers.html",
        {"request": request, "customers": customers, "territories": territories},
    )


@app.post("/customers")
def create_customer(
    cust_code: str = Form(...),
    name: str = Form(...),
    trade_name: str = Form(""),
    territory_name: str = Form(""),
    group_name: str = Form(""),
    iws_code: str = Form(""),
    db: Session = Depends(get_db),
):
    territory_id = None
    if territory_name.strip():
        territory = db.execute(
            text("SELECT id FROM territories WHERE name = :name"), {"name": territory_name.strip()}
        ).mappings().first()
        if territory is None:
            territory_id = db.execute(
                text("INSERT INTO territories (name) VALUES (:name) RETURNING id"),
                {"name": territory_name.strip()},
            ).scalar_one()
        else:
            territory_id = territory["id"]

    try:
        db.execute(
            text(
                """
                INSERT INTO customers (cust_code, name, trade_name, territory_id, group_name, iws_code, created_at)
                VALUES (:cust_code, :name, NULLIF(:trade_name, ''), :territory_id,
                        NULLIF(:group_name, ''), NULLIF(:iws_code, ''), NOW())
                """
            ),
            {
                "cust_code": cust_code.strip(),
                "name": name.strip(),
                "trade_name": trade_name.strip(),
                "territory_id": territory_id,
                "group_name": group_name.strip(),
                "iws_code": iws_code.strip(),
            },
        )
        db.commit()
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Could not create customer: {exc}") from exc

    return RedirectResponse(url="/customers", status_code=303)


@app.get("/stores")
def stores_page(request: Request, db: Session = Depends(get_db)):
    customers = db.execute(
        text("SELECT id, cust_code, name FROM customers ORDER BY cust_code")
    ).mappings().all()
    stores = db.execute(
        text(
            """
            SELECT s.id, s.city, s.state, s.address_1, s.postcode,
                   c.cust_code, c.name AS customer_name
            FROM stores s
            LEFT JOIN customers c ON c.id = s.customer_id
            ORDER BY s.id DESC
            LIMIT 200
            """
        )
    ).mappings().all()
    return templates.TemplateResponse(
        "stores.html", {"request": request, "stores": stores, "customers": customers}
    )


@app.post("/stores")
def create_store(
    customer_id: int = Form(...),
    address_1: str = Form(...),
    city: str = Form(...),
    state: str = Form(...),
    postcode: str = Form(""),
    country: str = Form("AUSTRALIA"),
    db: Session = Depends(get_db),
):
    db.execute(
        text(
            """
            INSERT INTO stores (customer_id, address_1, city, state, postcode, country, created_at)
            VALUES (:customer_id, :address_1, :city, :state, NULLIF(:postcode, ''), NULLIF(:country, ''), NOW())
            """
        ),
        {
            "customer_id": customer_id,
            "address_1": address_1.strip(),
            "city": city.strip(),
            "state": state.strip(),
            "postcode": postcode.strip(),
            "country": country.strip(),
        },
    )
    db.commit()
    return RedirectResponse(url="/stores", status_code=303)


@app.get("/events")
def events_page(request: Request, db: Session = Depends(get_db)):
    customers = db.execute(
        text("SELECT id, cust_code, name FROM customers ORDER BY cust_code")
    ).mappings().all()
    stores = db.execute(
        text("SELECT id, city, state, address_1 FROM stores ORDER BY id DESC LIMIT 300")
    ).mappings().all()
    events = db.execute(
        text(
            """
            SELECT ve.id, ve.event_date, ve.event_type, ve.action, ve.status, ve.notes,
                   c.cust_code, c.name AS customer_name,
                   s.city, s.state
            FROM visit_events ve
            LEFT JOIN customers c ON c.id = ve.customer_id
            LEFT JOIN stores s ON s.id = ve.store_id
            ORDER BY ve.event_date DESC, ve.id DESC
            LIMIT 300
            """
        )
    ).mappings().all()
    statuses = db.execute(
        text("SELECT value FROM reference_values WHERE category='status' AND active ORDER BY sort_order")
    ).scalars().all()
    actions = db.execute(
        text("SELECT value FROM reference_values WHERE category='action' AND active ORDER BY sort_order")
    ).scalars().all()
    return templates.TemplateResponse(
        "events.html",
        {
            "request": request,
            "events": events,
            "customers": customers,
            "stores": stores,
            "statuses": statuses,
            "actions": actions,
            "event_types": sorted(VALID_EVENT_TYPES),
        },
    )


@app.post("/events")
def create_event(
    customer_id: int = Form(...),
    store_id: int = Form(...),
    event_type: str = Form(...),
    event_date: str = Form(...),
    action: str = Form(""),
    status: str = Form(""),
    next_action: str = Form(""),
    last_contact: str = Form(""),
    notes: str = Form(""),
    db: Session = Depends(get_db),
):
    normalized_type = event_type.strip().lower()
    if normalized_type not in VALID_EVENT_TYPES:
        raise HTTPException(status_code=400, detail="Invalid event_type")

    db.execute(
        text(
            """
            INSERT INTO visit_events
              (customer_id, store_id, event_type, event_date, action, status, next_action, last_contact, notes, created_at)
            VALUES
              (:customer_id, :store_id, :event_type, :event_date, NULLIF(:action, ''), NULLIF(:status, ''),
               NULLIF(:next_action, ''), NULLIF(:last_contact, '')::date, NULLIF(:notes, ''), NOW())
            """
        ),
        {
            "customer_id": customer_id,
            "store_id": store_id,
            "event_type": normalized_type,
            "event_date": event_date,
            "action": action.strip(),
            "status": status.strip(),
            "next_action": next_action.strip(),
            "last_contact": last_contact.strip(),
            "notes": notes.strip(),
        },
    )
    db.commit()
    return RedirectResponse(url="/events", status_code=303)


@app.get("/calendar")
def calendar_page(
    request: Request,
    month: int = Query(default=datetime.utcnow().month, ge=1, le=12),
    year: int | None = Query(default=None),
    territory_id: str = Query(default=""),
    db: Session = Depends(get_db),
):
    setting_row = db.execute(
        text("SELECT calendar_year, week_start_day FROM calendar_settings WHERE id = 1")
    ).mappings().first()
    if year is None:
        year = int(setting_row["calendar_year"]) if setting_row else datetime.utcnow().year
    week_start_day = setting_row["week_start_day"] if setting_row else "monday"

    territories = db.execute(text("SELECT id, name FROM territories ORDER BY name")).mappings().all()
    start, end = month_window(year, month)

    territory_id_value = parse_optional_int(territory_id)

    event_rows = db.execute(
        text(
            """
            SELECT ve.event_date, ve.event_type, c.id AS customer_id, COALESCE(c.cust_code, '') AS cust_code,
                   COALESCE(c.name, 'Unassigned') AS customer_name,
                   COALESCE(c.trade_name, '') AS trade_name,
                   COALESCE(s.city || ', ' || s.state, '') AS location,
                   ve.notes, ve.status, ve.action
            FROM visit_events ve
            LEFT JOIN customers c ON c.id = ve.customer_id
            LEFT JOIN stores s ON s.id = ve.store_id
            WHERE ve.event_date BETWEEN :start AND :end
              AND (:territory_id IS NULL OR c.territory_id = :territory_id)
            ORDER BY ve.event_date, ve.id
            """
        ),
        {"start": start, "end": end, "territory_id": territory_id_value},
    ).mappings().all()

    # CVM month entries are the primary planning source for this grid.
    cvm_rows = db.execute(
        text(
            """
            SELECT e.planned_date, e.completed_manual, c.id AS customer_id,
                   COALESCE(c.cust_code, '') AS cust_code,
                   COALESCE(c.name, 'Unassigned') AS customer_name,
                   COALESCE(c.trade_name, '') AS trade_name,
                   COALESCE(fs.city || ', ' || fs.state, '') AS location
            FROM cvm_month_entries e
            JOIN customers c ON c.id = e.customer_id
            LEFT JOIN LATERAL (
                SELECT city, state
                FROM stores s
                WHERE s.customer_id = c.id
                ORDER BY s.id
                LIMIT 1
            ) fs ON TRUE
            WHERE e.year = :year
              AND e.month = :month
              AND e.planned_date IS NOT NULL
              AND (:territory_id IS NULL OR c.territory_id = :territory_id)
            ORDER BY e.planned_date, c.name
            """
        ),
        {"year": year, "month": month, "territory_id": territory_id_value},
    ).mappings().all()

    day_items = defaultdict(lambda: {"planned": [], "completed": []})
    planned_total = 0
    completed_total = 0

    # One row per customer/date in planner. Completed always wins over planned.
    status_by_key: dict[tuple, dict[str, object]] = {}

    def upsert_status(key: tuple, day: int, text_value: str, is_completed: bool):
        entry = status_by_key.get(key)
        if entry is None:
            entry = {"day": day, "text": text_value, "planned": False, "completed": False}
            status_by_key[key] = entry
        elif len(text_value) > len(str(entry["text"])):
            # Keep the most descriptive label when both sources provide one.
            entry["text"] = text_value

        if is_completed:
            entry["completed"] = True
            entry["planned"] = False
        elif not entry["completed"]:
            entry["planned"] = True

    for row in event_rows:
        day = row["event_date"].day
        title = f"{row['cust_code']} {row['customer_name']}".strip()
        detail_parts = [x for x in [row["trade_name"], row["location"], row["action"], row["status"]] if x]
        detail = " | ".join(detail_parts)
        note = row["notes"] or ""
        item_text = title if not detail else f"{title} ({detail})"
        if note:
            item_text = f"{item_text} - {note}"

        if row["customer_id"] is not None:
            key = ("cust", row["customer_id"], row["event_date"])
        else:
            key = ("anon", row["event_date"], row["cust_code"], row["customer_name"], row["location"])
        upsert_status(key, day, item_text, row["event_type"] == "completed")

    # Pull CVM entries into planner so monthly date/tick saves are always visible.
    for row in cvm_rows:
        planned_date = row["planned_date"]
        day = planned_date.day
        title = f"{row['cust_code']} {row['customer_name']}".strip()
        detail_parts = [x for x in [row["trade_name"], row["location"]] if x]
        detail = f" ({' | '.join(detail_parts)})" if detail_parts else ""
        item_text = f"{title}{detail}"
        key = ("cust", row["customer_id"], planned_date)
        upsert_status(key, day, item_text, bool(row["completed_manual"]))

    for entry in status_by_key.values():
        day = int(entry["day"])
        text_value = str(entry["text"])
        if bool(entry["completed"]):
            day_items[day]["completed"].append(text_value)
            completed_total += 1
        elif bool(entry["planned"]):
            day_items[day]["planned"].append(text_value)
            planned_total += 1

    firstweekday = 0 if week_start_day == "monday" else 6
    cal = calendar.Calendar(firstweekday=firstweekday)
    weeks = []
    for week_dates in cal.monthdatescalendar(year, month):
        week_cells = []
        for d in week_dates:
            week_cells.append(
                {
                    "date": d,
                    "day": d.day,
                    "in_month": d.month == month,
                    "items": day_items.get(d.day, {}) if d.month == month else {},
                }
            )
        weeks.append(week_cells)

    weekday_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    if week_start_day == "sunday":
        weekday_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]

    return templates.TemplateResponse(
        "calendar.html",
        {
            "request": request,
            "year": year,
            "month": month,
            "month_name": calendar.month_name[month],
            "weeks": weeks,
            "weekday_names": weekday_names,
            "week_start_day": week_start_day,
            "planned_total": planned_total,
            "completed_total": completed_total,
            "territories": territories,
            "territory_id": territory_id_value,
        },
    )


@app.get("/cvm")
def cvm_page(
    request: Request,
    year: int | None = Query(default=None),
    territory_id: str = Query(default=""),
    db: Session = Depends(get_db),
):
    setting_row = db.execute(
        text("SELECT calendar_year FROM calendar_settings WHERE id = 1")
    ).mappings().first()
    if year is None:
        year = int(setting_row["calendar_year"]) if setting_row else datetime.utcnow().year

    territory_id_value = parse_optional_int(territory_id)

    territories = db.execute(text("SELECT id, name FROM territories ORDER BY name")).mappings().all()

    rows = db.execute(
        text(
            """
            SELECT c.id, COALESCE(t.name, '') AS territory, c.cust_code, c.name AS customer_name,
                   COALESCE(c.trade_name, '') AS trade_name,
                   COALESCE(MIN(s.sort_bucket), '') AS sort_bucket
            FROM customers c
            LEFT JOIN territories t ON t.id = c.territory_id
            LEFT JOIN stores s ON s.customer_id = c.id
            WHERE (:territory_id IS NULL OR c.territory_id = :territory_id)
            GROUP BY c.id, t.name, c.cust_code, c.name, c.trade_name
            ORDER BY COALESCE(MIN(s.sort_bucket), 'zzz'), c.name
            """
        ),
        {"territory_id": territory_id_value},
    ).mappings().all()

    entry_rows = db.execute(
        text(
            """
            SELECT e.customer_id, e.month, e.planned_date, e.completed_manual
            FROM cvm_month_entries e
            JOIN customers c ON c.id = e.customer_id
            WHERE e.year = :year
              AND (:territory_id IS NULL OR c.territory_id = :territory_id)
            """
        ),
        {"year": year, "territory_id": territory_id_value},
    ).mappings().all()

    entry_map: dict[int, dict[int, dict[str, object]]] = defaultdict(dict)
    for e in entry_rows:
        entry_map[e["customer_id"]][e["month"]] = {
            "planned_date": e["planned_date"],
            "completed_manual": e["completed_manual"],
        }

    enriched_rows = []
    for r in rows:
        rid = r["id"]
        month_data = entry_map.get(rid, {})
        planned_total = sum(1 for m in month_data.values() if m.get("planned_date"))
        completed_total = sum(1 for m in month_data.values() if m.get("completed_manual"))
        completed_dates = [
            m.get("planned_date") for m in month_data.values() if m.get("completed_manual") and m.get("planned_date")
        ]
        last_completed = max(completed_dates) if completed_dates else None
        enriched_rows.append(
            {
                **dict(r),
                "month_data": month_data,
                "planned_total": planned_total,
                "completed_total": completed_total,
                "last_completed": last_completed,
            }
        )

    response = templates.TemplateResponse(
        "cvm.html",
        {
            "request": request,
            "rows": enriched_rows,
            "year": year,
            "months": MONTH_SHORT,
            "territories": territories,
            "territory_id": territory_id_value,
        },
    )
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    return response


@app.post("/cvm/month-update")
def cvm_month_update(
    customer_id: int = Form(...),
    month: int = Form(...),
    planned_date: str = Form(""),
    completed_manual: str | None = Form(default=None),
    year: int = Form(...),
    territory_id: str = Form(default=""),
    db: Session = Depends(get_db),
):
    if month < 1 or month > 12:
        raise HTTPException(status_code=400, detail="Invalid month")

    is_completed = bool(completed_manual)
    has_date = bool(planned_date.strip())
    if is_completed and not has_date:
        is_completed = False

    if not has_date and not is_completed:
        db.execute(
            text(
                """
                DELETE FROM cvm_month_entries
                WHERE customer_id = :customer_id
                  AND year = :year
                  AND month = :month
                """
            ),
            {"customer_id": customer_id, "year": year, "month": month},
        )
    else:
        db.execute(
            text(
                """
                INSERT INTO cvm_month_entries
                  (customer_id, year, month, planned_date, completed_manual, updated_at)
                VALUES
                  (:customer_id, :year, :month, NULLIF(:planned_date, '')::date, :completed_manual, NOW())
                ON CONFLICT (customer_id, year, month)
                DO UPDATE SET
                  planned_date = EXCLUDED.planned_date,
                  completed_manual = EXCLUDED.completed_manual,
                  updated_at = NOW()
                """
            ),
            {
                "customer_id": customer_id,
                "year": year,
                "month": month,
                "planned_date": planned_date.strip(),
                "completed_manual": is_completed,
            },
        )

    db.commit()
    territory_id_value = parse_optional_int(territory_id)
    territory_param = f"&territory_id={territory_id_value}" if territory_id_value else ""
    return RedirectResponse(url=f"/cvm?year={year}{territory_param}", status_code=303)


@app.get("/api/customers")
def list_customers(db: Session = Depends(get_db)):
    rows = db.execute(
        text(
            """
            SELECT c.id, c.cust_code, c.name, c.trade_name, c.group_name, c.iws_code,
                   t.name AS territory
            FROM customers c
            LEFT JOIN territories t ON t.id = c.territory_id
            ORDER BY c.cust_code
            """
        )
    ).mappings().all()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/events")
def list_events(
    start: date | None = None,
    end: date | None = None,
    db: Session = Depends(get_db),
):
    where = []
    params = {}
    if start is not None:
        where.append("ve.event_date >= :start")
        params["start"] = start
    if end is not None:
        where.append("ve.event_date <= :end")
        params["end"] = end

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    sql = text(
        f"""
        SELECT ve.id, ve.event_date, ve.event_type, ve.action, ve.status, ve.next_action,
               ve.last_contact, ve.notes, c.cust_code, c.name AS customer_name
        FROM visit_events ve
        LEFT JOIN customers c ON c.id = ve.customer_id
        {where_sql}
        ORDER BY ve.event_date DESC, ve.id DESC
        LIMIT 500
        """
    )
    rows = db.execute(sql, params).mappings().all()
    return {"items": [dict(r) for r in rows]}
