import calendar
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta

from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.exc import DataError, IntegrityError, SQLAlchemyError
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import SessionLocal, engine
from app.workbook_import import import_planner_workbook

app = FastAPI(title="Calendar Planner")
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")
logger = logging.getLogger(__name__)

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
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS products (
                  id BIGSERIAL PRIMARY KEY,
                  customer_id BIGINT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
                  product_name TEXT NOT NULL,
                  last_visit DATE,
                  action TEXT,
                  status TEXT,
                  next_action TEXT,
                  last_contact DATE,
                  notes TEXT,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text("ALTER TABLE customers ADD COLUMN IF NOT EXISTS cvm_notes TEXT")
        )
        conn.execute(
            text("ALTER TABLE customers ADD COLUMN IF NOT EXISTS door_count INTEGER")
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


def parse_optional_date(value: str | None, field_name: str, required: bool = False) -> date | None:
    if value is None:
        if required:
            raise HTTPException(status_code=400, detail=f"{field_name} is required")
        return None

    cleaned = value.strip()
    if not cleaned:
        if required:
            raise HTTPException(status_code=400, detail=f"{field_name} is required")
        return None

    try:
        return date.fromisoformat(cleaned)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}. Use YYYY-MM-DD.") from exc


def ensure_customer_exists(db: Session, customer_id: int):
    exists = db.execute(
        text("SELECT 1 FROM customers WHERE id = :customer_id"),
        {"customer_id": customer_id},
    ).scalar()
    if not exists:
        raise HTTPException(status_code=400, detail="Invalid customer_id")


def ensure_product_exists(db: Session, product_id: int):
    exists = db.execute(
        text("SELECT 1 FROM products WHERE id = :product_id"),
        {"product_id": product_id},
    ).scalar()
    if not exists:
        raise HTTPException(status_code=400, detail="Invalid product_id")


def load_customers_page_data(db: Session):
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
    return {"customers": customers, "territories": territories}


def render_customers_page(
    request: Request,
    db: Session,
    *,
    status_code: int = 200,
    form_error: str = "",
    form_values: dict[str, str] | None = None,
):
    context = load_customers_page_data(db)
    return templates.TemplateResponse(
        "customers.html",
        {
            "request": request,
            "customers": context["customers"],
            "territories": context["territories"],
            "form_error": form_error,
            "form_values": form_values or {},
        },
        status_code=status_code,
    )


def json_safe(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_safe(v) for v in value]
    return value


@app.get("/health")
def health(db: Session = Depends(get_db)):
    db.execute(text("SELECT 1"))
    return {"ok": True}


@app.get("/")
def dashboard(request: Request, db: Session = Depends(get_db)):
    today = date.today()
    counts = {
        "customers": db.execute(text("SELECT COUNT(*) FROM customers")).scalar_one(),
        "products": db.execute(text("SELECT COUNT(*) FROM products")).scalar_one(),
        "cvm_entries": db.execute(text("SELECT COUNT(*) FROM cvm_month_entries")).scalar_one(),
    }
    recent_products = db.execute(
        text(
            """
            SELECT p.updated_at, p.product_name, COALESCE(c.name, 'N/A') AS customer_name,
                   COALESCE(t.name, '') AS territory, COALESCE(p.status, '') AS status
            FROM products p
            JOIN customers c ON c.id = p.customer_id
            LEFT JOIN territories t ON t.id = c.territory_id
            ORDER BY p.updated_at DESC
            LIMIT 15
            """
        )
    ).mappings().all()
    upcoming_rows = db.execute(
        text(
            """
            SELECT e.id, e.planned_date AS event_date,
                   COALESCE(c.name, 'N/A') AS customer_name,
                   COALESCE(c.cust_code, '') AS cust_code,
                   COALESCE(t.name, '') AS territory,
                   CASE WHEN e.completed_manual THEN 'Completed' ELSE 'Planned' END AS status
            FROM cvm_month_entries e
            JOIN customers c ON c.id = e.customer_id
            LEFT JOIN territories t ON t.id = c.territory_id
            WHERE e.planned_date IS NOT NULL
            ORDER BY e.planned_date ASC, c.name ASC
            LIMIT 500
            """
        )
    ).mappings().all()

    month_points: list[dict[str, object]] = []
    month_cursor = date(today.year, today.month, 1)
    for _ in range(11):
        prev_month = month_cursor.month - 1
        prev_year = month_cursor.year
        if prev_month == 0:
            prev_month = 12
            prev_year -= 1
        month_cursor = date(prev_year, prev_month, 1)
    for _ in range(12):
        next_month = month_cursor.month + 1
        next_year = month_cursor.year
        if next_month == 13:
            next_month = 1
            next_year += 1
        month_end = date(next_year, next_month, 1) - timedelta(days=1)

        row = db.execute(
            text(
                """
                SELECT
                  SUM(CASE WHEN completed_manual THEN 0 ELSE 1 END) AS planned_count,
                  SUM(CASE WHEN completed_manual THEN 1 ELSE 0 END) AS completed_count
                FROM cvm_month_entries
                WHERE planned_date BETWEEN :start_date AND :end_date
                """
            ),
            {"start_date": month_cursor, "end_date": month_end},
        ).mappings().first()
        month_points.append(
            {
                "label": month_cursor.strftime("%b %y"),
                "planned": int(row["planned_count"] or 0),
                "completed": int(row["completed_count"] or 0),
            }
        )
        month_cursor = date(next_year, next_month, 1)

    settings = db.execute(
        text("SELECT calendar_year, week_start_day FROM calendar_settings WHERE id = 1")
    ).mappings().first()
    dashboard_data = {
        "counts": counts,
        "settings": dict(settings) if settings else {"calendar_year": today.year, "week_start_day": "monday"},
        "today": today.isoformat(),
        "upcoming": [json_safe(dict(r)) for r in upcoming_rows],
        "recent_products": [json_safe(dict(r)) for r in recent_products],
        "visitsByMonth": json_safe(month_points),
    }
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "dashboard_data": dashboard_data,
        },
    )


@app.get("/events")
def events_alias():
    return RedirectResponse(url="/cvm", status_code=307)


@app.get("/stores")
def stores_alias():
    return RedirectResponse(url="/customers", status_code=307)


@app.get("/customers")
def customers_page(request: Request, db: Session = Depends(get_db)):
    return render_customers_page(request, db)


@app.post("/customers")
def create_customer(
    request: Request,
    cust_code: str = Form(...),
    name: str = Form(...),
    trade_name: str = Form(""),
    territory_name: str = Form(""),
    group_name: str = Form(""),
    iws_code: str = Form(""),
    db: Session = Depends(get_db),
):
    form_values = {
        "cust_code": cust_code.strip(),
        "name": name.strip(),
        "trade_name": trade_name.strip(),
        "territory_name": territory_name.strip(),
        "group_name": group_name.strip(),
        "iws_code": iws_code.strip(),
    }
    if not form_values["cust_code"] or not form_values["name"]:
        return render_customers_page(
            request,
            db,
            status_code=400,
            form_error="Cust Code and Customer Name are required.",
            form_values=form_values,
        )

    try:
        territory_id = None
        if form_values["territory_name"]:
            territory = db.execute(
                text("SELECT id FROM territories WHERE name = :name"), {"name": form_values["territory_name"]}
            ).mappings().first()
            if territory is None:
                territory_id = db.execute(
                    text("INSERT INTO territories (name) VALUES (:name) RETURNING id"),
                    {"name": form_values["territory_name"]},
                ).scalar_one()
            else:
                territory_id = territory["id"]

        db.execute(
            text(
                """
                INSERT INTO customers (cust_code, name, trade_name, territory_id, group_name, iws_code, created_at)
                VALUES (:cust_code, :name, NULLIF(:trade_name, ''), :territory_id,
                        NULLIF(:group_name, ''), NULLIF(:iws_code, ''), NOW())
                """
            ),
            {
                "cust_code": form_values["cust_code"],
                "name": form_values["name"],
                "trade_name": form_values["trade_name"],
                "territory_id": territory_id,
                "group_name": form_values["group_name"],
                "iws_code": form_values["iws_code"],
            },
        )
        db.commit()
    except (IntegrityError, DataError):
        db.rollback()
        return render_customers_page(
            request,
            db,
            status_code=400,
            form_error="Could not create customer. Check for duplicate customer code or invalid values.",
            form_values=form_values,
        )
    except SQLAlchemyError as exc:
        db.rollback()
        logger.exception("Unexpected database error while creating customer")
        raise HTTPException(status_code=500, detail="Unexpected server error while creating customer.") from exc

    return RedirectResponse(url="/customers", status_code=303)


@app.post("/customers/{customer_id}/delete")
def delete_customer(customer_id: int, db: Session = Depends(get_db)):
    ensure_customer_exists(db, customer_id)

    try:
        # Explicitly delete related rows so behavior is consistent across DB engines.
        db.execute(text("DELETE FROM visit_events WHERE customer_id = :customer_id"), {"customer_id": customer_id})
        db.execute(text("DELETE FROM products WHERE customer_id = :customer_id"), {"customer_id": customer_id})
        db.execute(text("DELETE FROM cvm_month_entries WHERE customer_id = :customer_id"), {"customer_id": customer_id})
        # Stores use ON DELETE SET NULL in Postgres schema, so remove them here as part of client delete.
        db.execute(text("DELETE FROM stores WHERE customer_id = :customer_id"), {"customer_id": customer_id})
        db.execute(text("DELETE FROM customers WHERE id = :customer_id"), {"customer_id": customer_id})
        db.commit()
    except (IntegrityError, DataError) as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail="Could not delete customer. Check related data.") from exc
    except SQLAlchemyError as exc:
        db.rollback()
        logger.exception("Unexpected database error while deleting customer")
        raise HTTPException(status_code=500, detail="Unexpected server error while deleting customer.") from exc

    return RedirectResponse(url="/customers", status_code=303)


@app.get("/products")
def products_page(
    request: Request,
    customer_id: int | None = Query(default=None),
    territory: str = Query(default=""),
    action: str = Query(default=""),
    status: str = Query(default=""),
    q: str = Query(default=""),
    db: Session = Depends(get_db),
):
    territory_filter = territory.strip()
    action_filter = action.strip()
    status_filter = status.strip()
    text_filter = q.strip()

    customers = db.execute(
        text(
            """
            SELECT c.id, c.cust_code, c.name, COALESCE(t.name, '') AS territory
            FROM customers c
            LEFT JOIN territories t ON t.id = c.territory_id
            ORDER BY c.cust_code
            """
        )
    ).mappings().all()
    actions = db.execute(
        text("SELECT value FROM reference_values WHERE category='action' AND active ORDER BY sort_order")
    ).scalars().all()
    statuses = db.execute(
        text("SELECT value FROM reference_values WHERE category='status' AND active ORDER BY sort_order")
    ).scalars().all()
    territories = db.execute(
        text(
            """
            SELECT DISTINCT COALESCE(t.name, '') AS territory
            FROM customers c
            LEFT JOIN territories t ON t.id = c.territory_id
            WHERE COALESCE(t.name, '') <> ''
            ORDER BY territory
            """
        )
    ).scalars().all()

    where_clauses: list[str] = []
    sql_params: dict[str, object] = {}

    if customer_id is not None:
        where_clauses.append("c.id = :customer_id")
        sql_params["customer_id"] = customer_id
    if territory_filter:
        where_clauses.append("COALESCE(t.name, '') = :territory")
        sql_params["territory"] = territory_filter
    if action_filter:
        where_clauses.append("COALESCE(p.action, '') = :action")
        sql_params["action"] = action_filter
    if status_filter:
        where_clauses.append("COALESCE(p.status, '') = :status")
        sql_params["status"] = status_filter
    if text_filter:
        where_clauses.append(
            """
            (
              LOWER(COALESCE(p.product_name, '')) LIKE :text_filter
              OR LOWER(COALESCE(c.name, '')) LIKE :text_filter
              OR LOWER(COALESCE(c.cust_code, '')) LIKE :text_filter
              OR LOWER(COALESCE(p.notes, '')) LIKE :text_filter
            )
            """
        )
        sql_params["text_filter"] = f"%{text_filter.lower()}%"

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    products = db.execute(
        text(
            f"""
            SELECT p.id,
                   p.product_name,
                   lv.last_visit,
                   p.action,
                   p.status,
                   p.next_action,
                   p.last_contact,
                   p.notes,
                   c.id AS customer_id, c.cust_code, c.name AS customer_name,
                   COALESCE(t.name, '') AS territory
            FROM products p
            JOIN customers c ON c.id = p.customer_id
            LEFT JOIN territories t ON t.id = c.territory_id
            LEFT JOIN (
              SELECT e.customer_id, MAX(e.planned_date) AS last_visit
              FROM cvm_month_entries e
              WHERE e.completed_manual = TRUE
                AND e.planned_date IS NOT NULL
              GROUP BY e.customer_id
            ) lv ON lv.customer_id = p.customer_id
            {where_sql}
            ORDER BY c.cust_code, p.product_name
            """
        ),
        sql_params,
    ).mappings().all()
    return templates.TemplateResponse(
        "products.html",
        {
            "request": request,
            "products": products,
            "customers": customers,
            "actions": actions,
            "statuses": statuses,
            "territories": territories,
            "filters": {
                "customer_id": customer_id,
                "territory": territory_filter,
                "action": action_filter,
                "status": status_filter,
                "q": text_filter,
            },
        },
    )


@app.post("/products")
def create_product(
    customer_id: int = Form(...),
    product_name: str = Form(...),
    last_visit: str = Form(""),
    action: str = Form(""),
    status: str = Form(""),
    next_action: str = Form(""),
    last_contact: str = Form(""),
    notes: str = Form(""),
    db: Session = Depends(get_db),
):
    ensure_customer_exists(db, customer_id)
    last_visit_value = parse_optional_date(last_visit, "last_visit")
    last_contact_value = parse_optional_date(last_contact, "last_contact")
    if not product_name.strip():
        raise HTTPException(status_code=400, detail="product_name is required")

    try:
        db.execute(
            text(
                """
                INSERT INTO products
                  (customer_id, product_name, last_visit, action, status, next_action, last_contact, notes, created_at, updated_at)
                VALUES
                  (:customer_id, :product_name, :last_visit, NULLIF(:action, ''), NULLIF(:status, ''),
                   NULLIF(:next_action, ''), :last_contact, NULLIF(:notes, ''), NOW(), NOW())
                """
            ),
            {
                "customer_id": customer_id,
                "product_name": product_name.strip(),
                "last_visit": last_visit_value,
                "action": action.strip(),
                "status": status.strip(),
                "next_action": next_action.strip(),
                "last_contact": last_contact_value,
                "notes": notes.strip(),
            },
        )
        db.commit()
    except (IntegrityError, DataError) as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail="Could not create product. Check field values.") from exc
    except SQLAlchemyError as exc:
        db.rollback()
        logger.exception("Unexpected database error while creating product")
        raise HTTPException(status_code=500, detail="Unexpected server error while creating product.") from exc

    return RedirectResponse(url="/products", status_code=303)


@app.post("/products/{product_id}")
def update_product(
    product_id: int,
    customer_id: int = Form(...),
    product_name: str = Form(...),
    last_visit: str | None = Form(default=None),
    action: str = Form(""),
    status: str = Form(""),
    next_action: str = Form(""),
    last_contact: str = Form(""),
    notes: str = Form(""),
    db: Session = Depends(get_db),
):
    ensure_product_exists(db, product_id)
    ensure_customer_exists(db, customer_id)
    last_visit_value = parse_optional_date(last_visit, "last_visit")
    last_contact_value = parse_optional_date(last_contact, "last_contact")
    if not product_name.strip():
        raise HTTPException(status_code=400, detail="product_name is required")

    try:
        db.execute(
            text(
                """
                UPDATE products
                SET customer_id = :customer_id,
                    product_name = :product_name,
                    last_visit = COALESCE(:last_visit, last_visit),
                    action = NULLIF(:action, ''),
                    status = NULLIF(:status, ''),
                    next_action = NULLIF(:next_action, ''),
                    last_contact = :last_contact,
                    notes = NULLIF(:notes, ''),
                    updated_at = NOW()
                WHERE id = :product_id
                """
            ),
            {
                "product_id": product_id,
                "customer_id": customer_id,
                "product_name": product_name.strip(),
                "last_visit": last_visit_value,
                "action": action.strip(),
                "status": status.strip(),
                "next_action": next_action.strip(),
                "last_contact": last_contact_value,
                "notes": notes.strip(),
            },
        )
        db.commit()
    except (IntegrityError, DataError) as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail="Could not update product. Check field values.") from exc
    except SQLAlchemyError as exc:
        db.rollback()
        logger.exception("Unexpected database error while updating product")
        raise HTTPException(status_code=500, detail="Unexpected server error while updating product.") from exc

    return RedirectResponse(url="/products", status_code=303)


@app.get("/calendar")
def calendar_page(
    request: Request,
    month: int = Query(default=datetime.utcnow().month, ge=1, le=12),
    year: int | None = Query(default=None),
    territory_id: str = Query(default=""),
    week_start_day: str = Query(default=""),
    db: Session = Depends(get_db),
):
    calendar_today = date.today()
    setting_row = db.execute(
        text("SELECT calendar_year, week_start_day FROM calendar_settings WHERE id = 1")
    ).mappings().first()
    if year is None:
        year = int(setting_row["calendar_year"]) if setting_row else datetime.utcnow().year

    requested_week_start = week_start_day.strip().lower()
    persisted_week_start = str(setting_row["week_start_day"]).lower() if setting_row else "monday"
    if requested_week_start in {"monday", "sunday"}:
        resolved_week_start = requested_week_start
        if requested_week_start != persisted_week_start:
            db.execute(
                text("UPDATE calendar_settings SET week_start_day = :week_start_day WHERE id = 1"),
                {"week_start_day": requested_week_start},
            )
            db.commit()
    else:
        resolved_week_start = persisted_week_start if persisted_week_start in {"monday", "sunday"} else "monday"

    territories = db.execute(text("SELECT id, name FROM territories ORDER BY name")).mappings().all()
    territory_id_value = parse_optional_int(territory_id)

    # CVM month entries are the primary planning source for this grid.
    cvm_rows = db.execute(
        text(
            """
            SELECT e.planned_date, e.completed_manual, c.id AS customer_id,
                   COALESCE(c.cust_code, '') AS cust_code,
                   COALESCE(c.name, 'Unassigned') AS customer_name,
                   COALESCE(c.trade_name, '') AS trade_name
            FROM cvm_month_entries e
            JOIN customers c ON c.id = e.customer_id
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

    for row in cvm_rows:
        planned_date = row["planned_date"]
        day = planned_date.day
        title = f"{row['cust_code']} {row['customer_name']}".strip()
        detail_parts = [x for x in [row["trade_name"]] if x]
        detail = f" ({' | '.join(detail_parts)})" if detail_parts else ""
        item_text = f"{title}{detail}"
        if bool(row["completed_manual"]):
            day_items[day]["completed"].append(item_text)
            completed_total += 1
        else:
            day_items[day]["planned"].append(item_text)
            planned_total += 1

    firstweekday = 0 if resolved_week_start == "monday" else 6
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
    if resolved_week_start == "sunday":
        weekday_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]

    prev_month = 12 if month == 1 else month - 1
    prev_year = year - 1 if month == 1 else year
    next_month = 1 if month == 12 else month + 1
    next_year = year + 1 if month == 12 else year

    return templates.TemplateResponse(
        "calendar.html",
        {
            "request": request,
            "year": year,
            "month": month,
            "month_name": calendar.month_name[month],
            "weeks": weeks,
            "weekday_names": weekday_names,
            "week_start_day": resolved_week_start,
            "planned_total": planned_total,
            "completed_total": completed_total,
            "territories": territories,
            "territory_id": territory_id_value,
            "prev_month": prev_month,
            "prev_year": prev_year,
            "next_month": next_month,
            "next_year": next_year,
            "today": calendar_today,
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
                   COALESCE(c.door_count, 0) AS door_count,
                   COALESCE(c.cvm_notes, '') AS cvm_notes,
                   COALESCE(c.group_name, '') AS sort_bucket
            FROM customers c
            LEFT JOIN territories t ON t.id = c.territory_id
            WHERE (:territory_id IS NULL OR c.territory_id = :territory_id)
            ORDER BY COALESCE(c.group_name, 'zzz'), c.name
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

    ensure_customer_exists(db, customer_id)
    planned_date_value = parse_optional_date(planned_date, "planned_date")
    is_completed = bool(completed_manual)
    has_date = planned_date_value is not None
    if is_completed and not has_date:
        is_completed = False

    try:
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
                      (:customer_id, :year, :month, :planned_date, :completed_manual, NOW())
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
                    "planned_date": planned_date_value,
                    "completed_manual": is_completed,
                },
            )

        db.commit()
    except (IntegrityError, DataError) as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail="Could not save CVM month entry. Check input values.") from exc
    except SQLAlchemyError as exc:
        db.rollback()
        logger.exception("Unexpected database error while saving CVM month entry")
        raise HTTPException(status_code=500, detail="Unexpected server error while saving CVM month entry.") from exc

    territory_id_value = parse_optional_int(territory_id)
    territory_param = f"&territory_id={territory_id_value}" if territory_id_value else ""
    return RedirectResponse(url=f"/cvm?year={year}{territory_param}", status_code=303)


@app.post("/cvm/notes-update")
def cvm_notes_update(
    customer_id: int = Form(...),
    notes: str = Form(""),
    year: int = Form(...),
    territory_id: str = Form(default=""),
    db: Session = Depends(get_db),
):
    ensure_customer_exists(db, customer_id)
    try:
        db.execute(
            text("UPDATE customers SET cvm_notes = NULLIF(:notes, '') WHERE id = :customer_id"),
            {"customer_id": customer_id, "notes": notes.strip()},
        )
        db.commit()
    except (IntegrityError, DataError) as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail="Could not save CVM notes. Check input values.") from exc
    except SQLAlchemyError as exc:
        db.rollback()
        logger.exception("Unexpected database error while saving CVM notes")
        raise HTTPException(status_code=500, detail="Unexpected server error while saving CVM notes.") from exc

    territory_id_value = parse_optional_int(territory_id)
    territory_param = f"&territory_id={territory_id_value}" if territory_id_value else ""
    return RedirectResponse(url=f"/cvm?year={year}{territory_param}", status_code=303)


@app.get("/import")
def import_page(request: Request):
    return templates.TemplateResponse(
        "import.html",
        {
            "request": request,
            "result": None,
            "error": "",
            "uploaded_name": "",
            "year_override": "",
            "import_mode": "preview",
            "upsert_policy": "merge",
            "validation_mode": "standard",
            "duplicate_policy": "last_wins",
        },
    )


@app.post("/import/workbook")
async def import_workbook(
    request: Request,
    workbook_file: UploadFile = File(...),
    year_override: str = Form(""),
    import_mode: str = Form("apply"),
    upsert_policy: str = Form("merge"),
    validation_mode: str = Form("standard"),
    duplicate_policy: str = Form("last_wins"),
    db: Session = Depends(get_db),
):
    mode = import_mode.strip().lower() or "apply"
    if mode not in {"preview", "apply"}:
        return templates.TemplateResponse(
            "import.html",
            {
                "request": request,
                "result": None,
                "error": "Invalid import mode. Use preview or apply.",
                "uploaded_name": workbook_file.filename or "",
                "year_override": year_override,
                "import_mode": mode,
                "upsert_policy": upsert_policy,
                "validation_mode": validation_mode,
                "duplicate_policy": duplicate_policy,
            },
            status_code=400,
        )

    year_value = None
    if year_override.strip():
        year_value = parse_optional_int(year_override)
        if year_value is None or year_value < 2000 or year_value > 2100:
            return templates.TemplateResponse(
                "import.html",
                {
                    "request": request,
                    "result": None,
                    "error": "Year override must be between 2000 and 2100.",
                    "uploaded_name": workbook_file.filename or "",
                    "year_override": year_override,
                    "import_mode": mode,
                    "upsert_policy": upsert_policy,
                    "validation_mode": validation_mode,
                    "duplicate_policy": duplicate_policy,
                },
                status_code=400,
            )

    payload = await workbook_file.read()
    try:
        result = import_planner_workbook(
            db,
            content=payload,
            filename=workbook_file.filename or "workbook.xlsx",
            year_override=year_value,
            upsert_policy=upsert_policy,
            validation_mode=validation_mode,
            duplicate_policy=duplicate_policy,
            dry_run=(mode == "preview"),
        )
        if mode == "preview":
            db.rollback()
        else:
            if result.get("can_apply", True):
                db.commit()
            else:
                db.rollback()
                return templates.TemplateResponse(
                    "import.html",
                    {
                        "request": request,
                        "result": result,
                        "error": "Import blocked by validation rules. Review blockers and row-level issues.",
                        "uploaded_name": workbook_file.filename or "",
                        "year_override": str(year_value or ""),
                        "import_mode": mode,
                        "upsert_policy": upsert_policy,
                        "validation_mode": validation_mode,
                        "duplicate_policy": duplicate_policy,
                    },
                    status_code=400,
                )
        return templates.TemplateResponse(
            "import.html",
            {
                "request": request,
                "result": result,
                "error": "",
                "uploaded_name": workbook_file.filename or "",
                "year_override": str(year_value or ""),
                "import_mode": mode,
                "upsert_policy": upsert_policy,
                "validation_mode": validation_mode,
                "duplicate_policy": duplicate_policy,
            },
        )
    except HTTPException as exc:
        db.rollback()
        return templates.TemplateResponse(
            "import.html",
            {
                "request": request,
                "result": None,
                "error": str(exc.detail),
                "uploaded_name": workbook_file.filename or "",
                "year_override": year_override,
                "import_mode": mode,
                "upsert_policy": upsert_policy,
                "validation_mode": validation_mode,
                "duplicate_policy": duplicate_policy,
            },
            status_code=400,
        )
    except (ValueError, TypeError) as exc:
        db.rollback()
        return templates.TemplateResponse(
            "import.html",
            {
                "request": request,
                "result": None,
                "error": f"Import failed: {exc}",
                "uploaded_name": workbook_file.filename or "",
                "year_override": year_override,
                "import_mode": mode,
                "upsert_policy": upsert_policy,
                "validation_mode": validation_mode,
                "duplicate_policy": duplicate_policy,
            },
            status_code=400,
        )
    except SQLAlchemyError:
        db.rollback()
        logger.exception("Unexpected database error during workbook import")
        return templates.TemplateResponse(
            "import.html",
            {
                "request": request,
                "result": None,
                "error": "Unexpected import database error.",
                "uploaded_name": workbook_file.filename or "",
                "year_override": year_override,
                "import_mode": mode,
                "upsert_policy": upsert_policy,
                "validation_mode": validation_mode,
                "duplicate_policy": duplicate_policy,
            },
            status_code=500,
        )


@app.get("/api/customers")
def list_customers(db: Session = Depends(get_db)):
    rows = db.execute(
        text(
            """
            SELECT c.id, c.cust_code, c.name, c.trade_name, c.group_name, c.iws_code, c.door_count,
                   t.name AS territory
            FROM customers c
            LEFT JOIN territories t ON t.id = c.territory_id
            ORDER BY c.cust_code
            """
        )
    ).mappings().all()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/products")
def list_products(db: Session = Depends(get_db)):
    rows = db.execute(
        text(
            """
            SELECT p.id,
                   p.product_name,
                   lv.last_visit,
                   p.action,
                   p.status,
                   p.next_action,
                   p.last_contact,
                   p.notes,
                   c.cust_code, c.name AS customer_name, COALESCE(t.name, '') AS territory
            FROM products p
            JOIN customers c ON c.id = p.customer_id
            LEFT JOIN territories t ON t.id = c.territory_id
            LEFT JOIN (
              SELECT e.customer_id, MAX(e.planned_date) AS last_visit
              FROM cvm_month_entries e
              WHERE e.completed_manual = TRUE
                AND e.planned_date IS NOT NULL
              GROUP BY e.customer_id
            ) lv ON lv.customer_id = p.customer_id
            ORDER BY c.cust_code, p.product_name
            LIMIT 1000
            """
        )
    ).mappings().all()
    return {"items": [dict(r) for r in rows]}
