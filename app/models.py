from datetime import date, datetime

from sqlalchemy import Boolean, CheckConstraint, Date, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class Territory(Base):
    __tablename__ = "territories"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(Text, unique=True)


class Customer(Base):
    __tablename__ = "customers"

    id: Mapped[int] = mapped_column(primary_key=True)
    cust_code: Mapped[str] = mapped_column(String(64), unique=True)
    name: Mapped[str] = mapped_column(Text)
    trade_name: Mapped[str | None] = mapped_column(Text)
    territory_id: Mapped[int | None] = mapped_column(ForeignKey("territories.id"))
    group_name: Mapped[str | None] = mapped_column(Text)
    group_2_iws: Mapped[str | None] = mapped_column(Text)
    iws_code: Mapped[str | None] = mapped_column(Text)
    old_value: Mapped[str | None] = mapped_column(Text)
    old_name: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class Store(Base):
    __tablename__ = "stores"

    id: Mapped[int] = mapped_column(primary_key=True)
    customer_id: Mapped[int | None] = mapped_column(ForeignKey("customers.id", ondelete="SET NULL"))
    address_1: Mapped[str | None] = mapped_column(Text)
    address_2: Mapped[str | None] = mapped_column(Text)
    city: Mapped[str | None] = mapped_column(Text)
    state: Mapped[str | None] = mapped_column(Text)
    postcode: Mapped[str | None] = mapped_column(String(32))
    country: Mapped[str | None] = mapped_column(Text)
    main_contact: Mapped[str | None] = mapped_column(Text)
    owner_name: Mapped[str | None] = mapped_column(Text)
    owner_phone: Mapped[str | None] = mapped_column(Text)
    owner_email: Mapped[str | None] = mapped_column(Text)
    store_manager_name: Mapped[str | None] = mapped_column(Text)
    store_phone: Mapped[str | None] = mapped_column(Text)
    store_email: Mapped[str | None] = mapped_column(Text)
    market_manager_name: Mapped[str | None] = mapped_column(Text)
    marketing_phone: Mapped[str | None] = mapped_column(Text)
    marketing_email: Mapped[str | None] = mapped_column(Text)
    account_dept_name: Mapped[str | None] = mapped_column(Text)
    accounting_phone: Mapped[str | None] = mapped_column(Text)
    accounting_email: Mapped[str | None] = mapped_column(Text)
    sort_bucket: Mapped[str | None] = mapped_column(Text)
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ReferenceValue(Base):
    __tablename__ = "reference_values"

    id: Mapped[int] = mapped_column(primary_key=True)
    category: Mapped[str] = mapped_column(Text)
    value: Mapped[str] = mapped_column(Text)
    sort_order: Mapped[int] = mapped_column(Integer)
    active: Mapped[bool] = mapped_column(Boolean)


class CalendarSetting(Base):
    __tablename__ = "calendar_settings"
    __table_args__ = (
        CheckConstraint("week_start_day IN ('monday','sunday')", name="calendar_week_start_chk"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    calendar_year: Mapped[int] = mapped_column(Integer)
    week_start_day: Mapped[str] = mapped_column(String(16))


class PublicHoliday(Base):
    __tablename__ = "public_holidays"

    id: Mapped[int] = mapped_column(primary_key=True)
    holiday_date: Mapped[date] = mapped_column(Date)
    name: Mapped[str] = mapped_column(Text)
    territory_id: Mapped[int | None] = mapped_column(ForeignKey("territories.id"))


class AnnualLeave(Base):
    __tablename__ = "annual_leaves"

    id: Mapped[int] = mapped_column(primary_key=True)
    start_date: Mapped[date] = mapped_column(Date)
    end_date: Mapped[date] = mapped_column(Date)
    rep_name: Mapped[str | None] = mapped_column(Text)
    notes: Mapped[str | None] = mapped_column(Text)
    territory_id: Mapped[int | None] = mapped_column(ForeignKey("territories.id"))


class VisitEvent(Base):
    __tablename__ = "visit_events"
    __table_args__ = (
        CheckConstraint(
            "event_type IN ('planned','completed','annual_leave','public_holiday','note')",
            name="visit_event_type_chk",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    customer_id: Mapped[int | None] = mapped_column(ForeignKey("customers.id", ondelete="CASCADE"))
    store_id: Mapped[int | None] = mapped_column(ForeignKey("stores.id", ondelete="SET NULL"))
    event_type: Mapped[str] = mapped_column(String(32))
    event_date: Mapped[date] = mapped_column(Date)
    action: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str | None] = mapped_column(Text)
    next_action: Mapped[str | None] = mapped_column(Text)
    last_contact: Mapped[date | None] = mapped_column(Date)
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
