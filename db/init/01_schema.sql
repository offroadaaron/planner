CREATE TABLE IF NOT EXISTS territories (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS customers (
  id BIGSERIAL PRIMARY KEY,
  cust_code TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  trade_name TEXT,
  territory_id BIGINT REFERENCES territories(id),
  group_name TEXT,
  group_2_iws TEXT,
  iws_code TEXT,
  old_value TEXT,
  old_name TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stores (
  id BIGSERIAL PRIMARY KEY,
  customer_id BIGINT REFERENCES customers(id) ON DELETE SET NULL,
  address_1 TEXT,
  address_2 TEXT,
  city TEXT,
  state TEXT,
  postcode TEXT,
  country TEXT,
  main_contact TEXT,
  owner_name TEXT,
  owner_phone TEXT,
  owner_email TEXT,
  store_manager_name TEXT,
  store_phone TEXT,
  store_email TEXT,
  market_manager_name TEXT,
  marketing_phone TEXT,
  marketing_email TEXT,
  account_dept_name TEXT,
  accounting_phone TEXT,
  accounting_email TEXT,
  sort_bucket TEXT,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reference_values (
  id BIGSERIAL PRIMARY KEY,
  category TEXT NOT NULL,
  value TEXT NOT NULL,
  sort_order INT NOT NULL DEFAULT 0,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  UNIQUE(category, value)
);

CREATE TABLE IF NOT EXISTS calendar_settings (
  id SMALLINT PRIMARY KEY DEFAULT 1,
  calendar_year INT NOT NULL,
  week_start_day TEXT NOT NULL DEFAULT 'monday',
  CHECK (week_start_day IN ('monday', 'sunday')),
  CHECK (id = 1)
);

CREATE TABLE IF NOT EXISTS public_holidays (
  id BIGSERIAL PRIMARY KEY,
  holiday_date DATE NOT NULL,
  name TEXT NOT NULL,
  territory_id BIGINT REFERENCES territories(id),
  UNIQUE (holiday_date, name, territory_id)
);

CREATE TABLE IF NOT EXISTS annual_leaves (
  id BIGSERIAL PRIMARY KEY,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  rep_name TEXT,
  notes TEXT,
  territory_id BIGINT REFERENCES territories(id),
  CHECK (end_date >= start_date)
);

CREATE TABLE IF NOT EXISTS visit_events (
  id BIGSERIAL PRIMARY KEY,
  customer_id BIGINT REFERENCES customers(id) ON DELETE CASCADE,
  store_id BIGINT REFERENCES stores(id) ON DELETE SET NULL,
  event_type TEXT NOT NULL,
  event_date DATE NOT NULL,
  action TEXT,
  status TEXT,
  next_action TEXT,
  last_contact DATE,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (event_type IN ('planned', 'completed', 'annual_leave', 'public_holiday', 'note'))
);

CREATE INDEX IF NOT EXISTS idx_customers_territory_id ON customers (territory_id);
CREATE INDEX IF NOT EXISTS idx_stores_customer_id ON stores (customer_id);
CREATE INDEX IF NOT EXISTS idx_visit_events_event_date ON visit_events (event_date);
CREATE INDEX IF NOT EXISTS idx_visit_events_customer_id ON visit_events (customer_id);
CREATE INDEX IF NOT EXISTS idx_reference_values_category ON reference_values (category);

INSERT INTO reference_values (category, value, sort_order)
VALUES
  ('shown', 'SHOWN', 1),
  ('shown', 'NOT SHOWN', 2),
  ('shown', 'COMPLETED', 3),
  ('status', 'ORDERED', 1),
  ('status', 'NOT ORDERING', 2),
  ('status', 'FOLLOW UP REQUIRED', 3),
  ('action', 'EMAIL', 1),
  ('action', 'CALL', 2),
  ('action', 'NO ACTION REQ', 3),
  ('action', 'IN PERSON VISIT', 4)
ON CONFLICT (category, value) DO NOTHING;

INSERT INTO calendar_settings (id, calendar_year, week_start_day)
VALUES (1, EXTRACT(YEAR FROM NOW())::INT, 'monday')
ON CONFLICT (id) DO NOTHING;
