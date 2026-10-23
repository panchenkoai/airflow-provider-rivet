-- Demo source for the quickstart: the `shop` database (created by POSTGRES_DB) with two tables.
-- rivet's PostgreSQL CDC reads a logical slot over an ordinary connection: the role needs REPLICATION, the server
-- wal_level=logical (set on the command line in the compose file).
CREATE ROLE rivet LOGIN REPLICATION PASSWORD 'rivet_demo';

CREATE TABLE customers (
  id         SERIAL PRIMARY KEY,
  name       VARCHAR(100) NOT NULL,
  tier       VARCHAR(10)  NOT NULL,
  created_at TIMESTAMP(6) NOT NULL DEFAULT now(),
  updated_at TIMESTAMP(6) NOT NULL DEFAULT now()
);

CREATE TABLE orders (
  id          SERIAL PRIMARY KEY,
  customer_id INT           NOT NULL,
  amount      NUMERIC(12,2) NOT NULL,
  status      VARCHAR(20)   NOT NULL,
  created_at  TIMESTAMP(6)  NOT NULL DEFAULT now(),
  updated_at  TIMESTAMP(6)  NOT NULL DEFAULT now()
);

-- updated_at moves on every UPDATE, as the batch (incremental) pipeline's cursor needs.
CREATE FUNCTION touch_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at := now(); RETURN NEW; END $$;
CREATE TRIGGER customers_touch BEFORE UPDATE ON customers FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER orders_touch BEFORE UPDATE ON orders FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

INSERT INTO customers (name, tier)
  SELECT 'customer ' || n, (ARRAY['free', 'pro', 'team'])[1 + n % 3] FROM generate_series(1, 1000) n;
INSERT INTO orders (customer_id, amount, status)
  SELECT 1 + n % 1000, round(5 + (n * 37 % 9500) / 10.0, 2), (ARRAY['new', 'paid', 'shipped'])[1 + n % 3]
  FROM generate_series(1, 5000) n;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO rivet;
