-- Demo source for the quickstart: the `shop` database (created by MYSQL_DATABASE) with two tables.
-- rivet's MySQL CDC reads the binlog: REPLICATION SLAVE and REPLICATION CLIENT are server-wide grants.
CREATE USER IF NOT EXISTS 'rivet'@'%' IDENTIFIED BY 'rivet_demo';
GRANT SELECT ON shop.* TO 'rivet'@'%';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'rivet'@'%';

USE shop;
SET SESSION cte_max_recursion_depth = 10000;

CREATE TABLE customers (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  name       VARCHAR(100) NOT NULL,
  tier       VARCHAR(10)  NOT NULL,
  created_at DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
);

CREATE TABLE orders (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  customer_id INT           NOT NULL,
  amount      DECIMAL(12,2) NOT NULL,
  status      VARCHAR(20)   NOT NULL,
  created_at  DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at  DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
);

INSERT INTO customers (name, tier)
  WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 1000)
  SELECT CONCAT('customer ', n), ELT(1 + n % 3, 'free', 'pro', 'team') FROM g;

INSERT INTO orders (customer_id, amount, status)
  WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 5000)
  SELECT 1 + n % 1000, ROUND(5 + (n * 37 % 9500) / 10, 2), ELT(1 + n % 3, 'new', 'paid', 'shipped') FROM g;
