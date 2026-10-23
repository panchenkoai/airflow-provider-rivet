-- Demo changes for the second run: 20 new customers, 30 orders shipped, 5 orders deleted.
INSERT INTO customers (name, tier) SELECT CONCAT('new customer ', n), 'pro' FROM (
  WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 20) SELECT n FROM g) s;
UPDATE orders SET status = 'shipped' WHERE status = 'new' ORDER BY id LIMIT 30;
DELETE FROM orders WHERE id <= 5;
