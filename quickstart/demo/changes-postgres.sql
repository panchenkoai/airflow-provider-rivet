-- Demo changes for the second run: 20 new customers, 30 orders shipped, 5 orders deleted.
INSERT INTO customers (name, tier) SELECT 'new customer ' || n, 'pro' FROM generate_series(1, 20) n;
UPDATE orders SET status = 'shipped' WHERE id IN (SELECT id FROM orders WHERE status = 'new' ORDER BY id LIMIT 30);
DELETE FROM orders WHERE id <= 5;
