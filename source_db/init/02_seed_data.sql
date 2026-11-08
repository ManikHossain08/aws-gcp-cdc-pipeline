-- Realistic, deterministic seed data (the DMS full load picks this up).

INSERT INTO public.customers (customer_id, email, full_name, country_code, status, created_at) VALUES
    (1, 'ada.lovelace@example.com',   'Ada Lovelace',    'GB', 'active', '2026-10-01 09:00:00+00'),
    (2, 'alan.turing@example.com',    'Alan Turing',     'GB', 'active', '2026-10-02 10:30:00+00'),
    (3, 'grace.hopper@example.com',   'Grace Hopper',    'US', 'active', '2026-10-03 11:15:00+00'),
    (4, 'katherine.j@example.com',    'Katherine Johnson','US', 'active', '2026-10-04 08:45:00+00'),
    (5, 'edsger.d@example.com',       'Edsger Dijkstra', 'NL', 'suspended', '2026-10-05 14:00:00+00')
ON CONFLICT (customer_id) DO NOTHING;

INSERT INTO public.orders (order_id, customer_id, order_date, status, total_amount, currency, metadata, created_at) VALUES
    (101, 1, '2026-11-01', 'PAID',      120.50, 'GBP', '{"channel": "web"}',    '2026-11-01 09:10:00+00'),
    (102, 1, '2026-11-02', 'SHIPPED',    35.00, 'GBP', '{"channel": "mobile"}', '2026-11-02 12:00:00+00'),
    (103, 2, '2026-11-02', 'NEW',       999.99, 'GBP', '{"channel": "web"}',    '2026-11-02 15:30:00+00'),
    (104, 3, '2026-11-03', 'DELIVERED',  42.00, 'USD', '{"channel": "store"}',  '2026-11-03 16:45:00+00'),
    (105, 4, '2026-11-04', 'NEW',        15.25, 'USD', NULL,                    '2026-11-04 18:20:00+00')
ON CONFLICT (order_id) DO NOTHING;

-- Move sequences past the explicit ids.
SELECT setval('public.customers_customer_id_seq', (SELECT max(customer_id) FROM public.customers));
SELECT setval('public.orders_order_id_seq',       (SELECT max(order_id)    FROM public.orders));
