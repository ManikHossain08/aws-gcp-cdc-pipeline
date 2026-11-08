-- Demo source schema. Every replicated table MUST have a primary key (CDC identity).

CREATE TABLE IF NOT EXISTS public.customers (
    customer_id   BIGSERIAL    PRIMARY KEY,
    email         VARCHAR(320) NOT NULL UNIQUE,
    full_name     VARCHAR(200),
    country_code  CHAR(2),
    status        VARCHAR(20)  NOT NULL DEFAULT 'active'
                  CHECK (status IN ('active', 'suspended', 'closed')),
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS public.orders (
    order_id      BIGSERIAL     PRIMARY KEY,
    customer_id   BIGINT        NOT NULL REFERENCES public.customers (customer_id),
    order_date    DATE          NOT NULL DEFAULT current_date,
    status        VARCHAR(20)   NOT NULL
                  CHECK (status IN ('NEW', 'PAID', 'SHIPPED', 'DELIVERED', 'CANCELLED')),
    total_amount  NUMERIC(12,2) CHECK (total_amount >= 0),
    currency      CHAR(3)       NOT NULL DEFAULT 'USD',
    metadata      JSONB,
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS orders_customer_idx ON public.orders (customer_id);

-- Keep updated_at honest so the replica can be compared with the source.
CREATE OR REPLACE FUNCTION public.touch_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS customers_touch ON public.customers;
CREATE TRIGGER customers_touch BEFORE UPDATE ON public.customers
    FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();

DROP TRIGGER IF EXISTS orders_touch ON public.orders;
CREATE TRIGGER orders_touch BEFORE UPDATE ON public.orders
    FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();
