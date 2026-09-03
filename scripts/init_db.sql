-- Event-Driven Platform - Database Initialization
CREATE SCHEMA IF NOT EXISTS public;

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    product_id VARCHAR NOT NULL,
    quantity INTEGER NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    status VARCHAR DEFAULT 'pending',
    idempotency_key VARCHAR UNIQUE,
    correlation_id VARCHAR,
    payment_id VARCHAR,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_idempotency ON orders(idempotency_key);

-- Payments table
CREATE TABLE IF NOT EXISTS payments (
    payment_id VARCHAR PRIMARY KEY,
    order_id VARCHAR NOT NULL,
    customer_id VARCHAR NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    status VARCHAR DEFAULT 'pending',
    transaction_id VARCHAR,
    payment_method VARCHAR DEFAULT 'credit_card',
    failure_reason VARCHAR,
    idempotency_key VARCHAR UNIQUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_payments_order ON payments(order_id);
CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status);

-- Inventory table
CREATE TABLE IF NOT EXISTS inventory (
    product_id VARCHAR PRIMARY KEY,
    product_name VARCHAR NOT NULL,
    quantity_available INTEGER DEFAULT 0,
    quantity_reserved INTEGER DEFAULT 0,
    unit_price INTEGER DEFAULT 0,
    version INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Reservations table
CREATE TABLE IF NOT EXISTS reservations (
    reservation_id VARCHAR PRIMARY KEY,
    order_id VARCHAR NOT NULL,
    product_id VARCHAR NOT NULL,
    quantity INTEGER NOT NULL,
    status VARCHAR DEFAULT 'active',
    created_at TIMESTAMP DEFAULT NOW(),
    expires_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_reservations_order ON reservations(order_id);
CREATE INDEX IF NOT EXISTS idx_reservations_status ON reservations(status);

-- Seed inventory
INSERT INTO inventory (product_id, product_name, quantity_available, unit_price)
VALUES
    ('PROD-001', 'Wireless Headphones', 150, 4999),
    ('PROD-002', 'USB-C Cable', 500, 1299),
    ('PROD-003', 'Laptop Stand', 75, 7999),
    ('PROD-004', 'Mechanical Keyboard', 200, 12999),
    ('PROD-005', 'Webcam HD', 100, 8999)
ON CONFLICT (product_id) DO NOTHING;
