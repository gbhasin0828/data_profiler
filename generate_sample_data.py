"""
generate_sample_data.py — creates sample databases for local testing.

Usage:
    python generate_sample_data.py

Creates:
    sample/data.duckdb   — 3 schemas, 9 tables, ~50k rows
    sample/data.sqlite   — same structure, lighter row counts
"""

import os
import random
import sqlite3
from datetime import datetime, timedelta

import duckdb

os.makedirs("sample", exist_ok=True)

STATUSES      = ["PENDING", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
CATEGORIES    = ["Electronics", "Tools", "Clothing", "Food", "Misc"]
PAYMENT_TYPES = ["CREDIT_CARD", "DEBIT_CARD", "BANK_TRANSFER", "PAYPAL"]
WAREHOUSES    = ["Chicago", "Dallas", "Seattle", "New York", "Atlanta"]
SUPPLIERS     = ["Acme Corp", "Global Supply", "FastShip Inc", "MegaWare", "QuickStock"]

def random_date(start_year=2022, end_year=2024) -> str:
    start = datetime(start_year, 1, 1)
    end   = datetime(end_year, 12, 31)
    return (start + timedelta(days=random.randint(0, (end - start).days))).strftime("%Y-%m-%d %H:%M:%S")

def maybe_null(value, pct=10):
    """Return None pct% of the time to simulate real-world nulls."""
    return None if random.randint(1, 100) <= pct else value


# -----------------------------------------------------------------------
# DuckDB — 3 schemas, 9 tables, ~50k rows total
# -----------------------------------------------------------------------
print("Creating sample/data.duckdb ...")
con = duckdb.connect("sample/data.duckdb")

con.execute("CREATE SCHEMA IF NOT EXISTS sales")
con.execute("CREATE SCHEMA IF NOT EXISTS finance")
con.execute("CREATE SCHEMA IF NOT EXISTS inventory")

# --- sales.customers ---
con.execute("DROP TABLE IF EXISTS sales.customers")
con.execute("""
    CREATE TABLE sales.customers (
        customer_id   INTEGER,
        first_name    VARCHAR,
        last_name     VARCHAR,
        email         VARCHAR,
        city          VARCHAR,
        country       VARCHAR,
        created_at    TIMESTAMP
    )
""")
customers = [
    (i, f"First{i}", f"Last{i}", maybe_null(f"user{i}@email.com", 5),
     maybe_null(random.choice(WAREHOUSES)), "USA", random_date())
    for i in range(1, 5001)
]
con.executemany("INSERT INTO sales.customers VALUES (?,?,?,?,?,?,?)", customers)

# --- sales.products ---
con.execute("DROP TABLE IF EXISTS sales.products")
con.execute("""
    CREATE TABLE sales.products (
        product_id    INTEGER,
        product_name  VARCHAR,
        category      VARCHAR,
        price         DOUBLE,
        cost          DOUBLE,
        is_active     BOOLEAN
    )
""")
products = [
    (i, f"Product {i}", random.choice(CATEGORIES),
     round(random.uniform(5, 500), 2),
     maybe_null(round(random.uniform(1, 200), 2), 8),
     random.choice([True, False]))
    for i in range(1, 501)
]
con.executemany("INSERT INTO sales.products VALUES (?,?,?,?,?,?)", products)

# --- sales.orders ---
con.execute("DROP TABLE IF EXISTS sales.orders")
con.execute("""
    CREATE TABLE sales.orders (
        order_id      INTEGER,
        customer_id   INTEGER,
        status        VARCHAR,
        total_amount  DOUBLE,
        discount      DOUBLE,
        created_at    TIMESTAMP,
        shipped_at    TIMESTAMP
    )
""")
orders = [
    (i, random.randint(1, 5000), random.choice(STATUSES),
     round(random.uniform(10, 2000), 2),
     maybe_null(round(random.uniform(0, 50), 2), 30),
     random_date(), maybe_null(random_date(), 20))
    for i in range(1, 20001)
]
con.executemany("INSERT INTO sales.orders VALUES (?,?,?,?,?,?,?)", orders)

# --- sales.order_items ---
con.execute("DROP TABLE IF EXISTS sales.order_items")
con.execute("""
    CREATE TABLE sales.order_items (
        item_id       INTEGER,
        order_id      INTEGER,
        product_id    INTEGER,
        quantity      INTEGER,
        unit_price    DOUBLE,
        line_total    DOUBLE
    )
""")
items = [
    (i, random.randint(1, 20000), random.randint(1, 500),
     random.randint(1, 10),
     round(random.uniform(5, 500), 2),
     round(random.uniform(5, 5000), 2))
    for i in range(1, 30001)
]
con.executemany("INSERT INTO sales.order_items VALUES (?,?,?,?,?,?)", items)

# --- finance.invoices ---
con.execute("DROP TABLE IF EXISTS finance.invoices")
con.execute("""
    CREATE TABLE finance.invoices (
        invoice_id    INTEGER,
        order_id      INTEGER,
        amount        DOUBLE,
        tax           DOUBLE,
        issued_at     TIMESTAMP,
        due_at        TIMESTAMP,
        paid_at       TIMESTAMP
    )
""")
invoices = [
    (i, random.randint(1, 20000),
     round(random.uniform(10, 2000), 2),
     maybe_null(round(random.uniform(0, 200), 2), 5),
     random_date(), random_date(), maybe_null(random_date(), 25))
    for i in range(1, 10001)
]
con.executemany("INSERT INTO finance.invoices VALUES (?,?,?,?,?,?,?)", invoices)

# --- finance.payments ---
con.execute("DROP TABLE IF EXISTS finance.payments")
con.execute("""
    CREATE TABLE finance.payments (
        payment_id    INTEGER,
        invoice_id    INTEGER,
        amount        DOUBLE,
        payment_type  VARCHAR,
        paid_at       TIMESTAMP
    )
""")
payments = [
    (i, random.randint(1, 10000),
     round(random.uniform(10, 2000), 2),
     random.choice(PAYMENT_TYPES),
     maybe_null(random_date(), 10))
    for i in range(1, 8001)
]
con.executemany("INSERT INTO finance.payments VALUES (?,?,?,?,?)", payments)

# --- finance.ledger ---
con.execute("DROP TABLE IF EXISTS finance.ledger")
con.execute("""
    CREATE TABLE finance.ledger (
        ledger_id     INTEGER,
        payment_id    INTEGER,
        debit         DOUBLE,
        credit        DOUBLE,
        balance       DOUBLE,
        posted_at     TIMESTAMP
    )
""")
ledger = [
    (i, random.randint(1, 8000),
     maybe_null(round(random.uniform(0, 2000), 2), 20),
     maybe_null(round(random.uniform(0, 2000), 2), 20),
     round(random.uniform(-500, 5000), 2),
     random_date())
    for i in range(1, 8001)
]
con.executemany("INSERT INTO finance.ledger VALUES (?,?,?,?,?,?)", ledger)

# --- inventory.warehouses ---
con.execute("DROP TABLE IF EXISTS inventory.warehouses")
con.execute("""
    CREATE TABLE inventory.warehouses (
        warehouse_id  INTEGER,
        name          VARCHAR,
        city          VARCHAR,
        capacity      INTEGER,
        is_active     BOOLEAN
    )
""")
con.executemany("INSERT INTO inventory.warehouses VALUES (?,?,?,?,?)", [
    (i+1, WAREHOUSES[i], WAREHOUSES[i], random.randint(1000, 50000), True)
    for i in range(len(WAREHOUSES))
])

# --- inventory.stock_levels ---
con.execute("DROP TABLE IF EXISTS inventory.stock_levels")
con.execute("""
    CREATE TABLE inventory.stock_levels (
        stock_id      INTEGER,
        warehouse_id  INTEGER,
        product_id    INTEGER,
        quantity      INTEGER,
        reorder_level INTEGER,
        updated_at    TIMESTAMP
    )
""")
stock = [
    (i, random.randint(1, 5), random.randint(1, 500),
     random.randint(0, 10000),
     maybe_null(random.randint(10, 500), 15),
     random_date())
    for i in range(1, 5001)
]
con.executemany("INSERT INTO inventory.stock_levels VALUES (?,?,?,?,?,?)", stock)

# --- inventory.suppliers ---
con.execute("DROP TABLE IF EXISTS inventory.suppliers")
con.execute("""
    CREATE TABLE inventory.suppliers (
        supplier_id   INTEGER,
        name          VARCHAR,
        contact_email VARCHAR,
        country       VARCHAR,
        rating        DOUBLE
    )
""")
con.executemany("INSERT INTO inventory.suppliers VALUES (?,?,?,?,?)", [
    (i+1, SUPPLIERS[i], maybe_null(f"contact@{SUPPLIERS[i].lower().replace(' ','')}.com", 10),
     "USA", maybe_null(round(random.uniform(1, 5), 1), 5))
    for i in range(len(SUPPLIERS))
])

con.close()
print("  Done — 3 schemas, 9 tables")


# -----------------------------------------------------------------------
# SQLite — same structure, lighter row counts (SQLite is slower to insert)
# -----------------------------------------------------------------------
print("Creating sample/data.sqlite ...")
con = sqlite3.connect("sample/data.sqlite")

con.executescript("""
    DROP TABLE IF EXISTS customers;
    DROP TABLE IF EXISTS products;
    DROP TABLE IF EXISTS orders;
    DROP TABLE IF EXISTS order_items;
    DROP TABLE IF EXISTS invoices;
    DROP TABLE IF EXISTS payments;

    CREATE TABLE customers (
        customer_id INTEGER, first_name TEXT, last_name TEXT,
        email TEXT, city TEXT, country TEXT, created_at TEXT);

    CREATE TABLE products (
        product_id INTEGER, product_name TEXT, category TEXT,
        price REAL, cost REAL, is_active INTEGER);

    CREATE TABLE orders (
        order_id INTEGER, customer_id INTEGER, status TEXT,
        total_amount REAL, discount REAL, created_at TEXT, shipped_at TEXT);

    CREATE TABLE order_items (
        item_id INTEGER, order_id INTEGER, product_id INTEGER,
        quantity INTEGER, unit_price REAL, line_total REAL);

    CREATE TABLE invoices (
        invoice_id INTEGER, order_id INTEGER, amount REAL,
        tax REAL, issued_at TEXT, due_at TEXT, paid_at TEXT);

    CREATE TABLE payments (
        payment_id INTEGER, invoice_id INTEGER, amount REAL,
        payment_type TEXT, paid_at TEXT);
""")

con.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?,?)",
    [(i, f"First{i}", f"Last{i}", maybe_null(f"user{i}@email.com", 5),
      maybe_null(random.choice(WAREHOUSES)), "USA", random_date())
     for i in range(1, 1001)])

con.executemany("INSERT INTO products VALUES (?,?,?,?,?,?)",
    [(i, f"Product {i}", random.choice(CATEGORIES),
      round(random.uniform(5, 500), 2),
      maybe_null(round(random.uniform(1, 200), 2), 8),
      random.choice([1, 0]))
     for i in range(1, 201)])

con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?)",
    [(i, random.randint(1, 1000), random.choice(STATUSES),
      round(random.uniform(10, 2000), 2),
      maybe_null(round(random.uniform(0, 50), 2), 30),
      random_date(), maybe_null(random_date(), 20))
     for i in range(1, 3001)])

con.executemany("INSERT INTO order_items VALUES (?,?,?,?,?,?)",
    [(i, random.randint(1, 3000), random.randint(1, 200),
      random.randint(1, 10),
      round(random.uniform(5, 500), 2),
      round(random.uniform(5, 5000), 2))
     for i in range(1, 5001)])

con.executemany("INSERT INTO invoices VALUES (?,?,?,?,?,?,?)",
    [(i, random.randint(1, 3000),
      round(random.uniform(10, 2000), 2),
      maybe_null(round(random.uniform(0, 200), 2), 5),
      random_date(), random_date(), maybe_null(random_date(), 25))
     for i in range(1, 2001)])

con.executemany("INSERT INTO payments VALUES (?,?,?,?,?)",
    [(i, random.randint(1, 2000),
      round(random.uniform(10, 2000), 2),
      random.choice(PAYMENT_TYPES),
      maybe_null(random_date(), 10))
     for i in range(1, 1501)])

con.commit()
con.close()
print("  Done — 6 tables")

print()
print("Sample data ready. Run the profiler with:")
print("  python -m profiler run --config config.yaml")