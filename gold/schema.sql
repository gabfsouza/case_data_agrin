-- Tabelas da camada Silver (para referência)
CREATE TABLE IF NOT EXISTS carts (
    id INTEGER PRIMARY KEY,
    userId INTEGER,
    date TEXT,
    products TEXT,
    __v INTEGER
);

CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY,
    category TEXT,
    description TEXT,
    image TEXT,
    price REAL,
    rating TEXT,
    title TEXT
);
-- Tabelas da camada Gold (indicadores e métricas)

-- Top 10 produtos por receita
CREATE TABLE IF NOT EXISTS top10_products_revenue (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT,
    unit_price REAL,
    total_units_sold INTEGER,
    total_revenue REAL
);

-- Vendas diárias (resumo por dia)
CREATE TABLE IF NOT EXISTS sales_daily (
    date TEXT PRIMARY KEY,
    total_revenue REAL NOT NULL,
    total_orders INTEGER NOT NULL,
    total_items INTEGER NOT NULL,
    avg_ticket REAL,
    created_at TEXT
);

-- Performance de produtos
CREATE TABLE IF NOT EXISTS product_performance (
    product_id INTEGER PRIMARY KEY,
    product_title TEXT NOT NULL,
    category TEXT,
    total_quantity INTEGER,
    total_revenue REAL,
    avg_price REAL,
    rating_rate REAL,
    rating_count INTEGER,
    created_at TEXT
);

-- Performance de categorias
CREATE TABLE IF NOT EXISTS category_performance (
    category TEXT PRIMARY KEY,
    total_revenue REAL NOT NULL,
    total_quantity INTEGER NOT NULL,
    revenue_share_pct REAL,
    created_at TEXT
);

-- Lifetime Value (LTV) de usuários
CREATE TABLE IF NOT EXISTS user_ltv (
    user_id INTEGER PRIMARY KEY,
    total_orders INTEGER NOT NULL,
    total_items INTEGER NOT NULL,
    total_revenue REAL NOT NULL,
    avg_ticket REAL,
    customer_type TEXT NOT NULL,  -- 'new', 'recurrent', 'heavy'
    created_at TEXT
);
