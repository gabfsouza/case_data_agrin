from pyspark.sql import SparkSession
#from pyspark.sql.functions import col, sum, avg, max, min, count
import pyspark.sql.functions as F
import os
import sqlite3
import pandas as pd

import sys
# Se estiver rodando no Windows (fora do container), aplica o fix para windows
if sys.platform == 'win32':
    import socketserver
    if not hasattr(socketserver, 'UnixStreamServer'):
        class UnixStreamServer(socketserver.TCPServer):
            address_family = None
            def __init__(self, *args, **kwargs):
                pass
        socketserver.UnixStreamServer = UnixStreamServer


def get_minio_endpoint():
    if os.path.exists('/.dockerenv'):
        return "http://minio:9000"  # Dentro do container
    return "http://localhost:9000"  # Localmente

MINIO_ENDPOINT = get_minio_endpoint()


spark = SparkSession.builder \
    .appName("modeling_indicators") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED") \
    .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .getOrCreate()


df_carts = spark.read.parquet("s3a://datalake/silver/produtos_carts/")
df_products = spark.read.parquet("s3a://datalake/silver/produtos_products/")

df_carts.createOrReplaceTempView("silver_carts")
df_products.createOrReplaceTempView("silver_products")

temp_view_sales = spark.sql("""
    
    SELECT
        c.id                      AS cart_id,
        c.userId                  AS user_id,
        c.date                    AS date,
        c.year                    AS year,
        c.month                   AS month,
        p.id                      AS product_id,
        p.title                   AS product_title,
        p.category                AS category,
        p.price                   AS price,
        p.rating_rate             AS rating_rate,
        p.rating_count            AS rating_count,
        c.products_quantity       AS quantity,
        (p.price * c.products_quantity) AS revenue
    FROM silver_carts c
    left JOIN silver_products p ON c.products_productId = p.id
""")

temp_view_sales.createOrReplaceTempView("vw_sales")

# indicadores
# top 10 produtos por receita
df_top10_products_revenue = spark.sql("""
    SELECT
        p.id                      AS product_id,
        p.title                   AS product_name,
        p.category                AS category,
        p.price                   AS unit_price,
        SUM(c.products_quantity)  AS total_units_sold,
        SUM(p.price * c.products_quantity) AS total_revenue
    FROM silver_carts c
    INNER JOIN silver_products p
        ON c.products_productId = p.id
    GROUP BY
        p.id,
        p.title,
        p.category,
        p.price
    ORDER BY total_revenue DESC
    LIMIT 10
""")

# vendas diárias
df_sales_daily = spark.sql("""
    SELECT
        date,
        SUM(revenue) AS total_revenue, -- total de receita
        COUNT(DISTINCT cart_id) AS total_orders, -- total de pedidos
        SUM(quantity) AS total_items, -- total de itens
        ROUND(SUM(revenue) / COUNT(DISTINCT cart_id), 2) AS avg_ticket, -- ticket medio
        current_timestamp() AS created_at -- data de criacao
    FROM vw_sales
    GROUP BY date
""")

# performance por produto
df_product_perf = spark.sql("""
    SELECT
        product_id,
        product_title,
        category,
        SUM(quantity) AS total_quantity, -- total vendido por produto
        SUM(revenue) AS total_revenue, -- total de receita por produto
        ROUND(AVG(price), 2) AS avg_price, -- ticket medio por produto
        MAX(rating_rate) AS rating_rate, -- nota media por produto
        MAX(rating_count) AS rating_count, -- numero total de avaliacoes por produto
        current_timestamp() AS created_at -- data de criacao
    FROM vw_sales
    GROUP BY product_id, product_title, category
    ORDER BY total_revenue DESC -- ordena por receita decrescente
""")

# performance por categoria
df_category_perf = spark.sql("""
    WITH base AS (
        SELECT
            category,
            SUM(revenue) AS total_revenue, -- total de receita por categoria
            SUM(quantity) AS total_quantity -- total vendido por categoria
        FROM vw_sales
        GROUP BY category
    ),
    total AS (
        SELECT SUM(total_revenue) AS grand_total FROM base
    )
    SELECT
        b.category,
        b.total_revenue, -- total de receita por categoria
        b.total_quantity, -- total vendido por categoria
        ROUND(b.total_revenue / t.grand_total, 4) AS revenue_share_pct, -- share de receita por categoria
        current_timestamp() AS created_at
    FROM base b
    CROSS JOIN total t
""")

# LTV por usuario
df_user_ltv = spark.sql("""
    SELECT
        user_id,
        COUNT(DISTINCT cart_id) AS total_orders, -- total de pedidos por usuario
        SUM(quantity) AS total_items, -- total de itens por usuario
        SUM(revenue) AS total_revenue, -- total de receita por usuario
        ROUND(SUM(revenue) / COUNT(DISTINCT cart_id), 2) AS avg_ticket, -- ticket medio por usuario
        CASE
            WHEN COUNT(DISTINCT cart_id) = 1 THEN 'new'
            WHEN COUNT(DISTINCT cart_id) BETWEEN 2 AND 3 THEN 'recurrent'
            ELSE 'heavy'
        END AS customer_type, -- tipo de usuario
        current_timestamp() AS created_at -- data de criacao
    FROM vw_sales
    GROUP BY user_id
""")

## Escrita em Parquet na gold layer para o datalake
def write_gold(df, path, partition_cols=None):
    #escrever no s3a em parquet
    writer = df.write.mode("overwrite").format("parquet")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)
    return print(f"DataFrame escrito com sucesso em {path}")

## Apontamento para o banco de dados local
def get_sqlite_db_path():
    """Retorna o caminho do banco SQLite (local ou container)"""
    if os.path.exists('/.dockerenv'):
        # Dentro do container - workspace está montado em /home/hadoop/workspace
        # Criar o banco no diretório gold/ dentro do workspace
        workspace_dir = "/home/hadoop/workspace"
        gold_dir = os.path.join(workspace_dir, "gold")
        os.makedirs(gold_dir, exist_ok=True)
        db_path = os.path.join(gold_dir, "gold.db")
        return db_path
    else:
        # Localmente - cria no mesmo diretório do script
        current_dir = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(current_dir, "gold.db")
        return db_path

## Escrita em SQLite para o banco de dados local
def write_sqlite(df, db_path, table_name):

    try:
        # Garante que o diretório existe
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        
        # Converte DataFrame do Spark para Pandas
        # Se o DataFrame for muito grande, pode usar chunks, mas para Gold layer geralmente é pequeno
        pandas_df = df.toPandas()
        
        # Conecta ao SQLite e escreve
        conn = sqlite3.connect(db_path)
        
        try:
            # Escreve o DataFrame no SQLite (mode='replace' equivale ao overwrite)
            pandas_df.to_sql(
                name=table_name,
                con=conn,
                if_exists='replace',  # Substitui a tabela se existir
                index=False,  # Não grava o índice do pandas
                method='multi'  # Mais rápido para inserções múltiplas
            )
            conn.commit()
            return True
        finally:
            conn.close()
            
    except Exception as e:
        print(f"Erro ao escrever tabela '{table_name}' no SQLite: {e}")
        import traceback
        traceback.print_exc()
        return False



def main():
    write_gold(df_top10_products_revenue, "s3a://datalake/gold/produtos/top10_products_revenue/")
    write_gold(df_sales_daily, "s3a://datalake/gold/vendas/sales_daily/")
    write_gold(df_product_perf, "s3a://datalake/gold/produtos/product_performance/")
    write_gold(df_category_perf, "s3a://datalake/gold/produtos/category_performance/")
    write_gold(df_user_ltv, "s3a://datalake/gold/produtos/user_ltv/")
    
    db_path = get_sqlite_db_path()
    
    write_sqlite(df_top10_products_revenue, db_path, "top10_products_revenue")
    write_sqlite(df_sales_daily, db_path, "sales_daily")
    write_sqlite(df_product_perf, db_path, "product_performance")
    write_sqlite(df_category_perf, db_path, "category_performance")
    write_sqlite(df_user_ltv, db_path, "user_ltv")
    
if __name__ == "__main__":
    main()