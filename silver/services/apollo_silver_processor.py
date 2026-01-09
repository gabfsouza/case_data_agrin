import ast
import os
import sys
import glob
from collections import defaultdict
from os import getenv

# Adiciona o diretório raiz do projeto ao PYTHONPATH para permitir imports absolutos
# O workspace está montado em /home/hadoop/workspace no container
workspace_root = "/home/hadoop/workspace"
if os.path.exists(workspace_root):
    if workspace_root not in sys.path:
        sys.path.insert(0, workspace_root)
    # Para imports relativos dentro de silver
    silver_path = os.path.join(workspace_root, "silver")
    if silver_path not in sys.path:
        sys.path.insert(0, silver_path)
else:
    # Se não estiver no container, tenta usar caminho relativo
    current_dir = os.path.dirname(os.path.abspath(__file__))
    silver_dir = os.path.dirname(current_dir)  # Volta para silver/
    workspace = os.path.dirname(silver_dir)  # Volta para workspace root
    if workspace not in sys.path:
        sys.path.insert(0, workspace)
    if silver_dir not in sys.path:
        sys.path.insert(0, silver_dir)

from adapters.database_processor import (DatabaseProcessor,
                                         parse_spark_arguments,
                                         spark_init)

ENV_DEV = "dev"
PROCESS_MODE_ETL = "ETL"
PROCESS_MODE_EL = "EL"

COLUMN_TRANSFORMATIONS = {

    "carts": {
        "colunas": [
            "products",
        ],
        "transformations": {
            # products já vem como array de structs do Spark
            # Será explodido usando explode_outer no process_table
            # Mantém como está para passar pela validação
            "products": """col("products")""",
        },
    },
    "products": {
        "colunas": [
            "rating",
        ],
        "transformations": {
            # rating já vem como struct do Spark
            # Será desaninhado pelo unnest_df no process_table
            # Mantém como está para passar pela validação
            "rating": """col("rating")""",
        },
    },
}

#OVERWRITE_TABLES = defaultdict(str) 
#OVERWRITE_TABLES["gsheets_monitoramento_de_conta"] = "overwrite"

def load_tables_from_yml_catalog(yml_catalog_path: str = "yml_catalog_job") -> list[str]:
    """Carrega lista de tabelas baseado nos arquivos YAML disponíveis no catálogo.
    
    Args:
        yml_catalog_path (str): Caminho do diretório contendo os arquivos YAML
        
    Returns:
        list[str]: Lista de nomes das tabelas (sem extensão .yml)
    """
    tables = []
    try:
        # Ajusta o caminho baseado no workspace do container
        workspace_root = "/home/hadoop/workspace"
        if os.path.exists(workspace_root):
            full_path = os.path.join(workspace_root, yml_catalog_path)
        else:
            # Se não estiver no container, usa caminho relativo
            full_path = yml_catalog_path
        
        # Procura por arquivos .yml ou .yaml no diretório
        yml_pattern = os.path.join(full_path, "*.yml")
        yaml_pattern = os.path.join(full_path, "*.yaml")
        
        # Encontra todos os arquivos YAML
        yml_files = glob.glob(yml_pattern) + glob.glob(yaml_pattern)
        
        # Extrai o nome da tabela do nome do arquivo (remove extensão e caminho)
        for yml_file in yml_files:
            table_name = os.path.splitext(os.path.basename(yml_file))[0]
            tables.append(table_name)
        
        print(f"Found {len(tables)} tables in YAML catalog: {tables}")
        return tables
    except Exception as e:
        print(f"Error loading tables from YAML catalog: {e}")
        import traceback
        traceback.print_exc()
        return []


def initialize_database(args, spark):
    return DatabaseProcessor(
        spark=spark,
        origin_path=args.get("source_path", ""),
        target_path=args.get("target_path", ""),
        domain=args.get("domain", ""),
        subdomain=args.get("subdomain", ""),
        current_execution_date=args.get("current_execution_date", ""),
        #table_write_mode=OVERWRITE_TABLES,
    ) 
 

def process_etl( 
    database,
    write_format,
    parquet_path,
    custom_function = None,
    **custom_function_args,
):
    """
    Executes the ETL (Extract, Transform, Load) process for the given database.

    Args:
        database (DatabaseProcessor): The database processor instance.
        write_format (str): The format in which the data should be written (e.g., "iceberg").
        parquet_path (str): Path to the Parquet files (if applicable).

    Returns:
        None
    """
    print("Running in mode ETL")
    database.read_database()
    database.process_database(True, custom_function, **custom_function_args)
    database.write_database(True, write_format, parquet_path)
    database.finish_process()


def process_el(database, write_format, parquet_path):
    print("Running in mode EL")
    database.read_database()
    database.write_database(True, write_format, parquet_path)
    database.finish_process()


def process_database(databaseProcessor: DatabaseProcessor, table_name: str):
    try:
        tableProcessor = databaseProcessor.table_data[table_name]
        df = tableProcessor.process_table()
        
        # Aplica transformações de colunas se definidas
        if table_name in COLUMN_TRANSFORMATIONS:
            df = tableProcessor.set_multiple_schemas(
                COLUMN_TRANSFORMATIONS[table_name]["transformations"],
            )
        
        # Para carts: exploda array de products (cria uma linha por produto)
        if table_name == "carts" and "products" in df.columns:
            print(f"Explodindo array 'products' na tabela {table_name}")
            # Separa colunas normais das arrays
            normal_columns = [c for c in df.columns if c != "products"]
            # Explode o array mantendo as outras colunas
            from pyspark.sql.functions import explode_outer, col
            df = df.select(*[col(c) for c in normal_columns], explode_outer("products").alias("products"))
            # Desaninha o struct products em colunas separadas (products_productId, products_quantity)
            df = tableProcessor.set_dataframe(df)
            df = tableProcessor.unnest_df()
        
        # Para products: desaninha struct rating em colunas separadas (rating_count, rating_rate)
        if table_name == "products" and "rating" in df.columns:
            print(f"Desaninhando struct 'rating' na tabela {table_name}")
            df = tableProcessor.set_dataframe(df)
            df = tableProcessor.unnest_df()
        
        # Preenche arrays vazios (se necessário)
        df = tableProcessor.set_dataframe(df)
        df = tableProcessor.fill_array_column()
        
        return df
    except Exception as e:
        print(f"Error processing table {table_name}")
        print(e)
        raise e


def main():
    domain = "vendas"
    subdomain = "produtos"
    env = getenv("ENV", "")
    
    # Carrega tabelas automaticamente dos YAMLs disponíveis
    available_tables = ["products", "carts"]
    #load_tables_from_yml_catalog("yml_catalog_job")
    
    if env == ENV_DEV:
        optional_fields = {
            "source_path": "s3a://datalake/bronze/",
            "target_path": "s3a://datalake/silver/",
            "process_mode": PROCESS_MODE_ETL,
            "write_format": "parquet",
            "parquet_path": "s3a://datalake/silver/",
            "domain": domain,
            "subdomain": subdomain,
            "current_execution_date": "2025-01-15",
            "last_execution_date": "2025-01-01",
            "table_list": available_tables,  # Usa tabelas carregadas dos YAMLs
            "JOB_NAME": "silver_processor",
        }
    else:
        optional_fields = {
            "source_path": "s3a://datalake/bronze/",
            "target_path": "s3a://datalake/silver/",
            "process_mode": "",
            "write_format": "parquet",
            "parquet_path": "s3a://datalake/silver/",
            "domain": domain,
            "subdomain": subdomain,
            "current_execution_date": "",
            "last_execution_date": "",
            "table_list": available_tables,  # Usa tabelas carregadas dos YAMLs
            "JOB_NAME": "silver_processor",
        }

    args = parse_spark_arguments([], optional_fields)
    spark, args = spark_init(args)

    process_mode: str = (
        PROCESS_MODE_ETL if args.get("process_mode") == "" else args.get("process_mode", PROCESS_MODE_ETL)
    )
    write_format: str = args.get("write_format", "parquet")
    parquet_path: str = args.get("parquet_path", "s3a://datalake/silver/")
    execution_date: str = args.get("current_execution_date", "")
    table_list = args.get("table_list", [])
    
    # Garante que table_list seja uma lista de strings
    if isinstance(table_list, str):
        if table_list.startswith("["):
            table_list = ast.literal_eval(table_list)
        else:
            # Se for string simples, converte para lista
            table_list = [table_list]
    elif not isinstance(table_list, list):
        # Se não for lista nem string, tenta converter
        table_list = [str(table_list)] if table_list else []
    
    # Garante que todos os elementos são strings
    table_list = [str(t) if not isinstance(t, str) else t for t in table_list if t]
    args["environment"] = env

    database = initialize_database(args, spark)
    print(f"Processing tables {table_list} for date {execution_date}")
    
    # Define as tabelas a serem processadas (multithreading automático se múltiplas tabelas)
    if table_list:
        database.set_tables(table_list)
    else:
        # Se não houver tabelas definidas, usa as disponíveis nos YAMLs
        if available_tables:
            database.set_tables(available_tables)
        else:
            raise Exception("No tables found in YAML catalog and no table_list provided")

    if process_mode == PROCESS_MODE_ETL:
        process_etl(
            database,
            write_format,
            parquet_path,
            process_database,
            databaseProcessor=database,
        )
    elif process_mode == PROCESS_MODE_EL:
        process_el(database, write_format, parquet_path)
    else:
        raise Exception("Invalid process mode")


if __name__ == "__main__":
    main()
