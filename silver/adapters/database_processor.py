#
# Este módulo fornece funcionalidade para processar bancos de dados usando MinIO local.
# Inclui classes e funções para ler, processar e escrever dados com suporte a multi-threading.
# Suporta formatos Parquet e SQLite para execução local.
#

import datetime
import os
import sys

from pyspark.conf import SparkConf
from pyspark.sql import DataFrame, SparkSession

# Ajusta o path para permitir imports relativos dentro do módulo silver
# Quando executado no container, o workspace está em /home/hadoop/workspace
workspace_root = "/home/hadoop/workspace"
if os.path.exists(workspace_root):
    if workspace_root not in sys.path:
        sys.path.insert(0, workspace_root)
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

from adapters.table_processor import TableProcessor

# Configuração MinIO
MINIO_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin123")


class DatabaseProcessor:
    def __init__(
        self,
        spark,
        origin_path: str = "",
        target_path: str = "",
        domain: str = "",
        subdomain: str = "",
        current_execution_date: str = "",
        table_write_mode: dict[str, str] = {},
    ):
        self.spark = spark
        self.origin_path = origin_path
        self.target_path = target_path
        self.tables: list[str] = []
        self.table_data: dict[str, TableProcessor] = {}
        self.database_data: dict[str, DataFrame] = {}
        self.domain = domain
        self.subdomain = subdomain
        self.current_execution_date = current_execution_date
        self.table_write_mode = table_write_mode

    def set_tables(self, tables: list[str]):
        # Garante que tables é uma lista de strings
        if not isinstance(tables, list):
            tables = [str(tables)] if tables else []
        # Converte todos os elementos para string
        self.tables = [str(t) for t in tables if t]

    def get_year_month_day(self):
        if self.current_execution_date:
            return self.current_execution_date.split("-")
        return datetime.datetime.now().strftime("%Y-%m-%d").split("-")

    def read_database(self, multi_threading: bool = True):
        """Lê tabelas do MinIO usando multi-threading."""
        from concurrent.futures import ThreadPoolExecutor

        threads = None if multi_threading else 1
        table_process = {}

        with ThreadPoolExecutor(max_workers=threads) as executor:
            for table_name in self.tables:
                # Garante que table_name é uma string, não uma lista ou outro tipo
                if isinstance(table_name, list):
                    if len(table_name) > 0:
                        table_name = table_name[0]
                    else:
                        print(f"Warning: table_name é uma lista vazia, pulando...")
                        continue
                table_name = str(table_name)
                
                print(f"Reading table {table_name}")
                target_table_name = (
                    f"{self.subdomain}_{table_name}" if self.subdomain else table_name
                )
                year, month, day = self.get_year_month_day()

                # Cria TableProcessor com o caminho do arquivo YAML que contém a definição da PK
                # O arquivo YAML deve estar em: yml_catalog_job/{table_name}.yml
                # Formato esperado do YAML: columns_description com primary_key: true
                
                # Define source_path - baseado na estrutura de salvamento do bronze
                # Os arquivos são salvos como JSON: bronze/{table_name}/year={year}/month={month}/day={day}/{table_name}.json
                # Para JSON: s3a://datalake/bronze/{table_name}/year={year}/month={month}/day={day}/{table_name}.json
                if '/year=' in self.origin_path:
                    # Caminho já tem partições - adiciona nome do arquivo JSON se necessário
                    if self.origin_path.endswith('/'):
                        source_path = f"{self.origin_path}{table_name}.json"
                    elif not (self.origin_path.endswith('.json') or self.origin_path.endswith('.parquet')):
                        source_path = f"{self.origin_path}/{table_name}.json"
                    else:
                        source_path = self.origin_path
                elif self.origin_path.endswith('/'):
                    # Diretório base - constrói caminho para JSON particionado
                    source_path = f"{self.origin_path}{table_name}/year={year}/month={month}/day={day}/{table_name}.json"
                elif self.origin_path.endswith('.json') or self.origin_path.endswith('.parquet'):
                    # Caminho já especifica arquivo específico
                    source_path = self.origin_path
                else:
                    # Estrutura simples - tenta JSON primeiro (formato atual do bronze)
                    source_path = f"{self.origin_path}{table_name}.json"
                
                table_processor = TableProcessor(
                    self.spark,
                    table=table_name,
                    pk_file_path=f"yml_catalog_job/{table_name}.yml",
                    domain=self.domain,
                    subdomain=self.subdomain,
                    source_path=source_path,
                    target_path=self.target_path + target_table_name,
                    write_mode=self.table_write_mode.get(table_name, "append"),
                )

                self.table_data[table_name] = table_processor
                
                # Detecta se deve ler JSON ou Parquet baseado no source_path
                # Os arquivos no bronze estão salvos como JSON: bronze/{table_name}/year={year}/month={month}/day={day}/{table_name}.json
                # Atualiza o source_path do table_processor se foi modificado
                table_processor.source_path = source_path
                
                # Detecta o formato baseado na extensão do arquivo
                if source_path.endswith('.json'):
                    # Lê como JSON (formato atual do bronze)
                    table_process[table_name] = executor.submit(
                        table_processor.read_json_table
                    )
                elif source_path.endswith('.parquet'):
                    # Lê como Parquet (se houver arquivos Parquet)
                    table_process[table_name] = executor.submit(
                        table_processor.read_parquet_table
                    )
                elif source_path.endswith('/'):
                    # Se terminar com /, assume que é diretório particionado e tenta JSON primeiro
                    # Adiciona nome do arquivo JSON
                    source_path = f"{source_path}{table_name}.json"
                    table_processor.source_path = source_path
                    table_process[table_name] = executor.submit(
                        table_processor.read_json_table
                    )
                else:
                    # Por padrão, tenta JSON primeiro (formato atual do bronze)
                    if not source_path.endswith('.json'):
                        source_path = f"{source_path}/{table_name}.json" if not source_path.endswith('/') else f"{source_path}{table_name}.json"
                        table_processor.source_path = source_path
                    table_process[table_name] = executor.submit(
                        table_processor.read_json_table
                    )

        self.database_data = table_process
        return self.database_data

    def process_database(
        self,
        multi_threading: bool = True,
        custom_function: callable = None,
        **custom_function_args: dict,
    ):
        """Processa tabelas com formatação de dados e deduplicação usando PK, com multi-threading."""
        from concurrent.futures import ThreadPoolExecutor

        threads = None if multi_threading else 1
        table_process = {}

        with ThreadPoolExecutor(max_workers=threads) as executor:
            for table_name, table_data in self.database_data.items():
                # Aguarda a leitura completar
                df = table_data.result()
                self.table_data[table_name].set_dataframe(df)

                # Processa a tabela (formatação + deduplicação com PK)
                func = custom_function if custom_function else self.table_data[table_name].process_table
                print(f"Processing table {table_name} (formatting and deduplication)")
                table_process[table_name] = executor.submit(
                    func, **custom_function_args, table_name=table_name
                )

        self.database_data = table_process
        return self.database_data

    def write_database(
        self,
        multi_threading: bool = True,
        write_format: str = "parquet",
        parquet_path: str = "",
    ):
        """Escreve tabelas processadas usando multi-threading."""
        from concurrent.futures import ThreadPoolExecutor

        threads = None if multi_threading else 1
        table_process = {}

        with ThreadPoolExecutor(max_workers=threads) as executor:
            for table_name, table_data in self.database_data.items():
                # Aguarda o processamento completar
                df = table_data.result()
                self.table_data[table_name].set_dataframe(df)

                if write_format == "parquet":
                    # Usa parquet_path se fornecido, senão usa target_path
                    base_path = parquet_path if parquet_path else self.target_path
                    year, month, day = self.get_year_month_day()
                    target_table_name = (
                        f"{self.subdomain}_{table_name}" if self.subdomain else table_name
                    )
                    write_path = f"{base_path}{target_table_name}/year={year}/month={month}/day={day}/"
                    print(f"Writing table {table_name} to Parquet path {write_path}")
                    table_process[table_name] = executor.submit(
                        self.table_data[table_name].write_parquet_table, write_path, "overwrite"
                    )
                elif write_format == "iceberg":
                    # Iceberg ainda disponível se necessário
                    table_process[table_name] = executor.submit(
                        self.table_data[table_name].write_iceberg_table
                    )
                else:
                    # Padrão: Parquet
                    base_path = parquet_path if parquet_path else self.target_path
                    year, month, day = self.get_year_month_day()
                    target_table_name = (
                        f"{self.subdomain}_{table_name}" if self.subdomain else table_name
                    )
                    write_path = f"{base_path}{target_table_name}/year={year}/month={month}/day={day}/"
                    print(f"Writing table {table_name} to Parquet path {write_path} (default format)")
                    table_process[table_name] = executor.submit(
                        self.table_data[table_name].write_parquet_table, write_path, "overwrite"
                    )

        self.database_data = table_process
        return self.database_data

    def finish_process(self):
        """Aguarda todas as escritas completarem."""
        for table_name, table_data in self.database_data.items():
            table_data.result()
            print(f"Table {table_name} finished processing")


def parse_spark_arguments(sys_args: list = [], optional_fields: dict = {}) -> dict:
    """Parse argumentos do Spark (simplificado - retorna optional_fields mesclado).
    
    Args:
        sys_args (list): Argumentos da linha de comando (não usado atualmente)
        optional_fields (dict): Dicionário com campos opcionais
        
    Returns:
        dict: Dicionário com todos os argumentos
    """
    # Por enquanto, apenas retorna os optional_fields
    # Pode ser estendido para parse de sys.argv no futuro
    return optional_fields.copy()


def spark_init(args: dict = None):
    """Inicializa SparkSession configurado para MinIO."""
    if args is None:
        args = {}

    conf = SparkConf()

    # Configurações MinIO/S3
    conf.set("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    conf.set("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    conf.set("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

    # Configurações de performance
    conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
    conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
    conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
    conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")

    # Configuração opcional do SparkCatalog para Iceberg (se necessário)
    # Por padrão, usamos Parquet local, mas Iceberg está disponível como opção
    # conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
    # conf.set("spark.sql.catalog.spark_catalog.type", "hadoop")
    # conf.set("spark.sql.catalog.spark_catalog.warehouse", "s3a://datalake/warehouse")

    job_name = args.get("JOB_NAME", "transform")
    spark = SparkSession.builder.appName(job_name).config(conf=conf).getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    return spark, args
