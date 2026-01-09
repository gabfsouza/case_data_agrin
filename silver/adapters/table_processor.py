# Este módulo define a classe TableProcessor, que encapsula operações de leitura,
# transformação, deduplicação e escrita de dados localmente usando PySpark e MinIO.
# Principais funcionalidades:
# - Leitura de tabelas do MinIO (Parquet/JSON) local
# - Deduplicação baseada em chaves primárias e colunas de ordenação
# - Transformações para normalização de arrays, structs e flatten de dados aninhados
# - Limpeza de colunas técnicas (Airbyte, CDC)
# - Escrita de dados em formatos Parquet (local) e SQLite
# - Suporte a manipulação de metadados via YAML, detecção de colunas PII e ajuste de schemas


import datetime
import json
import os
import sys

import boto3
import yaml

# Configuração MinIO
MINIO_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin123")
import pyspark.sql.functions as F
from pyspark.conf import SparkConf
from pyspark.sql import DataFrame
from pyspark.sql.functions import (col, explode_outer, from_json,
                                   from_unixtime, row_number, unix_timestamp)
from pyspark.sql.types import BooleanType  # noqa: E401
from pyspark.sql.types import DoubleType  # noqa: E401
from pyspark.sql.types import IntegerType  # noqa: E401
from pyspark.sql.types import LongType  # noqa: E401
from pyspark.sql.types import MapType  # noqa: E401
from pyspark.sql.types import StringType  # noqa: E401
from pyspark.sql.types import StructField  # noqa: E401
from pyspark.sql.types import ArrayType, DateType, StructType, TimestampType
from pyspark.sql.window import Window


class TableProcessor:
    def __init__(
        self,
        spark,
        database: str = "",
        table: str = "",
        pk_file_path: str = "",
        primary_keys: list[str] = [],
        target_database: str = "",
        dataframe: DataFrame = None,
        domain: str = "",
        subdomain: str = "",
        source_path: str = "",
        target_path: str = "",
        execution_date: str = "",
        orderby_column: str = "",
        iceberg_target_path: str = "",
        write_mode: str = "append"
    ):
        self.spark = spark
        self.origin_database = database
        self.target_database = target_database
        self.table = table
        self.df = dataframe
        self.source_path = source_path
        self.contains_pii = False
        # Extrai bucket e caminho do arquivo PK (MinIO ou local)
        # Ex: s3a://datalake/metadata/domain/subdomain/table.json
        #     -> bucket: datalake, file: metadata/domain/subdomain/table.json
        # Ex: yml_catalog_job/table.yml
        #     -> bucket: "", file: yml_catalog_job/table.yml
        if pk_file_path.startswith(("s3a://", "s3://")):
            pk_path_clean = pk_file_path.replace("s3a://", "").replace("s3://", "")
            path_parts = pk_path_clean.split("/")
            self.pk_bucket = path_parts[0] if path_parts else ""
            self.pk_file = "/".join(path_parts[1:]) if len(path_parts) > 1 else ""
        else:
            # Arquivo local
            self.pk_bucket = ""
            self.pk_file = pk_file_path
        self.domain = domain
        self.subdomain = subdomain
        self.primary_keys = primary_keys
        self.target_path = target_path
        self.execution_date = execution_date
        self.orderby_column = orderby_column
        self.iceberg_target_path = iceberg_target_path
        self.table_name = (
            f"{self.subdomain}_{self.table}" if self.subdomain else self.table
        )
        log4j = self.spark._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger("ETLLogger")
        self.write_mode = write_mode
        # Cache para o método isEmpty
        self._isEmpty_cache = None
        self._isEmpty_cache_dataframe_id = None
        print(f"Writing in {self.write_mode}")


    def read_parquet_table(self, push_down_predicate: str = "") -> DataFrame:
        """Reads a Parquet table from S3 using PySpark.

        Args:
            push_down_predicate (str): Optional partition filter.

        Returns:
            DataFrame: Loaded Spark DataFrame."""
        try:
            print("Reading Parquet table from S3 using PySpark")

            # Verifica se source_path está definido
            if not self.source_path:
                raise ValueError(
                    "source_path é obrigatório para leitura de Parquet. "
                    "Configure o caminho S3 da tabela."
                )

            print(f"Lendo tabela Parquet do S3: {self.source_path}")

            # Lista de caminhos para tentar (ordem: arquivo específico primeiro, depois diretório)
            paths_to_try = []
            
            # Constrói lista de caminhos possíveis baseado na estrutura de salvamento
            # Arquivo salvo como: bronze/{table_name}/year={year}/month={month}/day={day}/{table_name}.parquet
            if self.source_path.endswith('/'):
                # Se é diretório, tenta o arquivo específico dentro dele primeiro
                if self.table:
                    file_path = f"{self.source_path}{self.table}.parquet"
                    paths_to_try.append(file_path)
                # Depois tenta o diretório inteiro (para leitura de múltiplos arquivos)
                paths_to_try.append(self.source_path)
            elif self.source_path.endswith('.parquet'):
                # Arquivo específico - tenta direto
                paths_to_try.append(self.source_path)
            else:
                # Caminho genérico - constrói caminho do arquivo
                if self.table:
                    file_path = f"{self.source_path}/{self.table}.parquet" if not self.source_path.endswith('/') else f"{self.source_path}{self.table}.parquet"
                    paths_to_try.append(file_path)
                paths_to_try.append(self.source_path)
                if not self.source_path.endswith('/'):
                    paths_to_try.append(f"{self.source_path}/")

            # Tenta ler de cada caminho possível
            df_read = None
            successful_path = None
            for path in paths_to_try:
                try:
                    print(f"Tentando ler: {path}")
                    
                    # Tenta ler o Parquet com tratamento de erro melhorado
                    df_read = self.spark.read.option("mergeSchema", "true").parquet(path)
                    
                    # Verifica se conseguiu ler com sucesso
                    # Se o DataFrame foi criado e tem schema, considera sucesso
                    if df_read is not None:
                        # Tenta verificar o schema (pode dar erro se vazio, mas tentamos)
                        try:
                            schema_count = len(df_read.schema)
                            print(f"Sucesso ao ler: {path} (schema com {schema_count} campos)")
                            successful_path = path
                            break
                        except:
                            # Se deu erro ao verificar schema, pode ser vazio, mas tenta usar
                            print(f"Lido com sucesso (possivelmente vazio): {path}")
                            successful_path = path
                            break
                except Exception as e:
                    error_msg = str(e)
                    error_type = type(e).__name__
                    # Log do erro mas continua tentando outros caminhos
                    if "CANNOT_INFER_EMPTY_SCHEMA" in error_msg:
                        print(f"Dataset vazio em {path}: {error_msg}")
                        # Continua tentando outros caminhos
                        continue
                    elif "length is too low: 0" in error_msg or "is not a Parquet file" in error_msg:
                        print(f"⚠️  Arquivo Parquet vazio ou corrompido em {path}")
                        print(f"   Detalhes: Arquivo existe mas tem tamanho 0 ou está corrompido")
                        # Arquivo existe mas está vazio/corrompido, continua tentando outros caminhos
                        continue
                    elif "does not exist" in error_msg.lower() or "Path does not exist" in error_msg:
                        print(f"⚠️  Arquivo não encontrado: {path}")
                        continue
                    elif "CANNOT_READ_FILE_FOOTER" in error_msg:
                        print(f"⚠️  Não foi possível ler o footer do arquivo Parquet em {path}")
                        print(f"   O arquivo pode estar vazio, corrompido ou não ser um Parquet válido")
                        continue
                    else:
                        print(f"Erro ao ler {path} ({error_type}): {error_msg[:200]}")  # Limita tamanho do erro
                        continue
            
            # Se conseguiu ler, usa o DataFrame
            if df_read is not None and successful_path:
                self.df = df_read
            else:
                # Se não conseguiu ler nenhum caminho, cria DataFrame vazio
                print(f"Não foi possível ler nenhum arquivo Parquet. Criando DataFrame vazio para tabela {self.table}")
                from pyspark.sql.types import StructType
                empty_schema = StructType([])
                self.df = self.spark.createDataFrame([], schema=empty_schema)

            # Aplica push_down_predicate se fornecido
            if push_down_predicate and self.df is not None:
                try:
                    self.df = self.df.filter(push_down_predicate)
                except:
                    pass  # Ignora erro se não conseguir aplicar filtro

            return self.df
        except Exception as e:
            print(
                f"Error reading table {self.table} from parquet with path {self.source_path}"
            )
            print(f"Error type: {type(e).__name__}")
            print(f"Error details: {e}")
            import traceback
            traceback.print_exc()
            # Retorna DataFrame vazio em caso de erro, não a exceção
            self.df = self.spark.createDataFrame([], schema=None)
            return self.df

    def read_json_table(self, push_down_predicate: str = "") -> DataFrame:
        """Reads a JSON table from S3.

        Returns:
            DataFrame: Loaded Spark DataFrame."""
        try:
            print(f"Lendo tabela JSON do S3: {self.source_path}")
            self.df: DataFrame = self.spark.read.json(self.source_path)
            
            # Aplica push_down_predicate se fornecido
            if push_down_predicate:
                self.df = self.df.filter(push_down_predicate)
                
            return self.df
        except Exception as e:
            print(
                f"Error reading table {self.table} from json with path {self.source_path}"
            )
            print(f"Error type: {type(e).__name__}")
            print(f"Error details: {e}")
            import traceback
            traceback.print_exc()
            # Retorna DataFrame vazio em caso de erro, não a exceção
            self.df = self.spark.createDataFrame([], schema=None)
            return self.df

    def isEmpty(self):
        """Checks if the current DataFrame is empty.
        
        Uses caching to avoid unnecessary PySpark job executions.
        The cache is invalidated when the DataFrame changes.

        Returns:
            bool: True if empty, False otherwise."""
        try:
            # Verifica se o DataFrame mudou desde a última verificação
            current_df_id = id(self.df) if self.df is not None else None
            
            # Se o DataFrame não mudou e temos cache, retorna o valor em cache
            if (self._isEmpty_cache is not None and 
                self._isEmpty_cache_dataframe_id == current_df_id):
                return self._isEmpty_cache
            
            # Se não há cache ou o DataFrame mudou, executa a verificação
            if self.df is None:
                self._isEmpty_cache = True
                self._isEmpty_cache_dataframe_id = current_df_id
                return self._isEmpty_cache
            
            # Verifica se self.df é uma exceção/erro em vez de um DataFrame
            if not isinstance(self.df, DataFrame):
                # Se não é um DataFrame (provavelmente é uma exceção), retorna True
                print(f"Warning: self.df is not a DataFrame for table {self.table}, treating as empty")
                self._isEmpty_cache = True
                self._isEmpty_cache_dataframe_id = current_df_id
                return True
            
            is_empty = self.df.isEmpty()
            
            # Atualiza o cache
            self._isEmpty_cache = is_empty
            self._isEmpty_cache_dataframe_id = current_df_id
            
            return is_empty
        except Exception as e:
            print(f"Error checking if table {self.table} is empty")
            print(f"Error type: {type(e).__name__}")
            print(f"Error details: {e}")
            import traceback
            traceback.print_exc()
            # Em caso de erro, limpa o cache e retorna True (assumindo que está vazio)
            self._isEmpty_cache = None
            self._isEmpty_cache_dataframe_id = None
            return True

    def _build_write_partitions(self) -> list[str]:
        if self.execution_date:
            year, month, day = self.execution_date.split("-")
            return [year, month, day]
        else:
            current_day = datetime.now().strftime("%Y-%m-%d").split("-")
            return current_day

    def _retrieve_table_metadata(self, bucket: str, key: str) -> dict:
        """Recupera metadados da tabela (PK) do MinIO ou sistema de arquivos local (suporta JSON e YAML).
        
        Args:
            bucket (str): Nome do bucket no MinIO (ex: 'datalake') ou vazio se for arquivo local
            key (str): Caminho do arquivo JSON/YAML (ex: 'metadata/domain/subdomain/table.json' ou 'yml_catalog_job/table.yml')
            
        Returns:
            dict: Dados do JSON/YAML com informações da PK e colunas
        """
        try:
            # Se bucket estiver vazio, assume que é arquivo local
            if not bucket:
                # Lista de possíveis caminhos para tentar
                possible_paths = [
                    key,  # Caminho exato fornecido
                    os.path.join(os.getcwd(), key),  # Caminho relativo ao diretório atual
                    os.path.join("/home/hadoop/workspace", key),  # Workspace do container
                ]
                
                file_content = None
                for path in possible_paths:
                    if os.path.exists(path):
                        with open(path, 'r', encoding='utf-8') as f:
                            file_content = f.read()
                        break
                
                if file_content is None:
                    raise FileNotFoundError(f"Metadata file not found in any of these paths: {possible_paths}")
            else:
                # Remove 's3a://' do bucket se presente
                bucket_clean = bucket.replace("s3a://", "").replace("s3://", "")
                
                # Configura cliente boto3 para MinIO
                s3_client = boto3.client(
                    "s3",
                    endpoint_url=MINIO_ENDPOINT,
                    aws_access_key_id=MINIO_ACCESS_KEY,
                    aws_secret_access_key=MINIO_SECRET_KEY,
                )
                
                file_content = (
                    s3_client.get_object(Bucket=bucket_clean, Key=key)
                    .get("Body")
                    .read()
                    .decode("utf-8")
                )
            
            # Detecta se é YAML ou JSON baseado na extensão do arquivo
            if key.endswith(('.yml', '.yaml')):
                return yaml.safe_load(file_content)
            else:
                return json.loads(file_content)
        except Exception as e:
            print(f"Error reading metadata file {key} from bucket {bucket}")
            print(e)
            return {}

    def _process_json_data(self, json_data: dict) -> list[str]:
        """Processa dados JSON ou YAML para extrair primary keys.
        
        Args:
            json_data (dict): Dados do JSON/YAML com informações das colunas
            
        Returns:
            list[str]: Lista de primary keys
        """
        try:
            self.primary_keys = []
            if "columns_description" in json_data:
                for column in json_data["columns_description"]:
                    if column.get("primary_key", False):
                        # YAML mantém o nome original, JSON pode ter lowercase
                        col_name = column["column"]
                        self.primary_keys.append(col_name.lower())
            return self.primary_keys
        except Exception as e:
            print("Error processing primary key data")
            print(e)
            return []

    def _process_yaml_data(self, yaml_data: dict) -> list[str]:
        """Processa dados YAML para extrair primary keys (método mantido para compatibilidade).
        
        Agora usa _process_json_data internamente pois ambos têm a mesma estrutura.
        
        Args:
            yaml_data (dict): Dados do YAML com informações das colunas
            
        Returns:
            list[str]: Lista de primary keys
        """
        return self._process_json_data(yaml_data)

    def _load_pii_columns(self, json_data: dict = {}) -> list[str]:
        try:
            if not json_data:
                json_data = self._retrieve_table_metadata(self.pk_bucket, self.pk_file)
            self.pii_columns = []
            if "columns_description" in json_data:
                for column in json_data["columns_description"]:
                    if column["classification"] == "PII":
                        self.pii_columns.append(column["column"])
                        self.contains_pii = True
        except Exception as e:
            print("Error loading PII columns")
            print(e)
            return e

    def build_push_down_predicate(
        self, last_executed_date: str, current_executed_date: str
    ) -> str:
        """
        Builds the push down predicate for the query
        """
        last_year, last_month, last_day = last_executed_date.split("-")
        current_year, current_month, current_day = current_executed_date.split("-")
        if last_year != current_year:
            predicate = f"year BETWEEN {last_year} AND {current_year}"
        elif last_month != current_month:
            predicate = (
                f"year = {last_year} AND month BETWEEN {last_month} AND {current_month}"
            )
        elif last_day != current_day:
            predicate = f"year = {last_year} AND month = {last_month}"
            predicate += f" AND day BETWEEN {last_day} AND {current_day}"
        else:
            predicate = (
                f"year = {last_year} AND month = {last_month} AND day = {last_day}"
            )
        return predicate

    def fill_array_column(self, only: list[str]  = []):
        """Preenche colunas nulas de Array/Struct com valores vazios.

        Evita recriar o DataFrame quando não houver colunas Array/Struct.
        Se `only` for fornecido, aplica apenas nessas colunas (se existirem).

        Args:
            only (list[str] | None): Lista opcional de colunas a tratar.

        Returns:
            DataFrame: DataFrame atualizado (ou o original, se nada mudar).
        """
        df = self.df
        try:
            # 1) Descobrir colunas alvo (Array/Struct), respeitando `only`
            targets: set[str] = set()
            for field in df.schema.fields:
                name, dt = field.name, field.dataType
                if only and name not in only:
                    continue
                if isinstance(dt, (ArrayType, StructType)):
                    targets.add(name)

            # 2) Se não há o que tratar, retorna imediatamente sem novo select
            if not targets:
                return df

            # 3) Monta as expressões só quando necessário
            exprs = []
            for field in df.schema.fields:
                name, dt = field.name, field.dataType

                # Colunas não-alvo: mantém como está
                if name not in targets:
                    exprs.append(F.col(name))
                    continue

                # Array: substitui nulo por array vazio tipado
                if isinstance(dt, ArrayType):
                    new_col = F.coalesce(F.col(name), F.array().cast(dt)).alias(name)

                # Struct: substitui nulo por struct vazio tipado
                elif isinstance(dt, StructType):
                    empty_struct = F.struct(
                        *[F.lit(None).cast(f.dataType).alias(f.name) for f in dt.fields]
                    ).cast(dt)
                    new_col = F.coalesce(F.col(name), empty_struct).alias(name)

                else:
                    new_col = F.col(name)

                exprs.append(new_col)

            # 4) Só aqui recria o DF (porque havia colunas-alvo)
            self.df = df.select(*exprs)
            return self.df

        except Exception as e:
            print(f"Error filling array/struct columns for table {self.table}")
            print(e)
            raise e

    def set_dataframe(self, dataframe: DataFrame):
        """Sets the current DataFrame.

        Args:
            dataframe (DataFrame): The DataFrame to set."""
        self.df = dataframe
        # Invalida o cache quando o DataFrame é alterado
        self._isEmpty_cache = None
        self._isEmpty_cache_dataframe_id = None

    def get_dataframe(self):
        """Gets the current DataFrame.

        Returns:
            DataFrame: Current DataFrame."""
        return self.df

    def set_multiple_schemas(self, schema: dict[str, str]) -> DataFrame:
        try:
            for column, expression in schema.items():
                if column in self.df.columns:
                    self.df = self.df.withColumn(column, eval(expression))
            return self.df
        except Exception as e:
            print(f"Error setting multiple schemas for table {self.table}")
            print(e)
            raise e

    def set_schema(self, target_column: str, target_expression: str = ""):
        if target_expression != "":
            self.df = self.df.withColumn(target_column, eval(target_expression))
        return self.df

    def check_primary_keys(self) -> bool:
        """Checks if primary key columns exist in the DataFrame.

        Returns:
            bool: True if all primary keys are present."""
        for pk in self.primary_keys:
            if pk not in self.df.columns:
                return False
        return True

    def unbox_column(
        self, column_name: str, default_schema: StructType = None
    ) -> DataFrame:
        """
        Unboxes a JSON column (stored as text) into a structured column.

        Args:
            column_name (str): The name of the column to unbox.
            default_schema (StructType): Optional default schema to use.

        Returns:
            DataFrame: Updated DataFrame with the unboxed column.
        """
        try:
            # Infer schema if no default schema is provided
            if default_schema is None:
                inferred_schema = self.spark.read.json(
                    self.df.rdd.map(lambda row: row[column_name])
                ).schema
            else:
                inferred_schema = default_schema

            # Apply the schema to the JSON column
            self.df = self.df.withColumn(
                column_name, from_json(col(column_name), inferred_schema)
            )
            return self.df
        except Exception as e:
            print(f"Error unboxing column {column_name} for table {self.table_name}")
            print(e)
            raise

    def deduplicate_table(
        self,
        primary_key: list[str] = [],
        orderby_column: list[str] = [],
    ) -> DataFrame:
        """Removes duplicate rows based on primary key and order columns.

        Args:
            primary_key (list[str]): List of primary key columns.
            orderby_column (list[str]): List of columns used to identify the latest record.

        Returns:
            DataFrame: Deduplicated DataFrame."""
        try:
            if primary_key:
                pk: list[str] = primary_key
                self.primary_keys = primary_key
            elif self.primary_keys:
                pk: list[str] = self.primary_keys
            else:
                # Tenta carregar do arquivo de metadados (JSON ou YAML)
                metadata = self._retrieve_table_metadata(self.pk_bucket, self.pk_file)
                if metadata:
                    pk = self._process_json_data(metadata)
                    self.primary_keys = pk
                else:
                    pk = []
            
            # Verifica se as PKs existem no DataFrame, senão tenta fallback
            if pk and self.check_primary_keys():
                pass  # Usa as PKs carregadas
            else:
                # Fallback: tenta encontrar coluna 'id' ou '_id'
                if "id" in self.df.columns:
                    pk = ["id"]
                    self.primary_keys = ["id"]
                elif "_id" in self.df.columns:
                    pk = ["_id"]
                    self.primary_keys = ["_id"]
                else:
                    raise ValueError(f"No primary key found for table {self.table_name}. Define PK no arquivo YAML.")

            # Lista de colunas de ordenação padrão (Airbyte/CDC)
            default_orderby_columns = [
                "_ab_cdc_updated_at",
                "_ab_updated_at",
                "_airbyte_extracted_at",
                "_ab_cdc_cursor",
                "updated_at",
                "updatedAt",
                "__v",  # Versão do documento (pode existir em algumas APIs)
            ]

            # Se orderby_column não for fornecido, usa a lista padrão
            if not orderby_column:
                orderby_column = default_orderby_columns

            # Filtra apenas as colunas que existem no DataFrame
            valid_orderby_columns = [
                col for col in orderby_column if col in self.df.columns
            ]
            
            # Se não encontrou nenhuma coluna de ordenação padrão, tenta usar a PK ou coluna de data
            if not valid_orderby_columns:
                # Tenta usar colunas comuns de timestamp/data
                alternative_cols = ["date", "created_at", "createdAt", "timestamp"]
                valid_orderby_columns = [
                    col for col in alternative_cols if col in self.df.columns
                ]
            
            # Se ainda não encontrou, usa a própria PK para ordenação (mantém a primeira ocorrência)
            if not valid_orderby_columns:
                # Usa a primeira PK como ordenação (ordenação natural)
                if pk and pk[0] in self.df.columns:
                    valid_orderby_columns = [pk[0]]
                    print(f"Usando PK '{pk[0]}' como coluna de ordenação para deduplicação")
                else:
                    # Se não há PK também, não faz deduplicação (retorna todos os registros)
                    print(f"Aviso: Não foi possível encontrar colunas de ordenação para deduplicação na tabela {self.table_name}")
                    print(f"Colunas disponíveis: {self.df.columns[:10]}...")  # Mostra primeiras 10 colunas
                    return self.df  # Retorna sem deduplicar

            print(
                f"Using partition col {pk} and columns {valid_orderby_columns} to deduplicate table {self.table_name}"
            )

            # Cria a janela de particionamento com todas as colunas de ordenação válidas
            window = Window.partitionBy([col(x) for x in pk]).orderBy(
                *[col(x).desc() for x in valid_orderby_columns]
            )

            self.df = self.df.withColumn("rank", row_number().over(window))
            self.df = self.df.filter(col("rank") == 1).drop("rank")
            return self.df
        except Exception as e:
            print(f"Error deduplicating table {self.table}")
            print(e)
            raise e

    def _clean_column_name(self, name: str) -> str:
        """Remove partes redundantes do nome da coluna, começando do final."""
        parts = name.split("_")
        seen = set()
        cleaned_parts = []
        for part in reversed(parts):
            if part not in seen:
                seen.add(part)
                cleaned_parts.append(part)
        cleaned_parts = list(reversed(cleaned_parts))
        return "_".join(cleaned_parts)

    def _rename_duplicated_columns(self) -> DataFrame:
        """Renomeia colunas removendo redundâncias a partir do final."""
        for old_name in self.df.columns:
            new_name = self._clean_column_name(old_name)
            if new_name != old_name and new_name not in self.df.columns:
                self.df = self.df.withColumnRenamed(old_name, new_name)
        return self.df

    def transform_cartesian_data(self) -> DataFrame:
        data = self.df
        "Transform from original dataframe.\n\n        Args:\n            data (pyspark dataframe): dataframe\n\n        Returns:\n            pyspark dataframe: Transformed Dataframe\n        "
        columns = [c[0] for c in data.dtypes if "array" not in c[1]]
        array_columns = [c[0] for c in data.dtypes if "array" in c[1]]
        if len(array_columns) > 0:
            dfs = []
            for column in array_columns:
                df_explode = data.select(
                    *columns, explode_outer(col(column)).alias(column)
                )
                dfs.append(df_explode)
            joined_df = dfs[0]
            for i in range(1, len(dfs)):
                joined_df = joined_df.join(dfs[i], on=columns, how="outer")
            selected_columns = [*columns, *array_columns]
            self.df = joined_df.select(*selected_columns).distinct()
            return self.df
        else:
            return self.df

    def transform_data(self) -> DataFrame:
        data = self.df
        "Transform from original dataframe.\n\n        Args:\n            data (pyspark dataframe): dataframe\n\n        Returns:\n            pyspark dataframe: Transformed Dataframe\n        "
        array_columns = [c[0] for c in data.dtypes if "array" in c[1]]
        if len(array_columns) > 0:
            for column in array_columns:
                max_length = data.select(F.max(F.size(column))).collect()[0][0]
                for i in range(max_length):
                    data = data.withColumn(f"{column}_{i}", F.col(column).getItem(i))
                data = data.drop(column)
            self.df = data
            return data
        else:
            return data.distinct()

    def unnest_df(self) -> DataFrame:
        try:
            flattened_df = self.df
            struct_cols_exist = True

            while struct_cols_exist:
                struct_cols_exist = False
                select_exprs = []
                # Itera sobre as colunas no schema atual do DataFrame
                for field in flattened_df.schema.fields:
                    field_name = field.name
                    field_type = field.dataType

                    if isinstance(field_type, StructType):
                        struct_cols_exist = (
                            True  # Marca que encontramos um struct nesta iteração
                        )
                        # Itera sobre os campos *dentro* do StructType
                        for child_field in field_type.fields:
                            child_field_name = child_field.name
                            # Cria a expressão para selecionar o campo filho com um alias
                            # Ex: coluna 'address.city' vira 'address_city'
                            select_exprs.append(
                                col(f"`{field_name}`.`{child_field_name}`").alias(
                                    f"{field_name}_{child_field_name}"
                                )
                            )
                    else:
                        # Se não for StructType, mantém a coluna como está
                        select_exprs.append(col(f"`{field_name}`"))

                if struct_cols_exist:
                    # Recria o DataFrame selecionando as expressões (colunas normais + filhos dos structs)
                    flattened_df = flattened_df.select(select_exprs)
                    # O loop continuará para verificar o *novo* schema de flattened_df
                else:
                    # Se nenhum struct foi encontrado nesta iteração, o processo terminou
                    break  # Sai do loop while
            self.df = flattened_df
            return flattened_df
        except Exception as e:
            print(f"Error unnesting DataFrame {self.table}")
            print(e)
            raise e

    def flatten_df(self) -> DataFrame:
        nested_df = self.df
        "Receive as nested(struct) df and flat columns.\n\n        Args:\n            nested_df (pyspark dataframe): dataframe\n\n        Returns:\n            pyspark dataframe: Flatted dataframe\n        "
        stack = [((), nested_df)]
        columns = []
        while len(stack) > 0:
            parents, df = stack.pop()
            flat_cols = [
                col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
                for c in df.dtypes
                if c[1][:6] != "struct"
            ]
            nested_cols = [c[0] for c in df.dtypes if c[1][:6] == "struct"]
            columns.extend(flat_cols)
            for nested_col in nested_cols:
                projected_df = df.select(nested_col + ".*")
                stack.append((parents + (nested_col,), projected_df))
        self.df = nested_df.select(columns)
        return self.df

    def has_array_or_struct_columns(self) -> bool:
        """Receive df and check if exist colunm type array or struct.

        Args:
            df (pyspark dataframe): dataframe

        Returns:
            boolean: True or False
        """
        for column_name, data_type in self.df.dtypes:
            if data_type.startswith("array") or data_type.startswith("struct"):
                return True
        return False

    def flatten_mongo_data(self) -> DataFrame:
        """Desaninha dados do MongoDB mantendo as colunas de controle do Airbyte.

        Returns:
            DataFrame: DataFrame com dados desaninhados e colunas de controle.
        """
        try:
            df = self.df
            # Separa as colunas de controle do Airbyte
            control_columns = [
                col
                for col in df.columns
                if col.startswith("_ab_cdc") or col.startswith("_airbyte")
            ]

            if "data" in df.columns:
                # Seleciona as colunas do campo data e as colunas de controle
                df = df.select(
                    *[
                        col("data." + f.name).alias(f.name)
                        for f in df.schema["data"].dataType.fields
                    ],
                    *[col(c) for c in control_columns],
                )
            self.df = df
            return df
        except Exception as e:
            print(f"Error flattening MongoDB data from table {self.table}")
            print(e)
            raise e

    def flatten_and_transform(self, cartesian_product: bool = False) -> DataFrame:
        while self.has_array_or_struct_columns():
            self.df = self.flatten_df()
            if cartesian_product:
                self.df = self.transform_cartesian_data()
            else:
                self.df = self.transform_data()
        return self.df

    def select_columns(self, columns: list[str]) -> DataFrame:
        try:
            self.df = self.df.select(*columns)
            return self.df
        except Exception as e:
            print(f"Error selecting columns from table {self.table}")
            print(e)
            raise e

    def delete_columns(self, columns: list[str]) -> DataFrame:
        try:
            self.df = self.df.drop(*columns)
            return self.df
        except Exception as e:
            print(f"Error deleting columns from table {self.table}")
            print(e)
            raise e

    def _clean_airbyte_columns(self) -> DataFrame:
        try:
            columns_to_drop = [
                col for col in self.df.columns if col.startswith("_airbyte")
            ]
            for col_name in ["year", "month", "day"]:
                if col_name in self.df.columns:
                    columns_to_drop.append(col_name)
            if columns_to_drop:
                self.df = self.df.drop(*columns_to_drop)
            return self.df
        except Exception as e:
            print(
                f"Error cleaning Airbyte and partition columns from table {self.table}"
            )
            print(e)
            return e

    def _clean_cdc_columns(self) -> DataFrame:
        try:
            columns = self.df.columns
            for column in columns:
                if column.startswith("_ab_cdc"):
                    self.df = self.df.drop(column)
            return self.df
        except Exception as e:
            print(f"Error cleaning CDC columns from table {self.table}")
            print(e)

    def _calculate_partitions(self) -> int:
        data_size = self.df.rdd.map(lambda row: len(str(row))).sum()
        part_size = 128 * 1024 * 1024
        data_size_parquet = data_size - data_size * (75 / 100)
        target_part = int(data_size_parquet / part_size) + 1
        return target_part

    def repartition_table(self, target_part: int = 0) -> DataFrame:
        try:
            if target_part == 0:
                target_part = self._calculate_partitions()
            self.df = self.df.coalesce(target_part)
            return self.df
        except Exception as e:
            print(f"Error repartitioning table {self.table}")
            print(e)
            raise e

    def correct_timestamp_columns(self, only: list[str]  = []):
        """Corrige colunas de timestamp armazenadas como string.

        Identifica colunas String que parecem ser timestamps (nome contém 'date' ou 'time'),
        e faz cast para TimestampType. Evita recriar o DataFrame quando não houver
        colunas candidatas.

        Args:
            only (list[str] | None): Lista opcional de colunas a tratar.
                Se fornecida, apenas essas colunas serão analisadas.

        Returns:
            DataFrame: DataFrame atualizado (ou o original, se nada mudar).
        """
        df = self.df
        try:
            # 1) Descobre colunas candidatas
            candidates: set[str] = set()
            for field in df.schema.fields:
                name, dt = field.name, field.dataType
                if only and name not in only:
                    continue
                if str(dt).lower().startswith("timestamp"):
                    candidates.add(name)
                if isinstance(dt, StringType) and (
                    "date" in name.lower() or "time" in name.lower()
                ):
                    candidates.add(name)

            # 2) Se não há candidatas, retorna o DF original
            if not candidates:
                return df

            # 3) Monta expressões para recriar apenas as colunas necessárias
            exprs = []
            for field in df.schema.fields:
                name, dt = field.name, field.dataType

                if name in candidates:
                    exprs.append(F.col(name).cast(TimestampType()).alias(name))
                else:
                    exprs.append(F.col(name))

            # 4) Só aqui recria o DF
            self.df = df.select(*exprs)
            return self.df

        except Exception as e:
            print(f"Error correcting timestamp columns for table {self.table}")
            print(e)
            raise e

    def process_and_flatten_table(
        self,
        deduplicate_orderby_column: str = "",
        repartition_size: int = 0,
        new_schema_expr: dict[str, str] = {},
        cartesian_product: bool = False,
    ) -> DataFrame:
        if self.isEmpty():
            self.logger.warn("DataFrame is empty")
            return self.df
        try:
            if self.write_mode == "append":
                self.deduplicate_table(orderby_column=deduplicate_orderby_column)
            self._clean_airbyte_columns()
            self.correct_timestamp_columns()
            for column, expression in new_schema_expr.items():
                self.set_schema(column, expression)
            self.flatten_and_transform(cartesian_product)
            self.repartition_table(repartition_size)
            return self.df
        except Exception as e:
            print(f"Error processing table {self.table}")
            print(e)
            raise e

    def nullify_invalid_timestamps(self) -> DataFrame:
        # Processo de converter coluna como data e timestamp preenchida equivocadamente
        try:
            for field in self.df.schema.fields:
                if isinstance(field.dataType, TimestampType):
                    self.df = self.df.withColumn(
                        field.name,
                        F.when(
                            F.col(field.name).cast("string").rlike("^0000-|^0001-01-"),
                            F.lit(None),
                        ).otherwise(F.col(field.name)),
                    )

                elif isinstance(field.dataType, DateType):
                    self.df = self.df.withColumn(
                        field.name,
                        F.when(
                            F.col(field.name).cast("string").rlike("^0000-|^0001-01-"),
                            F.lit(None),
                        ).otherwise(F.col(field.name)),
                    )

            return self.df
        except Exception as e:
            print(f"Error nullifying invalid timestamps in table {self.table}")
            print(e)
            raise e

    def process_table(
        self,
        new_schema_expr: dict[str, str] = {},
        repartition_size: int = 0,
        deduplicate_orderby_column: list[str] = [],
        table_name: str = "",
    ) -> DataFrame:
        """Runs full transformation pipeline: deduplication, cleanup, schema, and repartitioning.

        Args:
            new_schema_expr (dict[str, str]): Column transformations.
            repartition_size (int): Target number of partitions.
            deduplicate_orderby_column (str): Column for deduplication ordering.

        Returns:
            DataFrame: Final transformed DataFrame."""
        if self.isEmpty():
            self.logger.warn("DataFrame is empty")
            return self.df
        try:
            self.nullify_invalid_timestamps()
            # Limita o tamanho das strings antes de processar
            self.limit_string_length()
            # Aplica trim nas colunas string
            # self.trim_string_columns()

        except Exception as e:
            print(f"Error processing table {self.table_name}")
            print(e)
            raise e
        try:
            if self.write_mode == "append":
                self.deduplicate_table(orderby_column=deduplicate_orderby_column)
            self._clean_airbyte_columns()
            self.correct_timestamp_columns()
            for column, expression in new_schema_expr.items():
                self.set_schema(column, expression)
            self.fill_array_column()
            return self.df
        except Exception as e:
            print(f"Error processing table {self.table_name}")
            print(e)
            raise e

    def remove_prefix_column(self, prefix: str) -> DataFrame:
        try:
            for column in self.df.columns:
                if column.startswith(prefix):
                    new_column = column.replace(prefix, "")
                    self.df = self.df.withColumnRenamed(column, new_column)
            return self.df
        except Exception as e:
            print(
                f"Error removing prefix {prefix} from columns of table {self.table_name}"
            )
            print(e)
            raise e

    def limit_string_length(self, columns: list[str] = None) -> DataFrame:
        """Limita o tamanho das strings para menos de 32.000 caracteres (considerando caracteres acentuados).

        Args:
            columns (list[str], optional): Lista de colunas para limitar. Se None, aplica em todas as colunas string.

        Returns:
            DataFrame: DataFrame com strings limitadas.
        """
        try:
            # Define o limite de caracteres considerando caracteres acentuados
            CHAR_LIMIT = 32000

            def process_column(column_name: str, data_type) -> DataFrame:
                if isinstance(data_type, StringType):
                    return self.df.withColumn(
                        column_name,
                        F.expr(
                            f"CASE WHEN length({column_name}) > {CHAR_LIMIT} THEN substring({column_name}, 1, {CHAR_LIMIT}) ELSE {column_name} END"
                        ),
                    )
                elif isinstance(data_type, ArrayType):
                    element_type = data_type.elementType
                    if isinstance(element_type, StringType):
                        return self.df.withColumn(
                            column_name,
                            F.expr(
                                f"transform({column_name}, x -> CASE WHEN length(x) > {CHAR_LIMIT} THEN substring(x, 1, {CHAR_LIMIT}) ELSE x END)"
                            ),
                        )
                    elif isinstance(element_type, StructType):
                        struct_fields = []
                        for field in element_type.fields:
                            if isinstance(field.dataType, StringType):
                                struct_fields.append(
                                    f"CASE WHEN length(x.{field.name}) > {CHAR_LIMIT} THEN substring(x.{field.name}, 1, {CHAR_LIMIT}) ELSE x.{field.name} END as {field.name}"
                                )
                            else:
                                struct_fields.append(f"x.{field.name} as {field.name}")
                        return self.df.withColumn(
                            column_name,
                            F.expr(
                                f"transform({column_name}, x -> struct({', '.join(struct_fields)}))"
                            ),
                        )
                    elif isinstance(element_type, ArrayType):
                        return process_column(column_name, element_type)
                elif isinstance(data_type, StructType):
                    struct_fields = []
                    for field in data_type.fields:
                        if isinstance(field.dataType, StringType):
                            struct_fields.append(
                                f"CASE WHEN length({column_name}.{field.name}) > {CHAR_LIMIT} THEN substring({column_name}.{field.name}, 1, {CHAR_LIMIT}) ELSE {column_name}.{field.name} END as {field.name}"
                            )
                        else:
                            struct_fields.append(
                                f"{column_name}.{field.name} as {field.name}"
                            )
                    return self.df.withColumn(
                        column_name, F.expr(f"struct({', '.join(struct_fields)})")
                    )
                return self.df

            if columns is None:
                columns = []
                for field in self.df.schema.fields:
                    if isinstance(field.dataType, (StringType, ArrayType, StructType)):
                        columns.append(field.name)

            for column in columns:
                if column in self.df.columns:
                    field_type = self.df.schema[column].dataType
                    self.df = process_column(column, field_type)

            return self.df
        except Exception as e:
            print(f"Error limiting string length in table {self.table_name}")
            print(e)
            raise e


    def _check_iceberg_table_exists(self, location: str) -> bool:
        """Verifica se uma tabela Iceberg existe no caminho especificado.
        
        Args:
            location (str): Caminho s3a:// da tabela Iceberg
            
        Returns:
            bool: True se a tabela existe, False caso contrário.
        """
        try:
            # Tenta ler a tabela Iceberg diretamente do caminho
            self.spark.read.format("iceberg").load(location).limit(0).collect()
            return True
        except Exception:
            # Se der erro, a tabela não existe
            return False

    def set_table_name(self, table_name: str) -> None:
        """Define um novo nome para a tabela de destino.

        Args:
            table_name (str): Novo nome da tabela.
        """
        self.table_name = table_name

    def write_parquet_table(self, path: str = "", write_mode: str = "overwrite"):
        """Writes the DataFrame to a Parquet file on S3.

        Args:
            path (str): Output path on S3.
            write_mode (str): Write mode ('overwrite' or 'append')."""
        if self.isEmpty():
            self.logger.warn("DataFrame is empty")
            print(f"Dataframe vazio {self.table_name}")
            return
        
        # Usa o path passado como parâmetro, senão usa target_path
        final_path = path if path else self.target_path
        if not final_path:
            raise ValueError(f"No path provided for writing table {self.table_name}")
        
        try:
            print(f"Writing table {self.table_name} to Parquet: {final_path}")
            self.df.write.parquet(final_path, mode=write_mode, compression="snappy")
            print(f"Successfully wrote table {self.table_name} to {final_path}")
        except Exception as e:
            print(f"Error writing table {self.table_name} to parquet")
            print(e)
            raise e

    def write_iceberg_table(self):
        """Writes the DataFrame to an Iceberg table using SparkCatalog (local, MinIO-backed)."""
        if self.isEmpty():
            self.logger.warn("DataFrame is empty")
            print(f"Dataframe vazio {self.table_name}")
            return
        try:
            # Carrega PK se necessário
            if not self.primary_keys and self.write_mode == "append":
                metadata = self._retrieve_table_metadata(self.pk_bucket, self.pk_file)
                if metadata:
                    self.primary_keys = self._process_json_data(metadata)
            if not self.primary_keys:
                self.primary_keys = ["id"]
            
            # Limpa colunas técnicas antes de escrever
            self._clean_cdc_columns()
            self._clean_airbyte_columns()
            
            # Define o caminho no MinIO para a tabela Iceberg na silver
            # Se iceberg_target_path não foi fornecido, usa padrão: s3a://datalake/silver/{domain}/{subdomain}/{table_name}
            if self.iceberg_target_path:
                location = self.iceberg_target_path
            elif self.domain and self.subdomain:
                location = f"s3a://datalake/silver/{self.domain}/{self.subdomain}/{self.table_name}"
            elif self.domain:
                location = f"s3a://datalake/silver/{self.domain}/{self.table_name}"
            else:
                location = f"s3a://datalake/silver/{self.table_name}"
            
            # Verifica se a tabela já existe
            table_exists = self._check_iceberg_table_exists(location)
            
            if table_exists and self.write_mode == "append":
                # Para append com Iceberg, fazemos append simples
                # O Iceberg gerencia a deduplicação através de suas propriedades
                # Filtra dados deletados se houver coluna CDC
                write_df = self.df
                if "_ab_cdc_deleted_at" in self.df.columns:
                    write_df = self.df.filter(F.col("_ab_cdc_deleted_at").isNull())
                
                # Append na tabela Iceberg existente usando caminho direto
                # Para append, apenas escreve no mesmo caminho (Iceberg gerencia automaticamente)
                write_df.write \
                    .format("iceberg") \
                    .mode("append") \
                    .option("write-target-file-size-bytes", "134217728") \
                    .option("write.parquet.compression-codec", "snappy") \
                    .save(location)
            elif table_exists and self.write_mode == "overwrite":
                # Para overwrite, deleta e recria usando caminho direto
                # Deleta os arquivos do diretório (não usa catálogo)
                try:
                    # Tenta deletar usando Hadoop FileSystem
                    hadoop_conf = self.spark._jsc.hadoopConfiguration()
                    fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                        self.spark._jvm.java.net.URI(location),
                        hadoop_conf
                    )
                    path = self.spark._jvm.org.apache.hadoop.fs.Path(location)
                    if fs.exists(path):
                        fs.delete(path, True)  # True = recursive
                except Exception as delete_error:
                    print(f"Aviso: Não foi possível deletar tabela existente: {delete_error}")
                    pass
                
                # Escreve nova tabela usando caminho direto
                self.df.write \
                    .format("iceberg") \
                    .mode("overwrite") \
                    .option("write-target-file-size-bytes", "134217728") \
                    .option("write.parquet.compression-codec", "snappy") \
                    .save(location)
            else:
                # Cria nova tabela
                write_df = self.df
                if "_ab_cdc_deleted_at" in self.df.columns:
                    write_df = self.df.filter(F.col("_ab_cdc_deleted_at").isNull())
                
                # Escreve usando caminho direto em vez de catálogo
                write_df.write \
                    .format("iceberg") \
                    .mode("overwrite") \
                    .option("write-target-file-size-bytes", "134217728") \
                    .option("write.parquet.compression-codec", "snappy") \
                    .save(location)
        except Exception as e:
            print(f"Error writing table {self.table_name} to Iceberg")
            print(e)
            raise e
    
    def _build_iceberg_merge_query_direct(self, location: str) -> str:
        """Constrói query MERGE para Iceberg usando caminho direto s3a://.
        
        Para Iceberg, precisamos criar uma temp view da tabela existente e fazer merge nela.
        
        Args:
            location (str): Caminho s3a:// da tabela Iceberg
            
        Returns:
            str: Query SQL para merge
        """
        # Colunas de negócio (sem CDC/Airbyte)
        source_table_columns = [
            column
            for column in self.df.columns
            if not column.startswith("_ab_cdc") and not column.startswith("_airbyte")
        ]
        
        # Condição ON para primary keys
        on_conditions = [f"t.{pk} = s.{pk}" for pk in self.primary_keys]
        on_clause = " AND ".join(on_conditions)
        
        # Condição DELETE
        delete_clause = ""
        if "_ab_cdc_deleted_at" in self.df.columns:
            delete_clause = "WHEN MATCHED AND s._ab_cdc_deleted_at IS NOT NULL THEN DELETE"
        
        # UPDATE SET
        update_set = ", ".join([f"t.{col} = s.{col}" for col in source_table_columns])
        
        # INSERT
        insert_columns = ", ".join(source_table_columns)
        insert_values = ", ".join([f"s.{col}" for col in source_table_columns])
        insert_condition = ""
        if "_ab_cdc_deleted_at" in self.df.columns:
            insert_condition = "AND s._ab_cdc_deleted_at IS NULL"
        
        # Para Iceberg, precisamos fazer merge diretamente na tabela usando o formato iceberg
        # Cria uma temp view da tabela existente para usar no MERGE
        existing_df = self.spark.read.format("iceberg").load(location)
        existing_df.createOrReplaceTempView(f"tmp_{self.table_name}_existing")
        
        # Monta a query MERGE
        query_parts = [
            f"MERGE INTO tmp_{self.table_name}_existing t",
            f"USING tmp_{self.table_name}_new s",
            f"ON {on_clause}"
        ]
        
        if delete_clause:
            query_parts.append(delete_clause)
        
        query_parts.append(f"WHEN MATCHED THEN UPDATE SET {update_set}")
        
        if insert_condition:
            query_parts.append(f"WHEN NOT MATCHED {insert_condition} THEN INSERT ({insert_columns}) VALUES ({insert_values})")
        else:
            query_parts.append(f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})")
        
        query = " ".join(query_parts)
        
        # Após o merge, precisamos escrever de volta na tabela Iceberg
        # Mas na verdade, para Iceberg, o merge deve ser feito diretamente na tabela
        # Vamos usar uma abordagem diferente: fazer merge via DataFrame API
        return query
    
    def _build_iceberg_merge_query(self) -> str:
        """Constrói query MERGE para Iceberg (método legado, mantido para compatibilidade).
        
        Use _build_iceberg_merge_query_direct() para novas implementações.
        """
        # Tenta usar o location padrão
        location = self.iceberg_target_path if self.iceberg_target_path else f"s3a://datalake/silver/{self.table_name}"
        return self._build_iceberg_merge_query_direct(location)


    def write_sqlite_table(
        self, db_path: str = "silver.db", schema: str = "main"
    ):
        """Escreve DataFrame no SQLite usando estratégia incremental igual ao Iceberg.

        Implementa carga incremental com merge:
        1. Limpa colunas técnicas (CDC/Airbyte)
        2. Faz schema evolution se necessário
        3. Executa merge baseado em primary keys

        Funciona com write_mode='append' (incremental) e write_mode='overwrite'.

        Args:
            db_path (str): Caminho do arquivo SQLite (ex: 'silver.db' ou '/path/to/silver.db')
            schema (str): Schema do SQLite (padrão: 'main')
        """
        if self.isEmpty():
            self.logger.warn("DataFrame is empty")
            return

        if self.write_mode not in ["append", "overwrite"]:
            raise ValueError(
                "write_sqlite_table funciona com write_mode='append' ou 'overwrite'"
            )

        try:
            # 1. Limpa colunas técnicas antes de processar
            self._clean_cdc_columns()
            self._clean_airbyte_columns()

            # 2. Carrega Primary keys
            if not self.primary_keys and self.write_mode == "append":
                metadata = self._retrieve_table_metadata(self.pk_bucket, self.pk_file)
                if metadata:
                    self.primary_keys = self._process_json_data(metadata)
            if not self.primary_keys:
                self.primary_keys = ["id"]

            # 3. Colunas de negócio (já limpas)
            source_table_columns = self.df.columns

            # 4. Verifica se tabela existe e lê colunas
            table_exists = self._check_sqlite_table_exists(db_path, self.table_name)
            target_table_columns = []
            if table_exists:
                target_table_columns = self._get_sqlite_table_columns(
                    db_path, self.table_name
                )

            # 5. Schema evolution: adiciona colunas faltantes
            missing_columns = [
                col
                for col in source_table_columns
                if col.lower() not in [c.lower() for c in target_table_columns]
            ]

            # 6. Cria temp view
            self.df.createOrReplaceTempView(f"tmp_{self.table_name}")

            # 7. Executa merge ou overwrite
            if table_exists and self.write_mode == "append":
                # Faz merge incremental
                self._execute_sqlite_merge(
                    db_path, self.table_name, source_table_columns, missing_columns
                )
            elif self.write_mode == "overwrite":
                # Overwrite: deleta e recria
                self._execute_sqlite_overwrite(
                    db_path, self.table_name, source_table_columns, missing_columns
                )
            else:
                # Cria nova tabela
                self._execute_sqlite_create(
                    db_path, self.table_name, source_table_columns
                )

        except Exception as e:
            print(f"Erro escrevendo {self.table_name} no SQLite: {e}")
            raise e

    def _get_sqlite_jdbc_url(self, db_path: str) -> str:
        """Constrói URL JDBC para SQLite.
        
        Args:
            db_path (str): Caminho do arquivo SQLite
            
        Returns:
            str: URL JDBC formatada
        """
        # Garante que o caminho é absoluto
        import os
        if not os.path.isabs(db_path):
            # Se for relativo, cria no diretório atual ou em um diretório padrão
            db_path = os.path.abspath(db_path)
        
        # SQLite JDBC URL: jdbc:sqlite:/path/to/database.db
        return f"jdbc:sqlite:{db_path}"
    
    def _check_sqlite_table_exists(self, db_path: str, table_name: str) -> bool:
        """Verifica se uma tabela existe no SQLite.
        
        Args:
            db_path (str): Caminho do arquivo SQLite
            table_name (str): Nome da tabela
            
        Returns:
            bool: True se a tabela existe
        """
        try:
            jdbc_url = self._get_sqlite_jdbc_url(db_path)
            # Tenta ler a tabela
            self.spark.read.format("jdbc").option("url", jdbc_url).option(
                "dbtable", table_name
            ).load().limit(0).collect()
            return True
        except Exception:
            return False
    
    def _get_sqlite_table_columns(self, db_path: str, table_name: str) -> list[str]:
        """Lê colunas de uma tabela SQLite.
        
        Args:
            db_path (str): Caminho do arquivo SQLite
            table_name (str): Nome da tabela
            
        Returns:
            list[str]: Lista de nomes das colunas
        """
        try:
            jdbc_url = self._get_sqlite_jdbc_url(db_path)
            df = self.spark.read.format("jdbc").option("url", jdbc_url).option(
                "dbtable", table_name
            ).load()
            return df.columns
        except Exception as e:
            print(f"Erro lendo colunas da tabela {table_name}: {e}")
            return []
    
    def _get_sqlite_type(self, column_name: str) -> str:
        """Mapeia tipo Spark para SQLite.
        
        Args:
            column_name (str): Nome da coluna
            
        Returns:
            str: Tipo SQLite correspondente
        """
        try:
            if column_name.lower() not in self.df.columns:
                return "TEXT"
            
            spark_type = self.df.schema[column_name.lower()].dataType
            
            if isinstance(spark_type, (ArrayType, StructType, MapType)):
                return "TEXT"  # SQLite não tem tipos complexos, usa JSON como texto
            elif isinstance(spark_type, StringType):
                return "TEXT"
            elif isinstance(spark_type, IntegerType):
                return "INTEGER"
            elif isinstance(spark_type, LongType):
                return "INTEGER"  # SQLite INTEGER pode armazenar até 64 bits
            elif isinstance(spark_type, DoubleType):
                return "REAL"
            elif isinstance(spark_type, BooleanType):
                return "INTEGER"  # SQLite usa 0/1 para boolean
            elif isinstance(spark_type, DateType):
                return "TEXT"  # SQLite não tem tipo DATE nativo, usa TEXT ISO8601
            elif isinstance(spark_type, TimestampType):
                return "TEXT"  # SQLite não tem tipo TIMESTAMP nativo, usa TEXT ISO8601
            else:
                return "TEXT"
        except Exception as e:
            return "TEXT"
    
    def _execute_sqlite_create(
        self, db_path: str, table_name: str, columns: list[str]
    ):
        """Cria uma nova tabela no SQLite.
        
        Args:
            db_path (str): Caminho do arquivo SQLite
            table_name (str): Nome da tabela
            columns (list[str]): Lista de colunas
        """
        jdbc_url = self._get_sqlite_jdbc_url(db_path)
        
        # Filtra dados deletados se houver coluna CDC
        write_df = self.df
        if "_ab_cdc_deleted_at" in self.df.columns:
            write_df = self.df.filter(F.col("_ab_cdc_deleted_at").isNull())
        
        # Escreve no SQLite
        write_df.write.format("jdbc").mode("overwrite").option("url", jdbc_url).option(
            "dbtable", table_name
        ).option("createTableColumnTypes", self._build_sqlite_column_types(columns)).save()
    
    def _execute_sqlite_overwrite(
        self, db_path: str, table_name: str, columns: list[str], missing_columns: list[str]
    ):
        """Executa overwrite no SQLite (deleta e recria).
        
        Args:
            db_path (str): Caminho do arquivo SQLite
            table_name (str): Nome da tabela
            columns (list[str]): Lista de colunas
            missing_columns (list[str]): Colunas faltantes (para schema evolution)
        """
        jdbc_url = self._get_sqlite_jdbc_url(db_path)
        
        # Schema evolution: adiciona colunas faltantes antes de overwrite
        if missing_columns:
            for col in missing_columns:
                print(f"Schema evolution - Adicionando coluna faltante: {col}")
                col_type = self._get_sqlite_type(col)
                try:
                    self.spark.read.format("jdbc").option("url", jdbc_url).option(
                        "dbtable", f"(ALTER TABLE {table_name} ADD COLUMN {col} {col_type}) AS SELECT 1"
                    ).load().collect()
                except:
                    # SQLite não suporta ALTER TABLE em algumas versões via JDBC
                    # Vamos fazer o overwrite que recria a tabela
                    pass
        
        # Overwrite: deleta e recria
        self.df.write.format("jdbc").mode("overwrite").option("url", jdbc_url).option(
            "dbtable", table_name
        ).option("createTableColumnTypes", self._build_sqlite_column_types(columns)).save()
    
    def _execute_sqlite_merge(
        self, db_path: str, table_name: str, columns: list[str], missing_columns: list[str]
    ):
        """Executa merge incremental no SQLite.
        
        Args:
            db_path (str): Caminho do arquivo SQLite
            table_name (str): Nome da tabela
            columns (list[str]): Lista de colunas
            missing_columns (list[str]): Colunas faltantes (para schema evolution)
        """
        jdbc_url = self._get_sqlite_jdbc_url(db_path)
        
        # Filtra dados deletados se houver coluna CDC
        write_df = self.df
        if "_ab_cdc_deleted_at" in self.df.columns:
            write_df = self.df.filter(F.col("_ab_cdc_deleted_at").isNull())
        
        # Lê dados existentes
        try:
            existing_df = self.spark.read.format("jdbc").option("url", jdbc_url).option(
                "dbtable", table_name
            ).load()
            
            # Schema evolution: adiciona colunas faltantes ao DataFrame existente
            if missing_columns:
                for col in missing_columns:
                    print(f"Schema evolution - Adicionando coluna faltante: {col}")
                    existing_df = existing_df.withColumn(col, F.lit(None).cast(self._get_sqlite_type(col)))
            
            # Faz merge: combina dados existentes com novos
            # Usa unionByName para combinar, permitindo colunas faltantes
            combined_df = existing_df.unionByName(write_df, allowMissingColumns=True)
            
            # Deduplica baseado em PK (mantém o mais recente se houver coluna de timestamp)
            order_cols = []
            if "_ab_cdc_updated_at" in combined_df.columns:
                order_cols.append(F.col("_ab_cdc_updated_at").desc())
            elif "updated_at" in combined_df.columns:
                order_cols.append(F.col("updated_at").desc())
            elif "_ab_updated_at" in combined_df.columns:
                order_cols.append(F.col("_ab_updated_at").desc())
            
            if not order_cols:
                order_cols.append(F.lit(1))
            
            window = Window.partitionBy([F.col(pk) for pk in self.primary_keys]).orderBy(*order_cols)
            deduplicated_df = combined_df.withColumn("rank", F.row_number().over(window)).filter(
                F.col("rank") == 1
            ).drop("rank")
            
            # Escreve resultado final (overwrite para substituir completamente)
            deduplicated_df.write.format("jdbc").mode("overwrite").option("url", jdbc_url).option(
                "dbtable", table_name
            ).option("createTableColumnTypes", self._build_sqlite_column_types(columns)).save()
            
        except Exception as e:
            # Se a tabela não existe ou erro, cria nova
            print(f"Tabela não existe ou erro ao ler, criando nova: {e}")
            self._execute_sqlite_create(db_path, table_name, columns)
    
    def _build_sqlite_column_types(self, columns: list[str]) -> str:
        """Constrói string de tipos de colunas para SQLite.
        
        Args:
            columns (list[str]): Lista de nomes de colunas
            
        Returns:
            str: String formatada para createTableColumnTypes do JDBC
        """
        type_mapping = {}
        for col in columns:
            type_mapping[col] = self._get_sqlite_type(col)
        
        return ", ".join([f"{col} {type_mapping[col]}" for col in columns])


