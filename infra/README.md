# 🐳 Docker Compose - Spark + MinIO

Este docker-compose configura um ambiente completo com:
- **MinIO**: Armazenamento S3-compatível
- **Spark**: PySpark (AWS Glue 5.0) configurado para usar MinIO

## 🚀 Como usar

### 1. Iniciar os serviços

```bash
cd infra
docker-compose up -d
```

### 2. Acessar os serviços

- **MinIO Console**: http://localhost:9001
  - Usuário: `minioadmin`
  - Senha: `minioadmin123`
- **MinIO API**: http://localhost:9000
- **Spark UI**: http://localhost:4040 (quando Spark estiver rodando)
- **Spark History Server**: http://localhost:18080

### 3. Usar Spark dentro do container

```bash
# Entrar no container Spark
docker exec -it spark bash

# Dentro do container, você pode usar Python com Spark
python3
```

Ou execute scripts Python diretamente:

```bash
docker exec -it spark python3 /home/hadoop/workspace/seu_script.py
```

### 4. Parar os serviços

```bash
docker-compose down
```

### 5. Parar e remover volumes (limpar dados)

```bash
docker-compose down -v
```

## 📁 Estrutura

- O diretório raiz do projeto é montado em `/home/hadoop/workspace` no container Spark
- Os dados do MinIO são persistidos no volume `minio_data`
- Os dados permanecem mesmo após parar os containers

## 🔧 Configurações

### MinIO
- **Endpoint**: `http://localhost:9000`
- **Credenciais**: `minioadmin` / `minioadmin123`
- **Porta API**: `9000`
- **Porta Console**: `9001`

## 💡 Usar Spark com MinIO

### Dentro do container Spark:

```python
from pyspark.sql import SparkSession

# Spark já está configurado para MinIO
spark = SparkSession.builder \
    .appName("AnalyticsDataBronze") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

# Ler do MinIO
df = spark.read.json("s3a://datalake/bronze/products.json")

# Escrever no MinIO
df.write.parquet("s3a://datalake/silver/output.parquet")
```

**Nota**: Dentro do container, use `http://minio:9000` como endpoint (nome do serviço no docker-compose)

### Usar boto3 (dentro ou fora do container):

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",  # ou http://minio:9000 dentro do container
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin123",
)

# Criar bucket
s3.create_bucket(Bucket="datalake")

# Upload
s3.put_object(
    Bucket="datalake",
    Key="bronze/teste.txt",
    Body=b"Conteudo do arquivo"
)
```

## 🐛 Troubleshooting

### Porta já em uso
- Altere as portas no `docker-compose.yml` se necessário
- Exemplo: `"9002:9001"` para usar porta 9002 no host para MinIO Console

### Container não inicia
- Verifique se o Docker está rodando: `docker ps`
- Verifique os logs: 
  ```bash
  docker-compose logs minio
  docker-compose logs spark
  ```

### Spark não consegue acessar MinIO
- Verifique se ambos os containers estão rodando: `docker-compose ps`
- Use `http://minio:9000` como endpoint (nome do serviço no docker-compose)
- Verifique a rede: `docker network inspect infra_spark-minio`

### Acessar dados após reiniciar
- Os dados são persistidos no volume `minio_data`
- Mesmo após `docker-compose down`, os dados permanecem
- Para limpar completamente: `docker-compose down -v`
