# 🪣 Inicialização Automática do MinIO

## 📋 O que faz

Quando o MinIO sobe, automaticamente:

1. ✅ Aguarda o MinIO estar disponível
2. ✅ Cria o bucket `datalake`
3. ✅ Cria as pastas:
   - `bronze/`
   - `silver/`
   - `gold/`

## 🚀 Como funciona

O docker-compose.yml inclui um serviço `minio-init` que:

- Depende do MinIO estar saudável (healthcheck)
- Executa o script `infra/minio-init.sh`
- Usa o MinIO Client (mc) para criar a estrutura
- Executa apenas uma vez (restart: "no")

## 📁 Estrutura criada

```
datalake/
├── bronze/
├── silver/
└── gold/
```

## 🔍 Verificar se funcionou

### Via MinIO Console:
1. Acesse: http://localhost:9001
2. Login: `minioadmin` / `minioadmin123`
3. Vá em "Buckets" → `datalake`
4. Você verá as pastas: bronze, silver, gold

### Via MinIO Client:
```bash
docker exec minio-init mc ls -r local/datalake/
```

### Via código Python:
```python
import boto3

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin123'
)

# Lista buckets
buckets = s3.list_buckets()
print("Buckets:", [b['Name'] for b in buckets['Buckets']])

# Lista objetos no bucket datalake
objects = s3.list_objects_v2(Bucket='datalake', Prefix='')
if 'Contents' in objects:
    print("Objetos:", [obj['Key'] for obj in objects['Contents']])
```

## 🐛 Troubleshooting

### O container minio-init falhou

Verifique os logs:
```bash
docker-compose logs minio-init
```

### O bucket não foi criado

1. Verifique se o MinIO está rodando:
   ```bash
   docker-compose ps minio
   ```

2. Execute manualmente:
   ```bash
   docker exec minio mc alias set local http://minio:9000 minioadmin minioadmin123
   docker exec minio mc mb local/datalake
   docker exec minio mc ls local/datalake
   ```

### Re-executar a inicialização

Se precisar re-executar:
```bash
docker-compose up minio-init
```

## 📝 Notas

- O script cria objetos `.gitkeep` vazios nas pastas para garantir que elas existam
- No MinIO/S3, "pastas" são apenas prefixos, mas os objetos vazios ajudam na visualização
- O container `minio-init` executa apenas uma vez e depois para (restart: "no")
- Se o bucket já existir, o script não falha, apenas informa que já existe

