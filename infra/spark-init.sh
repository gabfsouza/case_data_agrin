#!/bin/bash
# Script de inicialização do Spark com configurações MinIO

# Configurações do Spark para MinIO
export SPARK_CONF_DIR=/opt/spark/conf

# Criar diretório de configuração se não existir
mkdir -p $SPARK_CONF_DIR

# Configurar Spark para MinIO
cat > $SPARK_CONF_DIR/spark-defaults.conf << EOF
# Configurações básicas
spark.driver.memory 1g
spark.executor.memory 1g
spark.sql.adaptive.enabled true
spark.sql.adaptive.coalescePartitions.enabled true

# Configurações MinIO/S3
spark.hadoop.fs.s3a.endpoint http://minio:9000
spark.hadoop.fs.s3a.access.key minioadmin
spark.hadoop.fs.s3a.secret.key minioadmin123
spark.hadoop.fs.s3a.path.style.access true
spark.hadoop.fs.s3a.impl org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.connection.ssl.enabled false
spark.hadoop.fs.s3a.aws.credentials.provider org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
EOF

echo "✅ Configurações do Spark aplicadas!"

