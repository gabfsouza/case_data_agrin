# Case Data Agrin - Pipeline de Dados Bronze-Silver-Gold

## 📖 Sobre o Projeto

Este projeto implementa uma arquitetura de datalake moderna seguindo o padrão **Medallion Architecture** (Bronze-Silver-Gold), utilizando Apache Spark para processamento de dados, MinIO como armazenamento S3-compatível e Prefect como orquestrador de workflows.

## 🏗️ Arquitetura

### A Jornada dos Dados

O pipeline processa dados de e-commerce através de três camadas distintas:

1. **Bronze Layer** (`bronze/`)
   - **Origem**: API FakeStore (produtos, categorias, carrinhos)
   - **Função**: Ingestão bruta dos dados, preservando o formato original
   - **Destino**: MinIO bucket `datalake/bronze/`
   - **Formato**: JSON particionado por data (ano/mês/dia)

2. **Silver Layer** (`silver/`)
   - **Origem**: Dados brutos do Bronze
   - **Função**: Limpeza, validação e enriquecimento dos dados
   - **Processamento**: Spark SQL para transformações e normalização
   - **Destino**: MinIO bucket `datalake/silver/`
   - **Formato**: Parquet otimizado para consultas

3. **Gold Layer** (`gold/`)
   - **Origem**: Dados processados do Silver
   - **Função**: Agregações e modelagem de indicadores de negócio
   - **Processamento**: Cálculo de métricas e KPIs
   - **Destino**: SQLite local (`gold/gold.db`)
   - **Uso**: Análises e relatórios finais

### Orquestração com Prefect

O **Prefect** atua como o maestro da orquestração, garantindo que cada etapa seja executada na ordem correta:

- **Fluxo Sequencial**: Bronze → Silver → Gold
- **Retry Automático**: Em caso de falha, tenta novamente automaticamente
- **Agendamento**: Execução diária às 6h (horário de Brasília)
- **Monitoramento**: Interface web para acompanhamento em tempo real

## 🚀 Como Executar

### Pré-requisitos

- Docker e Docker Compose instalados
- Python 3.8+ (para executar o orquestrador)

### Passo a Passo

1. **Subir a infraestrutura**:
   ```bash
   docker-compose up -d
   ```

2. **Aguardar inicialização** (cerca de 30-60 segundos):
   - MinIO será configurado automaticamente com bucket `datalake` e pastas `bronze`, `silver`, `gold`
   - Spark container estará pronto para processar jobs

3. **Iniciar o orquestrador Prefect**:
   ```bash
   python orchestrator/apollo_flow_prefect.py
   ```
   
   ⚠️ **Importante**: Este comando deve ser executado localmente (não dentro do container) e manterá o processo rodando para que a DAG apareça na interface do Prefect.

### Acessos

Após subir os serviços, você pode acessar:

- **MinIO Console**: http://localhost:9001
  - Usuário: `minioadmin`
  - Senha: `minioadmin123`
  - API S3: http://localhost:9000

- **Prefect UI**: http://localhost:4200
  - Interface para monitorar e executar workflows

- **Spark UI**: http://localhost:4040
  - Monitoramento de jobs Spark em execução

## 📁 Estrutura do Projeto

```
case_data_agrin/
├── bronze/              # Camada Bronze - Ingestão de dados
│   ├── insert_s3.py     # Job principal de ingestão
│   └── fakestore_api.py # Cliente da API FakeStore
├── silver/              # Camada Silver - Processamento e limpeza
│   ├── services/        # Processadores de dados
│   └── adapters/        # Adaptadores para Spark
├── gold/                # Camada Gold - Agregações e indicadores
│   ├── modeling_indicators.py
│   └── gold.db          # Banco SQLite com resultados
├── orchestrator/        # Orquestração Prefect
│   └── apollo_flow_prefect.py
├── infra/               # Configurações de infraestrutura
│   ├── Dockerfile       # Imagem Spark customizada
│   └── requirements.txt # Dependências Python
└── docker-compose.yml   # Orquestração de containers
```

## 🔧 Tecnologias

- **Apache Spark**: Processamento distribuído de dados
- **MinIO**: Armazenamento S3-compatível
- **Prefect**: Orquestração de workflows
- **SQLite**: Banco de dados para camada Gold
- **Python**: Linguagem principal do projeto

## 📝 Notas

- O pipeline é **idempotente**: pode ser executado múltiplas vezes sem duplicar dados
- Os dados são particionados por data para otimizar consultas
- O container Spark monta o workspace como volume para desenvolvimento facilitado

## 📊 Visualização dos Dados

Após a execução do pipeline, os dados processados na camada Gold ficam disponíveis no banco SQLite localizado em `gold/gold.db`.

### Conectando com DBeaver

Recomendamos o uso do **DBeaver** para visualizar e consultar os dados:

1. **Instalar DBeaver**: Baixe em https://dbeaver.io/download/

2. **Criar nova conexão**:
   - Tipo: SQLite
   - Caminho do banco: `gold/gold.db` (caminho absoluto do arquivo no seu projeto)

3. **Explorar os dados**: Após conectar, você poderá visualizar todas as tabelas e indicadores gerados pela camada Gold, executar queries SQL e exportar resultados.

