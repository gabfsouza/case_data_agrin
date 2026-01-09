#!/bin/bash
# Script de inicialização do MinIO
# Cria o bucket datalake e as pastas bronze, silver e gold
# Script idempotente: pode ser executado múltiplas vezes sem problemas

echo "=========================================="
echo "Inicializando MinIO - Criando estrutura"
echo "=========================================="

# Configurações
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_ACCESS_KEY="${MINIO_ROOT_USER:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_ROOT_PASSWORD:-minioadmin123}"
BUCKET_NAME="datalake"
MAX_RETRIES=30
RETRY_DELAY=2

# Função para aguardar o MinIO estar pronto
wait_for_minio() {
    echo "Aguardando MinIO estar disponivel..."
    local retries=0
    while [ $retries -lt $MAX_RETRIES ]; do
        if mc alias set local ${MINIO_ENDPOINT} ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY} > /dev/null 2>&1; then
            if mc admin info local > /dev/null 2>&1; then
                echo "MinIO esta disponivel!"
                return 0
            fi
        fi
        retries=$((retries + 1))
        echo "Tentativa $retries/$MAX_RETRIES - Aguardando MinIO..."
        sleep $RETRY_DELAY
    done
    echo "ERRO: MinIO nao esta disponivel apos $MAX_RETRIES tentativas"
    return 1
}

# Configura o alias do MinIO Client
setup_mc() {
    echo "Configurando MinIO Client..."
    mc alias set local ${MINIO_ENDPOINT} ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY}
    echo "MinIO Client configurado com sucesso!"
}

# Cria o bucket se não existir
create_bucket() {
    echo "Verificando bucket '$BUCKET_NAME'..."
    if mc ls local/${BUCKET_NAME} > /dev/null 2>&1; then
        echo "Bucket '$BUCKET_NAME' ja existe, pulando criacao..."
        return 0
    else
        echo "Criando bucket '$BUCKET_NAME'..."
        if mc mb local/${BUCKET_NAME} 2>/dev/null; then
            echo "Bucket '$BUCKET_NAME' criado com sucesso!"
        else
            echo "Aviso: Nao foi possivel criar o bucket (pode ja existir)"
        fi
    fi
}

# Cria as pastas (prefixos) dentro do bucket
create_folders() {
    echo "Criando pastas no bucket '$BUCKET_NAME'..."
    
    # Função auxiliar para criar pasta se não existir
    create_folder_if_not_exists() {
        local folder_name=$1
        local folder_path="${BUCKET_NAME}/${folder_name}"
        local marker_file="${folder_path}/.gitkeep"
        
        # Verifica se o arquivo marcador já existe
        if mc stat local/${marker_file} > /dev/null 2>&1; then
            echo "  Pasta '$folder_name' ja existe (marcador encontrado), pulando..."
            return 0
        fi
        
        # Cria o arquivo marcador usando printf com pipe
        # No MinIO/S3, pastas são apenas prefixos, então criamos um objeto marcador vazio
        if printf "" | mc pipe local/${marker_file} 2>/dev/null; then
            # Aguarda um momento para garantir que foi criado
            sleep 0.5
            # Verifica se foi criado com sucesso
            if mc stat local/${marker_file} > /dev/null 2>&1; then
                echo "  Pasta '$folder_name' criada com sucesso!"
                return 0
            fi
        fi
        
        # Se ainda não existe, tenta método alternativo: criar arquivo temporário
        local temp_file="/tmp/marker_${folder_name}_$$"
        if printf "" > "${temp_file}" 2>/dev/null; then
            if mc cp "${temp_file}" local/${marker_file} 2>/dev/null; then
                rm -f "${temp_file}" 2>/dev/null
                if mc stat local/${marker_file} > /dev/null 2>&1; then
                    echo "  Pasta '$folder_name' criada com sucesso!"
                    return 0
                fi
            fi
            rm -f "${temp_file}" 2>/dev/null
        fi
        
        echo "  Erro: Nao foi possivel criar a pasta '$folder_name'"
        return 1
    }
    
    # Cria cada pasta se não existir
    create_folder_if_not_exists "bronze"
    create_folder_if_not_exists "silver"
    create_folder_if_not_exists "gold"
    
    echo "Verificacao de pastas concluida!"
}

# Lista a estrutura criada
list_structure() {
    echo ""
    echo "=========================================="
    echo "Estrutura criada no MinIO:"
    echo "=========================================="
    mc ls -r local/${BUCKET_NAME}/
    echo "=========================================="
}

# Executa o processo de inicialização
main() {
    wait_for_minio
    setup_mc
    create_bucket
    create_folders
    list_structure
    echo ""
    echo "Inicializacao do MinIO concluida com sucesso!"
}

# Executa
main


