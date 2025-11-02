#!/bin/bash
#
# Script per resettare completamente il cluster Neo4j
# Usa questo script quando vuoi caricare un nuovo dump di Wikipedia
#

set -e

echo "========================================="
echo "Reset Cluster Neo4j"
echo "========================================="
echo ""

# Torna alla directory principale del progetto
cd "$(dirname "$0")/.."

# Controlla se Docker è in esecuzione
if ! docker info > /dev/null 2>&1; then
    echo "❌ Errore: Docker non è in esecuzione!"
    exit 1
fi

echo "⏹️  Fermando il cluster Neo4j..."
docker-compose down

echo ""
echo "🗑️  Eliminando i dati Neo4j..."

if [ -d "neo4j-data" ]; then
    # Opzione 1: Elimina solo i database mantenendo la struttura
    echo "   Eliminazione database e transazioni..."
    rm -rf neo4j-data/*/data/databases/* 2>/dev/null || true
    rm -rf neo4j-data/*/data/transactions/* 2>/dev/null || true
    rm -rf neo4j-data/*/data/cluster-state/* 2>/dev/null || true
    
    echo "✅ Dati eliminati (struttura mantenuta)"
else
    echo "⚠️  Directory neo4j-data non trovata"
fi

echo ""
echo "========================================="
echo "✅ Reset completato!"
echo "========================================="
echo ""
echo "Prossimi passi:"
echo "  1. Assicurati di avere i file .sql corretti in /data/"
echo "  2. cd src"
echo "  3. ./2_start_docker.sh"
echo "  4. python 3_load_graph_data.py"
echo ""
