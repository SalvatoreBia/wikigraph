#!/bin/bash
#
# Script per eseguire il job Spark di caricamento grafo e community detection
#

echo "========================================="
echo "  Avvio Job Spark: Graph Loading"
echo "========================================="
echo ""

# Verifica che il DB esista
if [ ! -f "src/page_map.db" ]; then
    echo "ERRORE: File 'src/page_map.db' non trovato!"
    echo "Esegui prima: python3 src/1_build_id_title_map.py"
    exit 1
fi

# Verifica che il dump SQL esista
if [ ! -f "data/itwiki-latest-pagelinks.sql" ]; then
    echo "ERRORE: File 'data/itwiki-latest-pagelinks.sql' non trovato!"
    echo "Assicurati di aver scompattato il dump SQL nella cartella 'data/'"
    exit 1
fi

# Verifica che i container siano attivi
echo "Verifica container Spark..."
if ! docker ps | grep -q spark-master; then
    echo "ERRORE: Container spark-master non attivo!"
    echo "Esegui: docker compose up -d"
    exit 1
fi

# Conta i worker attivi
WORKERS=$(docker ps | grep spark-worker | wc -l)
echo "✓ Spark Master: attivo"
echo "✓ Spark Workers: $WORKERS attivi"
echo ""

# Esegui il job
echo "Avvio job Spark sul cluster..."
echo "   (Puoi monitorare il progresso su http://localhost:8080)"
echo ""

# Configurazione corretta per Docker networking
# - spark.driver.host: usa il nome del container invece dell'hostname interno
# - spark.driver.bindAddress: permette connessioni da tutti gli indirizzi
# - spark.driver.port: porta fissa per il driver (evita porte random)
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 \
    --conf "spark.jars.ivy=/tmp/.ivy2" \
    --conf "spark.driver.memory=2g" \
    --conf "spark.executor.memory=1g" \
    --conf "spark.executor.cores=2" \
    --conf "spark.cores.max=8" \
    --conf "spark.driver.host=spark-master" \
    --conf "spark.driver.bindAddress=0.0.0.0" \
    --conf "spark.driver.port=7078" \
    --conf "spark.blockManager.port=7079" \
    /opt/spark-apps/3_load_graph_spark.py

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "========================================="
    echo " Job completato con successo!"
    echo "========================================="
    echo ""
    echo "Risultati salvati in: ./spark_output/"
    echo "  - community_map.parquet"
    echo "  - graph_vertices.parquet"
    echo "  - graph_edges.parquet"
else
    echo "========================================="
    echo " Job terminato con errori"
    echo "========================================="
    exit $EXIT_CODE
fi
