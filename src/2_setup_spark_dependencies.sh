#!/bin/bash

#
# Script per installare le dipendenze Python necessarie
# su tutti i container Spark (master + 4 worker)
#

echo "========================================="
echo "  Setup Dipendenze Spark Cluster"
echo "========================================="
echo ""

PACKAGES="pandas tqdm pyarrow"

# Installa sul master
echo "Installazione dipendenze su spark-master..."
docker exec -u root spark-master pip3 install -q $PACKAGES
if [ $? -eq 0 ]; then
    echo "✓ spark-master: OK"
else
    echo "✗ spark-master: ERRORE"
    exit 1
fi

# Installa sui 4 worker
for worker in spark-worker-1 spark-worker-2 spark-worker-3 spark-worker-4; do
    echo "Installazione dipendenze su $worker..."
    docker exec -u root $worker pip3 install -q $PACKAGES
    if [ $? -eq 0 ]; then
        echo "✓ $worker: OK"
    else
        echo "✗ $worker: ERRORE"
        exit 1
    fi
done

echo ""
echo "========================================="
echo "  Setup completato con successo!"
echo "========================================="
echo ""
echo "Dipendenze installate:"
echo "  - pandas"
echo "  - tqdm"
echo "  - pyarrow"
echo ""
echo "Ora puoi eseguire:"
echo "  bash src/3_run_spark_job.sh"
