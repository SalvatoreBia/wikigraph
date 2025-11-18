#!/bin/bash

# File di mappatura
MAP_FILE="../data/pagemap.csv"

echo "--- Inizio traduzione Batch ID -> Nomi ---"
echo "Mappa di riferimento: $MAP_FILE"

# Controllo esistenza file mappa
if [[ ! -f "$MAP_FILE" ]]; then
    echo "Errore: File $MAP_FILE mancante."
    exit 1
fi

# Ciclo su tutti i file che iniziano con sample_ seguito da un numero
# Esempio match: sample_0.csv, sample_1.csv, sample_10.csv
for EDGE_FILE in ../data/sample_[0-9]*.csv; do

    # Controllo se esistono file (nel caso il glob non trovi nulla)
    if [[ ! -e "$EDGE_FILE" ]]; then
        echo "Nessun file sample_*.csv trovato nella cartella corrente."
        break
    fi

    # Genera il nome del file di output
    # Sostituisce "sample_" con "sample_with_names_" nel nome del file
    OUTPUT_FILE="${EDGE_FILE/sample_/sample_with_names_}"

    echo "Elaborazione in corso: $EDGE_FILE  -->  $OUTPUT_FILE"

    # AWK Script
    awk -F, '
        # Fase 1: Caricamento della Mappa (../data/pagemap.csv)
        NR == FNR { 
            # Pulisce il titolo da eventuali apici singoli extra
            gsub(/\047/, "", $2); 
            map[$1] = $2; 
            next 
        }

        # Fase 2: Elaborazione del file corrente (sample_X.csv)
        {
            # Gestione Header
            if (FNR == 1) { 
                print "src_title,dest_title"; 
                next 
            }

            # Lookup: se l id esiste nella mappa usa il nome, altrimenti tieni l id
            s = ($1 in map) ? map[$1] : $1
            d = ($2 in map) ? map[$2] : $2

            print s "," d
        }
    ' "$MAP_FILE" "$EDGE_FILE" > "$OUTPUT_FILE"

done

echo "--- Operazione completata su tutti i file ---"