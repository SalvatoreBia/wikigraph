#!/bin/bash

MAP_FILE="../data/pagemap.csv"

echo "--- Inizio traduzione Batch ID -> Nomi ---"
echo "Mappa di riferimento: $MAP_FILE"

if [[ ! -f "$MAP_FILE" ]]; then
    echo "Errore: File $MAP_FILE mancante."
    exit 1
fi

mkdir -p ../data/sample_with_names

for EDGE_FILE in ../data/sample/sample_[0-9]*.csv; do

    if [[ ! -e "$EDGE_FILE" ]]; then
        echo "Nessun file sample_*.csv trovato nella cartella sample/."
        break
    fi

    BASE_NAME=$(basename "$EDGE_FILE")
    NUM=$(echo "$BASE_NAME" | sed 's/sample_\([0-9]*\)\.csv/\1/')
    OUTPUT_FILE="../data/sample_with_names/sample_with_names_${NUM}.csv"

    echo "Elaborazione in corso: $EDGE_FILE  -->  $OUTPUT_FILE"

    awk -F, '
        NR == FNR { 
            gsub(/\047/, "", $2); 
            map[$1] = $2; 
            next 
        }

        {
            if (FNR == 1) { 
                print "src_title,dest_title"; 
                next 
            }

            s = ($1 in map) ? map[$1] : $1
            d = ($2 in map) ? map[$2] : $2

            print s "," d
        }
    ' "$MAP_FILE" "$EDGE_FILE" > "$OUTPUT_FILE"

done

echo "--- Operazione completata su tutti i file ---"