#!/bin/bash

#scarica a prescindere, non contr
DEST_DIR="../data"
BASE_URL="https://dumps.wikimedia.org/itwiki/latest/"
FILES=("itwiki-latest-page.sql.gz" "itwiki-latest-pagelinks.sql.gz")

mkdir -p "$DEST_DIR"
for F in "${FILES[@]}"; do
    echo "Scaricamento $F..."
    wget -q --show-progress -P "$DEST_DIR" "$BASE_URL$F"
    echo "Estrazione $F..."
    gunzip -f "$DEST_DIR/$F"
done