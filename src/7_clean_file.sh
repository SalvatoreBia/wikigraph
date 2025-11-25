#!/bin/bash

# Controlla gli argomenti
if [ "$#" -lt 2 ]; then
    echo "Uso: $0 <file_xml_dump> <file_csv_1> [file_csv_2 ...]"
    echo "Esempio: ./clean_file.sh itwiki-latest-pages-articles.xml links1.csv links2.csv"
    exit 1
fi

XML_FILE="$1"
shift # Rimuove il primo argomento (xml), lasciando solo i CSV

# Eseguiamo un blocco Python incorporato
python3 -c "
import sys
import csv
import xml.etree.ElementTree as ET
import os

# Tenta di importare il pulitore
try:
    import mwparserfromhell
    HAS_MWPARSER = True
    print('[*] Libreria mwparserfromhell trovata. Il testo verrà pulito.')
except ImportError:
    HAS_MWPARSER = False
    print('[!] AVVISO: mwparserfromhell non installata. Il testo rimarrà grezzo (Wikitext).')
    print('[!] Per pulirlo: pip install mwparserfromhell')

# Configurazione
xml_file = sys.argv[1]
input_csv_files = sys.argv[2:]

# Mappa: Titolo Pagina (Normalizzato) -> Lista di file CSV di destinazione
title_map = {}

print(f'[*] Lettura dei file CSV di input...')

# 1. Leggiamo tutti i CSV
for csv_path in input_csv_files:
    base_name = os.path.basename(csv_path)
    output_path = os.path.splitext(base_name)[0] + '_content.csv'
    
    try:
        # Crea/Pulisce il file di output
        with open(output_path, 'w', newline='', encoding='utf-8') as f_out:
            writer = csv.writer(f_out)
            writer.writerow(['page_id', 'page_title', 'content'])
            
        with open(csv_path, 'r', encoding='utf-8') as f_in:
            reader = csv.reader(f_in)
            next(reader, None) # Salta header
            
            for row in reader:
                if not row: continue
                for raw_title in row:
                    if raw_title:
                        title_clean = raw_title.replace('_', ' ').strip()
                        if title_clean not in title_map:
                            title_map[title_clean] = set()
                        title_map[title_clean].add(output_path)
    except Exception as e:
        print(f'Errore file {csv_path}: {e}')

print(f'[*] Titoli unici da cercare: {len(title_map)}')
print(f'[*] Scansione XML in corso...')

# 2. Iteriamo sul file XML
context = ET.iterparse(xml_file, events=('end',))
found_count = 0
count = 0

for event, elem in context:
    if elem.tag.endswith('page'):
        current_title = None
        current_id = None
        
        # Estrai titolo e ID
        for child in elem:
            if child.tag.endswith('title'):
                current_title = child.text
            elif child.tag.endswith('id'):
                current_id = child.text
        
        if current_title and current_id:
            clean_title = current_title.strip()
            
            if clean_title in title_map:
                content = ''
                revision = None
                
                # Cerca revision e text
                for child in elem:
                    if child.tag.endswith('revision'):
                        revision = child
                        break
                
                if revision is not None:
                    text_node = None
                    for child in revision:
                        if child.tag.endswith('text'):
                            text_node = child
                            break
                    
                    if text_node is not None and text_node.text:
                        raw_content = text_node.text
                        
                        # === FASE DI PULIZIA ===
                        if HAS_MWPARSER:
                            try:
                                # Parsa il wikitext e rimuove il markup
                                wikicode = mwparserfromhell.parse(raw_content)
                                # strip_code() toglie tutto il markup. 
                                content = wikicode.strip_code().strip()
                            except Exception:
                                content = raw_content # Fallback se fallisce
                        else:
                            content = raw_content
                
                # Scrivi su file
                target_files = title_map[clean_title]
                for out_file in target_files:
                    with open(out_file, 'a', newline='', encoding='utf-8') as f_append:
                        writer = csv.writer(f_append)
                        writer.writerow([current_id, clean_title, content])
                
                found_count += 1
                if found_count % 100 == 0:
                    sys.stdout.write(f'\r[*] Pagine estratte: {found_count}')
                    sys.stdout.flush()

        # PULIZIA MEMORIA
        elem.clear()
        
    count += 1

print(f'\n[OK] Finito! Pagine estratte totali: {found_count}')
" "$XML_FILE" "$@"