#!/usr/bin/env python3
"""
Script per estrarre il contenuto delle pagine Wikipedia da un dump XML
e salvarlo in file CSV basati su liste di titoli fornite.
"""

import argparse
import csv
import os
import sys
import xml.etree.ElementTree as ET

# Costanti per i file di default
DEFAULT_XML_FILE = '../data/itwiki-latest-pages-articles.xml'
DEFAULT_CSV_FILE = '../data/sample_with_names_0.csv'
OUTPUT_DIR = '../data/'

# Tenta di importare il pulitore
try:
    import mwparserfromhell
    HAS_MWPARSER = True
    print('[*] Libreria mwparserfromhell trovata. Il testo verrà pulito.')
except ImportError:
    HAS_MWPARSER = False
    print('[!] AVVISO: mwparserfromhell non installata. Il testo rimarrà grezzo (Wikitext).')
    print('[!] Per pulirlo: pip install mwparserfromhell')


def parse_arguments():
    """Parsing degli argomenti da linea di comando."""
    parser = argparse.ArgumentParser(
        description='Estrae il contenuto delle pagine Wikipedia da un dump XML',
        usage='%(prog)s [file_xml_dump] [file_csv_1 ...]'
    )
    parser.add_argument('xml_file', nargs='?', default=DEFAULT_XML_FILE, 
                        help=f'File XML dump di Wikipedia (default: {DEFAULT_XML_FILE})')
    parser.add_argument('csv_files', nargs='*', default=[DEFAULT_CSV_FILE], 
                        help=f'File CSV con i titoli delle pagine (default: {DEFAULT_CSV_FILE})')
    
    return parser.parse_args()


def clean_content(raw_content):
    """Pulisce il contenuto Wikitext se possibile."""
    if HAS_MWPARSER:
        try:
            wikicode = mwparserfromhell.parse(raw_content)
            return wikicode.strip_code().strip()
        except Exception:
            return raw_content
    else:
        return raw_content


def load_titles_from_csv(csv_files):
    """
    Carica i titoli da tutti i file CSV e crea una mappa.
    
    Returns:
        dict: Mappa {titolo_normalizzato: set(file_output)}
    """
    title_map = {}
    
    print('[*] Lettura dei file CSV di input...')
    
    for csv_path in csv_files:
        base_name = os.path.basename(csv_path)
        output_path = os.path.join(OUTPUT_DIR, os.path.splitext(base_name)[0] + '_content.csv')
        
        try:
            # Crea/Pulisce il file di output con l'header
            with open(output_path, 'w', newline='', encoding='utf-8') as f_out:
                writer = csv.writer(f_out)
                writer.writerow(['page_id', 'page_title', 'content'])
            
            # Legge i titoli dal CSV di input
            with open(csv_path, 'r', encoding='utf-8') as f_in:
                reader = csv.reader(f_in)
                next(reader, None)  # Salta header
                
                for row in reader:
                    if not row:
                        continue
                    for raw_title in row:
                        if raw_title:
                            # Normalizza il titolo (sostituisce _ con spazi)
                            title_clean = raw_title.replace('_', ' ').strip()
                            if title_clean not in title_map:
                                title_map[title_clean] = set()
                            title_map[title_clean].add(output_path)
        except Exception as e:
            print(f'Errore nel file {csv_path}: {e}')
    
    return title_map


def extract_page_content(xml_file, title_map):
    """
    Estrae il contenuto delle pagine dal dump XML.
    
    Args:
        xml_file: Path al file XML dump
        title_map: Dizionario con i titoli da cercare
    """
    print(f'[*] Titoli unici da cercare: {len(title_map)}')
    print(f'[*] Scansione XML in corso...')
    
    context = ET.iterparse(xml_file, events=('end',))
    found_count = 0
    
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
                
                # Se il titolo è nella nostra lista
                if clean_title in title_map:
                    content = ''
                    revision = None
                    
                    # Cerca il nodo revision
                    for child in elem:
                        if child.tag.endswith('revision'):
                            revision = child
                            break
                    
                    # Estrai il testo dalla revision
                    if revision is not None:
                        text_node = None
                        for child in revision:
                            if child.tag.endswith('text'):
                                text_node = child
                                break
                        
                        if text_node is not None and text_node.text:
                            raw_content = text_node.text
                            content = clean_content(raw_content)
                    
                    # Scrivi su tutti i file di output associati
                    target_files = title_map[clean_title]
                    for out_file in target_files:
                        with open(out_file, 'a', newline='', encoding='utf-8') as f_append:
                            writer = csv.writer(f_append)
                            writer.writerow([current_id, clean_title, content])
                    
                    found_count += 1
                    if found_count % 100 == 0:
                        sys.stdout.write(f'\r[*] Pagine estratte: {found_count}')
                        sys.stdout.flush()
            
            # Pulizia memoria per liberare risorse
            elem.clear()
    
    print(f'\n[OK] Finito! Pagine estratte totali: {found_count}')


def main():
    """Funzione principale."""
    args = parse_arguments()
    
    # Verifica che il file XML esista
    if not os.path.exists(args.xml_file):
        print(f'[ERRORE] File XML non trovato: {args.xml_file}')
        sys.exit(1)
    
    # Verifica che i file CSV esistano
    for csv_file in args.csv_files:
        if not os.path.exists(csv_file):
            print(f'[ERRORE] File CSV non trovato: {csv_file}')
            sys.exit(1)
    
    # Carica i titoli dai CSV
    title_map = load_titles_from_csv(args.csv_files)
    
    if not title_map:
        print('[ERRORE] Nessun titolo trovato nei file CSV')
        sys.exit(1)
    
    # Estrai il contenuto dal dump XML
    extract_page_content(args.xml_file, title_map)


if __name__ == '__main__':
    main()
