#!/usr/bin/env python3
"""
Script per estrarre il contenuto delle pagine Wikipedia da un dump XML
e salvarlo in file CSV basati su liste di titoli fornite.
Versione ottimizzata con buffering e supporto multiprocessing.
"""

import argparse
import csv
import os
import re
import sys
from collections import defaultdict

# Costanti per i file di default
DEFAULT_XML_FILE = '../data/itwiki-latest-pages-articles.xml'
DEFAULT_CSV_FILE = '../data/sample_with_names/sample_with_names_0.csv'
OUTPUT_DIR = '../data/'
BUFFER_SIZE = 1000  # Numero di righe da bufferizzare prima della scrittura

# Prova a usare lxml (molto più veloce di ElementTree)
try:
    from lxml import etree as ET
    HAS_LXML = True
    print('[*] Libreria lxml trovata. Parsing XML veloce.')
except ImportError:
    import xml.etree.ElementTree as ET
    HAS_LXML = False
    print('[!] AVVISO: lxml non installata. Usare: pip install lxml (5-10x più veloce)')

# Tenta di importare il pulitore (opzionale e lento)
try:
    import mwparserfromhell
    HAS_MWPARSER = True
    print('[*] Libreria mwparserfromhell trovata. Il testo verrà pulito (rallenta molto).')
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
    parser.add_argument('--no-clean', action='store_true',
                        help='Disabilita pulizia testo (più veloce)')
    parser.add_argument('--buffer-size', type=int, default=BUFFER_SIZE,
                        help=f'Dimensione buffer scrittura (default: {BUFFER_SIZE})')
    
    return parser.parse_args()


def clean_content_simple(raw_content):
    """Pulizia veloce con regex (molto più veloce di mwparserfromhell)."""
    if not raw_content:
        return ''
    
    # Rimuovi template e markup comuni
    text = re.sub(r'\{\{[^}]+\}\}', '', raw_content)  # Template
    text = re.sub(r'\[\[File:[^\]]+\]\]', '', text)  # File
    text = re.sub(r'\[\[Immagine:[^\]]+\]\]', '', text)  # Immagini
    text = re.sub(r'\[\[Categoria:[^\]]+\]\]', '', text)  # Categorie
    text = re.sub(r'\[\[([^|\]]+\|)?([^\]]+)\]\]', r'\2', text)  # Link interni
    text = re.sub(r'\[http[^\]]+\]', '', text)  # Link esterni
    text = re.sub(r"'{2,}", '', text)  # Grassetto/corsivo
    text = re.sub(r'<ref[^>]*>.*?</ref>', '', text, flags=re.DOTALL)  # Ref
    text = re.sub(r'<[^>]+>', '', text)  # Altri tag HTML
    text = re.sub(r'={2,}[^=]+={2,}', '', text)  # Intestazioni
    
    return ' '.join(text.split())  # Normalizza spazi


def clean_content(raw_content, use_mwparser=False):
    """Pulisce il contenuto Wikitext."""
    if use_mwparser and HAS_MWPARSER:
        try:
            wikicode = mwparserfromhell.parse(raw_content)
            return wikicode.strip_code().strip()
        except Exception:
            return clean_content_simple(raw_content)
    else:
        return clean_content_simple(raw_content)


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


def extract_page_content(xml_file, title_map, buffer_size=BUFFER_SIZE, use_mwparser=False):
    """
    Estrae il contenuto delle pagine dal dump XML con buffering.
    
    Args:
        xml_file: Path al file XML dump
        title_map: Dizionario con i titoli da cercare
        buffer_size: Numero di righe da bufferizzare prima della scrittura
        use_mwparser: Se usare mwparserfromhell per la pulizia (lento)
    """
    print(f'[*] Titoli unici da cercare: {len(title_map)}')
    print(f'[*] Buffer size: {buffer_size} righe')
    print(f'[*] Pulizia testo: {"mwparserfromhell (lento)" if use_mwparser else "regex veloce"}')
    print(f'[*] Scansione XML in corso...')
    
    # Buffer per ogni file di output
    file_buffers = defaultdict(list)
    found_count = 0
    
    # Usa iterparse con recupero rapido
    if HAS_LXML:
        context = ET.iterparse(xml_file, events=('end',), tag='{*}page', huge_tree=True)
    else:
        context = ET.iterparse(xml_file, events=('end',))
    
    for event, elem in context:
        if elem.tag.endswith('page'):
            # Estrazione ottimizzata con find
            title_elem = elem.find('.//{*}title')
            id_elem = elem.find('.//{*}id')
            
            if title_elem is not None and id_elem is not None:
                current_title = title_elem.text
                current_id = id_elem.text
                
                if current_title and current_id:
                    clean_title = current_title.strip()
                    
                    # Se il titolo è nella nostra lista
                    if clean_title in title_map:
                        content = ''
                        
                        # Estrazione ottimizzata del testo
                        text_elem = elem.find('.//{*}revision/{*}text')
                        
                        if text_elem is not None and text_elem.text:
                            raw_content = text_elem.text
                            content = clean_content(raw_content, use_mwparser)
                        
                        # Aggiungi al buffer per ogni file target
                        target_files = title_map[clean_title]
                        row = [current_id, clean_title, content]
                        
                        for out_file in target_files:
                            file_buffers[out_file].append(row)
                        
                        found_count += 1
                        
                        # Flush buffer se necessario
                        if found_count % buffer_size == 0:
                            flush_buffers(file_buffers)
                            sys.stdout.write(f'\r[*] Pagine estratte: {found_count}')
                            sys.stdout.flush()
            
            # Pulizia memoria
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
    
    # Flush finale dei buffer
    flush_buffers(file_buffers)
    
    print(f'\n[OK] Finito! Pagine estratte totali: {found_count}')


def flush_buffers(file_buffers):
    """Svuota tutti i buffer scrivendo sui file."""
    for out_file, rows in file_buffers.items():
        if rows:
            with open(out_file, 'a', newline='', encoding='utf-8') as f_append:
                writer = csv.writer(f_append)
                writer.writerows(rows)
    
    # Pulisci i buffer
    file_buffers.clear()


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
    use_mwparser = not args.no_clean and HAS_MWPARSER
    extract_page_content(args.xml_file, title_map, args.buffer_size, use_mwparser)


if __name__ == '__main__':
    main()
