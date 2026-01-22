
import argparse
import csv
import os
import re
import sys
from collections import defaultdict

DEFAULT_XML_FILE = '../data/itwiki-latest-pages-articles.xml'
OUTPUT_DIR = '../data/sample_content/'
BUFFER_SIZE = 500  
PROGRESS_INTERVAL = 1000  

try:
    from lxml import etree as ET
    HAS_LXML = True
except ImportError:
    import xml.etree.ElementTree as ET
    HAS_LXML = False

try:
    import mwparserfromhell
    HAS_MWPARSER = True
except ImportError:
    HAS_MWPARSER = False

RE_TEMPLATE = re.compile(r'\{\{[^}]+\}\}')
RE_FILE = re.compile(r'\[\[File:[^\]]+\]\]')
RE_IMAGE = re.compile(r'\[\[Immagine:[^\]]+\]\]')
RE_CATEGORY = re.compile(r'\[\[Categoria:[^\]]+\]\]')
RE_INTERNAL_LINK = re.compile(r'\[\[([^|\]]+\|)?([^\]]+)\]\]')
RE_EXTERNAL_LINK = re.compile(r'\[https?[^\]]+\]')
RE_BOLD_ITALIC = re.compile(r"'{2,}")
RE_REF = re.compile(r'<ref[^>]*>.*?</ref>', re.DOTALL)
RE_REF_SELF_CLOSING = re.compile(r'<ref[^/]*/>')
RE_HTML_TAGS = re.compile(r'<[^>]+>')
RE_HEADERS = re.compile(r'={2,}[^=]+={2,}')
RE_COMMENTS = re.compile(r'<!--.*?-->', re.DOTALL)
RE_NOWIKI = re.compile(r'<nowiki>.*?</nowiki>', re.DOTALL)
RE_WHITESPACE = re.compile(r'\s+')

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Estrae il contenuto delle pagine Wikipedia da un dump XML',
        usage='%(prog)s <sample_number> [opzioni]'
    )
    parser.add_argument('sample_number', type=int,
                        help='Numero del sample (es: 0 per sample_with_names_0.csv)')
    parser.add_argument('--xml-file', default=DEFAULT_XML_FILE, 
                        help=f'File XML dump di Wikipedia (default: {DEFAULT_XML_FILE})')
    parser.add_argument('--fast-clean', action='store_true',
                        help='Usa pulizia veloce con regex invece di mwparserfromhell')
    parser.add_argument('--buffer-size', type=int, default=BUFFER_SIZE,
                        help=f'Dimensione buffer scrittura (default: {BUFFER_SIZE})')
    return parser.parse_args()

def clean_content_simple(raw_content):
    if not raw_content:
        return ''
    
    text = raw_content
    
    text = RE_COMMENTS.sub('', text)  
    text = RE_NOWIKI.sub('', text)  
    text = RE_TEMPLATE.sub('', text)  
    text = RE_FILE.sub('', text)  
    text = RE_IMAGE.sub('', text)  
    text = RE_CATEGORY.sub('', text)  
    text = RE_INTERNAL_LINK.sub(r'\2', text)  
    text = RE_EXTERNAL_LINK.sub('', text)  
    text = RE_BOLD_ITALIC.sub('', text)  
    text = RE_REF.sub('', text)  
    text = RE_REF_SELF_CLOSING.sub('', text)  
    text = RE_HTML_TAGS.sub('', text)  
    text = RE_HEADERS.sub('', text)  
    
    return RE_WHITESPACE.sub(' ', text).strip()

def clean_content(raw_content, use_mwparser=False):
    if use_mwparser and HAS_MWPARSER:
        try:
            wikicode = mwparserfromhell.parse(raw_content)
            return wikicode.strip_code().strip()
        except Exception:
            return clean_content_simple(raw_content)
    else:
        return clean_content_simple(raw_content)

def load_titles_from_csv(csv_files):
    title_map = {}
    print('[*] Lettura dei file CSV di input...')
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for csv_path in csv_files:
        base_name = os.path.basename(csv_path)
        output_path = os.path.join(OUTPUT_DIR, os.path.splitext(base_name)[0] + '_content.csv')
        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as f_out:
                writer = csv.writer(f_out)
                writer.writerow(['page_id', 'page_title', 'content'])
            with open(csv_path, 'r', encoding='utf-8') as f_in:
                reader = csv.reader(f_in)
                next(reader, None)  
                for row in reader:
                    if not row:
                        continue
                    for raw_title in row:
                        if raw_title:
                            title_clean = raw_title.replace('_', ' ').strip()
                            if title_clean not in title_map:
                                title_map[title_clean] = set()
                            title_map[title_clean].add(output_path)
        except Exception as e:
            print(f'Errore nel file {csv_path}: {e}')
    return title_map

def extract_page_content(xml_file, title_map, buffer_size=BUFFER_SIZE, use_mwparser=False):
    print(f'[*] Titoli unici da cercare: {len(title_map)}')
    print(f'[*] Buffer size: {buffer_size} righe')
    print(f'[*] Pulizia testo: {"mwparserfromhell (lento)" if use_mwparser else "regex veloce"}')
    print(f'[*] Scansione XML in corso...')
    
    file_buffers = defaultdict(list)
    found_count = 0
    
    if HAS_LXML:
        context = ET.iterparse(xml_file, events=('end',), tag='{*}page', huge_tree=True)
    else:
        context = ET.iterparse(xml_file, events=('end',))
    
    for event, elem in context:
        if elem.tag.endswith('page'):
            title_elem = elem.find('.//{*}title')
            id_elem = elem.find('.//{*}id')
            
            if title_elem is not None and id_elem is not None:
                current_title = title_elem.text
                current_id = id_elem.text
                
                if current_title and current_id:
                    clean_title = current_title.strip()
                    
                    if clean_title in title_map:
                        content = ''
                        
                        text_elem = elem.find('.//{*}revision/{*}text')
                        
                        if text_elem is not None and text_elem.text:
                            raw_content = text_elem.text
                            content = clean_content(raw_content, use_mwparser)
                        
                        target_files = title_map[clean_title]
                        row = [current_id, clean_title, content]
                        
                        for out_file in target_files:
                            file_buffers[out_file].append(row)
                        
                        found_count += 1
                        
                        if found_count % buffer_size == 0:
                            flush_buffers(file_buffers)
                            sys.stdout.write(f'\r[*] Pagine estratte: {found_count}')
                            sys.stdout.flush()
            
            elem.clear()
            if HAS_LXML:
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
    
    flush_buffers(file_buffers)
    
    print(f'\n[OK] Finito! Pagine estratte totali: {found_count}')

def flush_buffers(file_buffers):
    for out_file, rows in file_buffers.items():
        if rows:
            with open(out_file, 'a', newline='', encoding='utf-8') as f_append:
                writer = csv.writer(f_append)
                writer.writerows(rows)
    
    file_buffers.clear()

def main():
    args = parse_arguments()
    
    csv_file = f'../data/sample_with_names/sample_with_names_{args.sample_number}.csv'
    
    if not os.path.exists(args.xml_file):
        print(f'[ERRORE] File XML non trovato: {args.xml_file}')
        sys.exit(1)
    if not os.path.exists(csv_file):
        print(f'[ERRORE] File CSV non trovato: {csv_file}')
        sys.exit(1)
    title_map = load_titles_from_csv([csv_file])
    if not title_map:
        print('[ERRORE] Nessun titolo trovato nel file CSV')
        sys.exit(1)
    use_mwparser = not args.fast_clean and HAS_MWPARSER
    extract_page_content(args.xml_file, title_map, args.buffer_size, use_mwparser)

if __name__ == '__main__':
    main()
