import sqlite3
import gzip
import re
import os
from tqdm import tqdm

PAGE_DUMP_FILE = os.path.join('data', 'itwiki-latest-page.sql.gz')
DB_FILE = 'page_map.db'
BATCH_SIZE = 50000

PAGE_INSERT_REGEX = re.compile(
    r"\(([0-9]+),([0-9]+),'([^']+)'"
)

def create_db_schema(cursor):
    cursor.execute("DROP TABLE IF EXISTS page_map")
    cursor.execute("""
        CREATE TABLE page_map (
            page_id INTEGER PRIMARY KEY,
            page_title TEXT
        )
    """)
    print("Database e tabella 'page_map' creati.")

def process_page_dump():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    create_db_schema(cursor)
    
    batch = []
    total_pages = 0
    
    print(f"Inizio elaborazione di {PAGE_DUMP_FILE}...")
    
    with gzip.open(PAGE_DUMP_FILE, 'rt', encoding='utf-8') as f:
        progress_bar = tqdm(f, total=110_000_000, unit=' righe', desc="Elaboro 'page.sql'")
        
        for line in progress_bar:
            if not line.startswith('INSERT INTO `page`'):
                continue
            
            matches = PAGE_INSERT_REGEX.findall(line)
            
            for match in matches:
                page_id = int(match[0])
                namespace = int(match[1])
                page_title = match[2]
                
                if namespace == 0:
                    batch.append((page_id, page_title))
                    total_pages += 1
                
                if len(batch) >= BATCH_SIZE:
                    cursor.executemany("INSERT INTO page_map (page_id, page_title) VALUES (?, ?)", batch)
                    conn.commit()
                    batch = []
                    
        if batch:
            cursor.executemany("INSERT INTO page_map (page_id, page_title) VALUES (?, ?)", batch)
            conn.commit()

    print(f"\nElaborazione completata. Pagine totali (namespace 0) inserite: {total_pages}")
    
    print("Creazione indice su page_id...")
    cursor.execute("CREATE UNIQUE INDEX idx_page_id ON page_map (page_id)")
    conn.commit()
    print("Indice creato. Database pronto.")
    
    conn.close()

if __name__ == "__main__":
    process_page_dump()