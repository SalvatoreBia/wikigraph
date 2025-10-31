import sqlite3
import re
import os
from tqdm import tqdm

PAGE_DUMP_FILE = os.path.join('../data', 'itwiki-latest-page.sql')
DB_FILE = 'page_map.db'
BATCH_SIZE = 50000

#
# regex per cercare nelle righe INSERT
# la tupla (id, namespace, titolo)
#
PAGE_INSERT_REGEX = re.compile(
    r"\(([0-9]+),([0-9]+),'([^']+)'"
)

# 
# page_map.db contiene le coppie (id_pagina, titolo_pagina)
#
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
    
    #
    # batch è un buffer dove carico le righe processate
    # prime di inserirle nel db
    #
    batch = []
    total_pages = 0
    
    print(f"Inizio elaborazione di {PAGE_DUMP_FILE}...")
    
    with open(PAGE_DUMP_FILE, 'r', encoding='utf-8') as f:
        progress_bar = tqdm(f, total=110_000_000, unit=' righe', desc="Elaboro 'page.sql'")
        
        #
        # skippo tutte le righe che non sono insert
        #
        for line in progress_bar:
            if not line.startswith('INSERT INTO `page`'):
                continue
            
            matches = PAGE_INSERT_REGEX.findall(line)
            
            for match in matches:
                page_id = int(match[0])
                namespace = int(match[1])
                page_title = match[2]
                
                #
                # namespace = 0 -> sto filtrando solo per gli articoli
                #
                if namespace == 0:
                    batch.append((page_id, page_title))
                    total_pages += 1
                
                #
                # carico tutte le righe nel buffer nel db
                #
                if len(batch) >= BATCH_SIZE:
                    cursor.executemany("INSERT INTO page_map (page_id, page_title) VALUES (?, ?)", batch)
                    conn.commit()
                    batch = []
        
        #
        # ultimo batch da caricare
        #
        if batch:
            cursor.executemany("INSERT INTO page_map (page_id, page_title) VALUES (?, ?)", batch)
            conn.commit()

    print(f"\nElaborazione completata. Pagine totali (namespace 0) inserite: {total_pages}")
    
    #
    # metto un indice su page_id per velocizzare le query
    #
    print("Creazione indice su page_id...")
    cursor.execute("CREATE UNIQUE INDEX idx_page_id ON page_map (page_id)")
    conn.commit()
    print("Indice creato. Database pronto.")
    
    conn.close()

if __name__ == "__main__":
    process_page_dump()