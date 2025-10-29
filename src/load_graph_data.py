import sqlite3
import gzip
import re
import os
import hashlib
from neo4j import GraphDatabase
from tqdm import tqdm
import time

PAGELINKS_DUMP_FILE = os.path.join('data', 'itwiki-latest-pagelinks.sql.gz')
DB_FILE = 'page_map.db'
BATCH_SIZE = 50000
N_SERVERS = 4
MAX_LINES = None

NEO4J_SERVERS = {
    0: "bolt://localhost:7687",
    1: "bolt://localhost:7688",
    2: "bolt://localhost:7689",
    3: "bolt://localhost:7690",
}
NEO4J_USER = "neo4j"
NEO4J_PASS = "password"

#
# regex per cercare nelle righe INSERT
# la tupla (id_sorgente, 0, id_destinazione)
#
PAGELINKS_INSERT_REGEX = re.compile(
    r"\(([0-9]+),0,([0-9]+)\)"
)

#
# partizionamento: calcola su quale server Neo4j
# deve risiedere una pagina in base all'hash (MD5) del titolo
#
def get_server_id(page_title):
    hash_bytes = hashlib.md5(page_title.encode('utf-8')).digest()
    hash_int = int.from_bytes(hash_bytes, 'little')
    return hash_int % N_SERVERS

#
# apro una connessione (driver) per ognuno dei server
# e le ritorna tutte insieme
#
def create_drivers():
    drivers = {}
    print("Connessione ai server Neo4j...")
    for i in range(N_SERVERS):
        uri = NEO4J_SERVERS[i]
        drivers[i] = GraphDatabase.driver(uri, auth=(NEO4J_USER, NEO4J_PASS))
        try:
            drivers[i].verify_connectivity()
            print(f"Server {i} ({uri}) connesso.")
        except Exception as e:
            print(f"ERRORE: Impossibile connettersi al Server {i}. Verifica Docker e password.")
            print(e)
            return None
    return drivers

def close_drivers(drivers):
    print("Chiusura connessioni Neo4j...")
    for driver in drivers.values():
        driver.close()

#
# query Cypher per creare nodi e relazioni
# usa MERGE per evitare duplicati
#
# UNWIND permette di trasformare una lista in righe individuali
# MERGE  usa un nodo esistente, se non c'è lo crea
# con l'ultimo MERGE colleghiamo il nodo sorgente
# con quello destinazione
#
def add_links_batch(tx, links_batch):
    query = """
    UNWIND $links AS link
    MERGE (a:Page {title: link.source})
    MERGE (b:Page {title: link.target})
    MERGE (a)-[:LINKS_TO]->(b)
    """
    tx.run(query, links=links_batch)

#
# carica TUTTO il db SQLite in un dizionario in RAM
# per evitare query ripetute durante l'elaborazione
#
def preload_id_cache(conn_sqlite):
    print("Precaricamento cache ID->Titolo in memoria...")
    cursor = conn_sqlite.cursor()
    cursor.execute("SELECT page_id, page_title FROM page_map")
    
    cache = {}
    count = 0
    for page_id, page_title in cursor:
        cache[page_id] = page_title.replace('_', ' ')
        count += 1
        if count % 100000 == 0:
            print(f"  Caricati {count:,} mapping...")
    
    print(f"✓ Cache completa: {count:,} pagine in memoria")
    return cache

def process_pagelinks_dump(drivers):
    print(f"Connessione al database SQLite '{DB_FILE}'...")
    conn_sqlite = sqlite3.connect(DB_FILE)
    
    id_to_title_cache = preload_id_cache(conn_sqlite)
    
    def get_title_from_id(page_id):
        return id_to_title_cache.get(page_id)

    #
    # batches: un buffer per ogni server Neo4j
    # sessions: una sessione persistente per ogni server
    #
    batches = {i: [] for i in range(N_SERVERS)}
    sessions = {i: drivers[i].session() for i in range(N_SERVERS)}
    
    total_links_processed = 0
    start_time = time.time()
    
    print(f"Inizio elaborazione di {PAGELINKS_DUMP_FILE}...")
    if MAX_LINES:
        print(f"*** MODALITÀ TEST: Elaborando solo le prime {MAX_LINES} righe ***")
    
    with gzip.open(PAGELINKS_DUMP_FILE, 'rt', encoding='utf-8') as f:
        progress_bar = tqdm(f, total=MAX_LINES if MAX_LINES else 170_000_000, unit=' righe', desc="Elaboro 'pagelinks.sql'")
        
        lines_read = 0
        for line in progress_bar:
            #
            # limita il numero di righe (per test)
            #
            if MAX_LINES and lines_read >= MAX_LINES:
                break
            lines_read += 1
            
            #
            # skippo righe senza tuple
            #
            if '(' not in line:
                continue
            
            #
            # estrae tutte le tuple (source_id, 0, target_id)
            #
            matches = PAGELINKS_INSERT_REGEX.findall(line)
            
            for match in matches:
                try:
                    source_id = int(match[0])
                    target_id = int(match[1])
                except ValueError:
                    continue
                
                source_title = get_title_from_id(source_id)
                target_title = get_title_from_id(target_id)
                
                if source_title and target_title:
                    server_id = get_server_id(source_title)
                    batches[server_id].append({
                        "source": source_title, 
                        "target": target_title
                    })
                    total_links_processed += 1
                    
                    #
                    # quando il batch è pieno, lo scriviamo dentro neo4j
                    #
                    if len(batches[server_id]) >= BATCH_SIZE:
                        try:
                            sessions[server_id].execute_write(add_links_batch, batches[server_id])
                        except Exception as e:
                            print(f"Errore durante l'invio del batch al server {server_id}: {e}")
                        batches[server_id] = []
            
            if total_links_processed > 0 and total_links_processed % 50000 == 0:
                elapsed = time.time() - start_time
                lps = total_links_processed / elapsed
                progress_bar.set_description(f"Elaboro 'pagelinks.sql' ({lps:.0f} link/s, {len(id_to_title_cache):,} pagine)")

    #
    # invia i batch non completi a ciascun server
    #
    print("Invio dei batch rimanenti...")
    for server_id, batch in batches.items():
        if batch:
            try:
                sessions[server_id].execute_write(add_links_batch, batch)
                print(f"Inviato batch finale di {len(batch)} link al server {server_id}.")
            except Exception as e:
                print(f"Errore durante l'invio del batch finale al server {server_id}: {e}")

    print("Chiusura sessioni...")
    for session in sessions.values():
        session.close()
    conn_sqlite.close()
    
    elapsed = time.time() - start_time
    print("\n--- CARICAMENTO COMPLETATO ---")
    print(f"Link totali elaborati: {total_links_processed}")
    print(f"Tempo totale: {elapsed:.2f} secondi")

def main():
    if not os.path.exists(DB_FILE):
        print(f"ERRORE: File '{DB_FILE}' non trovato.")
        print("Esegui prima 'python3 src/build_id_title_map.py' per crearlo.")
        return
        
    drivers = create_drivers()
    if drivers:
        print("Creazione indici :Page(title) su tutti i server (può richiedere un minuto)...")
        for i, driver in drivers.items():
            with driver.session() as s:
                try:
                    #
                    # qui gli stiamo praticamente dicendo indicizzando
                    # il campo title, senza di questo la query MERGE dovrebbe
                    # ogni volta scorrere tutti i nodi
                    #
                    s.run("CREATE CONSTRAINT page_title_unique IF NOT EXISTS FOR (p:Page) REQUIRE p.title IS UNIQUE")
                    print(f"Indice creato sul server {i}.")
                except Exception as e:
                    print(f"Errore creazione indice su server {i}: {e}")
                    
        process_pagelinks_dump(drivers)
        close_drivers(drivers)

if __name__ == "__main__":
    main()
