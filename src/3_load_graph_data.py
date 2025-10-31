import sqlite3
import re
import os
import hashlib
from neo4j import GraphDatabase
from tqdm import tqdm
import time

# Percorsi assoluti basati sulla directory dello script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
PAGELINKS_DUMP_FILE = os.path.join(PROJECT_DIR, 'data', 'itwiki-latest-pagelinks.sql')
DB_FILE = os.path.join(SCRIPT_DIR, 'page_map.db')
BATCH_SIZE = 10_000  # Ridotto per invii più frequenti
N_SERVERS = 4
MAX_LINKS = 100_000  # Limita il numero di LINK (non righe) per testing

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
    max_retries = 30  # Prova per 30 volte (circa 1 minuto)
    retry_delay = 2   # Attendi 2 secondi tra ogni tentativo
    
    for i in range(N_SERVERS):
        uri = NEO4J_SERVERS[i]
        connected = False
        
        for attempt in range(max_retries):
            try:
                drivers[i] = GraphDatabase.driver(uri, auth=(NEO4J_USER, NEO4J_PASS))
                drivers[i].verify_connectivity()
                print(f"Server {i} ({uri}) connesso.")
                connected = True
                break
            except Exception as e:
                if attempt == 0:
                    print(f"Server {i} non ancora pronto, attendo l'inizializzazione...")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    print(f"ERRORE: Impossibile connettersi al Server {i} dopo {max_retries} tentativi.")
                    print(f"Verifica che Docker sia in esecuzione e che la password sia corretta.")
                    print(f"Errore: {e}")
                    # Chiudi eventuali connessioni già aperte
                    for j in range(i):
                        drivers[j].close()
                    return None
        
        if not connected:
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
    if MAX_LINKS:
        print(f"*** MODALITÀ TEST: Elaborando solo i primi {MAX_LINKS:,} link ***")
    
    with open(PAGELINKS_DUMP_FILE, 'r', encoding='utf-8') as f:
        progress_bar = tqdm(total=MAX_LINKS if MAX_LINKS else 50_000_000, unit=' link', desc="Elaboro 'pagelinks.sql'")
        
        for line in f:
            #
            # limita il numero di link (per test)
            #
            if MAX_LINKS and total_links_processed >= MAX_LINKS:
                break
            
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
                    progress_bar.update(1)
                    
                    #
                    # quando il batch è pieno, lo scriviamo dentro neo4j
                    #
                    if len(batches[server_id]) >= BATCH_SIZE:
                        try:
                            sessions[server_id].execute_write(add_links_batch, batches[server_id])
                        except Exception as e:
                            print(f"Errore durante l'invio del batch al server {server_id}: {e}")
                        batches[server_id] = []
            
            if total_links_processed > 0 and total_links_processed % 10000 == 0:
                elapsed = time.time() - start_time
                lps = total_links_processed / elapsed
                progress_bar.set_description(f"Elaboro 'pagelinks.sql' ({lps:.0f} link/s)")
    
    progress_bar.close()
    
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
