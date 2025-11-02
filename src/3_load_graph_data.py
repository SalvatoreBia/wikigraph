#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import re
import os
import hashlib
from neo4j import GraphDatabase
from tqdm import tqdm
import time

# === Percorsi ===
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
PAGELINKS_DUMP_FILE = os.path.join(PROJECT_DIR, 'data', 'itwiki-latest-pagelinks.sql')
DB_FILE = os.path.join(SCRIPT_DIR, 'page_map.db')

# === Parametri ===
BATCH_SIZE = 10_000
N_SERVERS = 4
MAX_LINKS = 100_000  # None per tutto

NEO4J_SERVERS = {
    0: "bolt://localhost:7687",
    1: "bolt://localhost:7688",
    2: "bolt://localhost:7689",
    3: "bolt://localhost:7690",
}
NEO4J_USER = "neo4j"
NEO4J_PASS = "password"

# === Regex per (srcId,0,tgtId) ===
PAGELINKS_INSERT_REGEX = re.compile(r"\(([0-9]+),0,([0-9]+)\)")

def get_server_id(page_title: str) -> int:
    h = hashlib.md5(page_title.encode('utf-8')).digest()
    return int.from_bytes(h, 'little') % N_SERVERS

def create_drivers():
    drivers = {}
    print("Connessione ai 4 server Neo4j shard...")
    max_retries, retry_delay = 30, 2
    for i in range(N_SERVERS):
        uri = NEO4J_SERVERS[i]
        for attempt in range(max_retries):
            try:
                drv = GraphDatabase.driver(uri, auth=(NEO4J_USER, NEO4J_PASS))
                drv.verify_connectivity()
                drivers[i] = drv
                print(f"Server {i} ({uri}) connesso.")
                break
            except Exception as e:
                if attempt == 0:
                    print(f"Server {i} non pronto, attendo l'inizializzazione...")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    print(f"ERRORE: Impossibile connettersi al Server {i}. Errore: {e}")
                    for d in drivers.values():
                        d.close()
                    return None
    print("Tutti i server connessi.")
    return drivers

def close_drivers(drivers):
    print("Chiusura connessioni Neo4j...")
    for driver in drivers.values():
        driver.close()

def add_links_batch(tx, links_batch):
    query = """
    UNWIND $links AS link
    MERGE (a:Page {title: link.source})
      ON CREATE SET a.serverId = link.sourceServerId
      ON MATCH  SET a.serverId = link.sourceServerId
    MERGE (b:Page {title: link.target})
      ON CREATE SET b.serverId = link.targetServerId
      ON MATCH  SET b.serverId = link.targetServerId
    MERGE (a)-[:LINKS_TO]->(b)
    """
    tx.run(query, links=links_batch)

def preload_id_cache(conn_sqlite):
    print("Precaricamento cache ID->Titolo in memoria...")
    cursor = conn_sqlite.cursor()
    cursor.execute("SELECT page_id, page_title FROM page_map")
    cache, count = {}, 0
    for page_id, page_title in tqdm(cursor, desc="Carico mappa ID->Titolo"):
        cache[int(page_id)] = page_title.replace('_', ' ')
        count += 1
    print(f"✓ Cache completa: {count:,} pagine")
    return cache

def process_pagelinks_dump(drivers):
    print(f"Connessione al database SQLite '{DB_FILE}'...")
    conn_sqlite = sqlite3.connect(DB_FILE)
    id_to_title = preload_id_cache(conn_sqlite)

    def title_of(pid): return id_to_title.get(pid)

    batches = {i: [] for i in range(N_SERVERS)}
    sessions = {i: drivers[i].session() for i in range(N_SERVERS)}

    total_links_processed = 0
    start_time = time.time()

    print(f"Inizio elaborazione di {PAGELINKS_DUMP_FILE}...")
    if MAX_LINKS:
        print(f"*** MODALITÀ TEST: Elaboro solo i primi {MAX_LINKS:,} link ***")

    with open(PAGELINKS_DUMP_FILE, 'r', encoding='utf-8') as f:
        progress_bar = tqdm(total=MAX_LINKS if MAX_LINKS else 50_000_000,
                            unit=' link', desc="Elaboro 'pagelinks.sql'")

        for line in f:
            if MAX_LINKS and total_links_processed >= MAX_LINKS:
                break
            if '(' not in line:
                continue

            matches = PAGELINKS_INSERT_REGEX.findall(line)
            for match in matches:
                if MAX_LINKS and total_links_processed >= MAX_LINKS:
                    break
                try:
                    src_id, tgt_id = int(match[0]), int(match[1])
                except ValueError:
                    continue

                src_title, tgt_title = title_of(src_id), title_of(tgt_id)
                if not (src_title and tgt_title):
                    continue

                server_id = get_server_id(src_title)
                tgt_server_id = get_server_id(tgt_title)
                batches[server_id].append({
                    "source": src_title,
                    "target": tgt_title,
                    "sourceServerId": server_id,
                    "targetServerId": tgt_server_id
                })
                total_links_processed += 1
                progress_bar.update(1)

                if len(batches[server_id]) >= BATCH_SIZE:
                    try:
                        sessions[server_id].execute_write(add_links_batch, batches[server_id])
                    finally:
                        batches[server_id] = []

            if total_links_processed and total_links_processed % 100_000 == 0:
                elapsed = time.time() - start_time
                lps = total_links_processed / max(elapsed, 1e-6)
                progress_bar.set_description(f"Elaboro 'pagelinks.sql' ({lps:.0f} link/s)")

    progress_bar.close()

    print("Invio dei batch rimanenti...")
    for server_id, batch in batches.items():
        if batch:
            try:
                sessions[server_id].execute_write(add_links_batch, batch)
                print(f"Inviato batch finale di {len(batch)} link al server {server_id}.")
            except Exception as e:
                print(f"Errore durante invio batch finale S{server_id}: {e}")

    print("Chiusura sessioni...")
    for s in sessions.values():
        s.close()
    conn_sqlite.close()

    elapsed = time.time() - start_time
    print("\n--- CARICAMENTO COMPLETATO ---")
    print(f"Link totali elaborati: {total_links_processed}")
    print(f"Tempo totale: {elapsed:.2f} s")

def main():
    if not os.path.exists(DB_FILE):
        print(f"ERRORE: File '{DB_FILE}' non trovato.")
        print("Esegui prima 'python3 src/build_id_title_map.py' per crearlo.")
        return

    drivers = create_drivers()
    if not drivers:
        return

    print("Creazione vincoli/indici su ciascuno shard...")
    for i, driver in drivers.items():
        with driver.session() as s:
            try:
                s.run("CREATE CONSTRAINT page_title_unique IF NOT EXISTS FOR (p:Page) REQUIRE p.title IS UNIQUE")
                s.run("CREATE INDEX page_gcid IF NOT EXISTS FOR (p:Page) ON (p.globalCommunityId)")
                s.run("CREATE INDEX page_serverid IF NOT EXISTS FOR (p:Page) ON (p.serverId)")
                print(f"Indice/i creati su server {i}.")
            except Exception as e:
                print(f"Errore creazione indici su server {i}: {e}")

    process_pagelinks_dump(drivers)
    close_drivers(drivers)

if __name__ == "__main__":
    main()
