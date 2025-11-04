#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script per caricare il grafo Wikipedia e calcolare le comunità in modo distribuito su Spark.
Sostituisce i 4 server Neo4j con un cluster Spark (1 master + 4 worker).
"""

import sqlite3
import re
import os
import hashlib
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
from pyspark.sql.functions import udf, col, count as spark_count
from graphframes import GraphFrame

# === Percorsi (DENTRO IL CONTAINER DOCKER) ===
SCRIPT_DIR = '/opt/spark-apps'  # ./src
DATA_DIR = '/opt/data'           # ./data
OUTPUT_DIR = '/opt/spark_output' # ./spark_output

PAGELINKS_DUMP_FILE = os.path.join(DATA_DIR, 'itwiki-latest-pagelinks.sql')
DB_FILE = os.path.join(SCRIPT_DIR, 'page_map.db')
COMMUNITY_MAP_FILE = os.path.join(OUTPUT_DIR, "community_map.parquet")
GRAPH_VERTICES_FILE = os.path.join(OUTPUT_DIR, "graph_vertices.parquet")
GRAPH_EDGES_FILE = os.path.join(OUTPUT_DIR, "graph_edges.parquet")

MAX_LINKS = 100_000  # Limite per test, None per caricare TUTTO il grafo
LABEL_PROPAGATION_ITERATIONS = 5
N_SERVERS = 4

# === Regex per estrarre i link ===
PAGELINKS_INSERT_REGEX = re.compile(r"\(([0-9]+),0,([0-9]+)\)")

# === Funzioni Helper ===

@udf(returnType=IntegerType())
def get_server_id_udf(page_title: str) -> int:
    """
    UDF per calcolare il serverId (shard) di una pagina in base al suo titolo.
    Manteniamo questa logica per compatibilità con lo script Neo4j originale.
    """
    if page_title is None:
        return None
    h = hashlib.md5(page_title.encode('utf-8')).digest()
    return int.from_bytes(h, 'little') % N_SERVERS

def load_id_to_title_map(db_path: str) -> dict:
    """
    Carica la mappa ID->Titolo da SQLite in un dizionario Python.
    Questa operazione avviene sul driver (spark-master).
    """
    print(f"Caricamento mappa ID->Titolo da '{db_path}'...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT page_id, page_title FROM page_map")
    
    id_to_title = {}
    count = 0
    for page_id, page_title in cursor:
        # Sostituiamo underscore con spazi (come in Neo4j)
        id_to_title[int(page_id)] = page_title.replace('_', ' ')
        count += 1
        if count % 500_000 == 0:
            print(f"  Caricati {count:,} titoli...")
    
    conn.close()
    print(f"✓ Mappa completa: {count:,} pagine caricate in memoria")
    return id_to_title

def parse_pagelinks_dump(dump_file: str, id_to_title: dict, max_links=None) -> list:
    """
    Legge il dump SQL dei pagelinks e restituisce una lista di tuple (source_title, target_title).
    Questa funzione gira sul driver ed è ottimizzata per velocità.
    """
    print(f"\nInizio lettura di '{dump_file}'...")
    if max_links:
        print(f"*** MODALITÀ TEST: Limite di {max_links:,} link ***")
    else:
        print("*** MODALITÀ COMPLETA: Carico tutto il grafo (può richiedere diversi minuti) ***")
    
    edges = []
    total_links = 0
    skipped = 0
    start_time = time.time()
    
    try:
        with open(dump_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                if max_links and total_links >= max_links:
                    break
                
                if '(' not in line:
                    continue
                
                matches = PAGELINKS_INSERT_REGEX.findall(line)
                for match in matches:
                    if max_links and total_links >= max_links:
                        break
                    
                    try:
                        src_id = int(match[0])
                        tgt_id = int(match[1])
                    except ValueError:
                        continue
                    
                    # Lookup dei titoli
                    src_title = id_to_title.get(src_id)
                    tgt_title = id_to_title.get(tgt_id)
                    
                    if not (src_title and tgt_title):
                        skipped += 1
                        continue
                    
                    edges.append((src_title, tgt_title))
                    total_links += 1
                
                # Progress feedback ogni 10k righe
                if line_num % 10_000 == 0:
                    elapsed = time.time() - start_time
                    lps = total_links / max(elapsed, 1e-6)
                    print(f"  Righe: {line_num:,} | Link: {total_links:,} | Velocità: {lps:.0f} link/s", end='\r')
    
    except FileNotFoundError:
        print(f"\nERRORE: File non trovato: '{dump_file}'")
        print("Assicurati che il dump SQL sia scompattato nella cartella 'data/'.")
        return None
    except Exception as e:
        print(f"\nERRORE durante la lettura: {e}")
        return None
    
    elapsed = time.time() - start_time
    print(f"\n✓ Lettura completata in {elapsed:.1f}s")
    print(f"  Link estratti: {total_links:,}")
    print(f"  Link ignorati (ID mancanti): {skipped:,}")
    
    return edges

def create_graph_dataframes(spark: SparkSession, edges_list: list) -> tuple:
    """
    Crea i DataFrame Spark per vertici e archi a partire dalla lista di edge.
    """
    print("\n=== Creazione DataFrame Spark ===")
    
    # Crea DataFrame degli archi
    print("Conversione edge list in DataFrame Spark...")
    edges_schema = StructType([
        StructField("src", StringType(), False),
        StructField("dst", StringType(), False)
    ])
    edges_df = spark.createDataFrame(edges_list, schema=edges_schema)
    edges_df = edges_df.cache()  # Cache per performance
    num_edges = edges_df.count()
    print(f"✓ Archi: {num_edges:,}")
    
    # Estrai i vertici unici (unione di tutti i source e dest)
    print("Estrazione vertici unici...")
    vertices_src = edges_df.select(col("src").alias("id")).distinct()
    vertices_dst = edges_df.select(col("dst").alias("id")).distinct()
    vertices_df = vertices_src.union(vertices_dst).distinct()
    
    # Aggiungi il serverId a ciascun vertice
    print("Calcolo serverId per ciascun vertice...")
    vertices_df = vertices_df.withColumn("serverId", get_server_id_udf(col("id")))
    vertices_df = vertices_df.cache()
    num_vertices = vertices_df.count()
    print(f"✓ Vertici: {num_vertices:,}")
    
    # Mostra distribuzione per server
    print("\nDistribuzione vertici per server:")
    vertices_df.groupBy("serverId").agg(spark_count("*").alias("count")).orderBy("serverId").show()
    
    return vertices_df, edges_df

def run_community_detection(graph: GraphFrame, max_iter: int) -> 'DataFrame':
    """
    Esegue Label Propagation Algorithm (LPA) per la community detection.
    
    LPA è un algoritmo distribuito nativo di GraphFrames che:
    - Assegna a ciascun nodo un'etichetta (label) iniziale
    - Iterativamente propaga le etichette ai vicini
    - Converge quando i nodi appartengono alla comunità della maggioranza dei loro vicini
    """
    
    print(f"\n=== Avvio Community Detection (Label Propagation, {max_iter} iter) ===")
    print("Questo algoritmo è distribuito sui 4 worker Spark...")
    
    start_time = time.time()
    
    # Label Propagation
    result_df = graph.labelPropagation(maxIter=max_iter)
    
    # Forza il calcolo (lazy evaluation)
    num_communities = result_df.select("label").distinct().count()
    
    elapsed = time.time() - start_time
    print(f"✓ Community detection completata in {elapsed:.1f}s")
    print(f"  Comunità identificate: {num_communities:,}")
    
    return result_df

def save_results(vertices_df, edges_df, communities_df):
    """
    Salva i risultati in formato Parquet per riutilizzo futuro.
    """
    print("\n=== Salvataggio Risultati ===")
    
    # 1. Salva il grafo (vertici e archi) per riutilizzo
    print(f"Salvataggio vertici in '{GRAPH_VERTICES_FILE}'...")
    vertices_df.write.mode("overwrite").parquet(GRAPH_VERTICES_FILE)
    print("✓ Vertici salvati")
    
    print(f"Salvataggio archi in '{GRAPH_EDGES_FILE}'...")
    edges_df.write.mode("overwrite").parquet(GRAPH_EDGES_FILE)
    print("✓ Archi salvati")
    
    # 2. Salva la mappa Community (per lo stream processor)
    print(f"Salvataggio mappa comunità in '{COMMUNITY_MAP_FILE}'...")
    community_map = communities_df.select(
        col("id").alias("title"),
        col("label").alias("globalCommunityId"),
        col("serverId")
    )
    community_map.write.mode("overwrite").parquet(COMMUNITY_MAP_FILE)
    print("✓ Mappa comunità salvata")
    
    # Mostra statistiche sulle comunità
    print("\nTop 10 comunità per dimensione:")
    communities_df.groupBy("label") \
        .agg(spark_count("*").alias("size")) \
        .orderBy(col("size").desc()) \
        .limit(10) \
        .show()

def main():
    print("="*70)
    print("  WIKIPEDIA GRAPH LOADER & COMMUNITY DETECTION - Spark Distribuito")
    print("="*70)
    
    #
    # verifico che page_map.db esista
    #
    if not os.path.exists(DB_FILE):
        print(f"\nERRORE: File '{DB_FILE}' non trovato.")
        print("Esegui prima 'python3 src/1_build_id_title_map.py' per crearlo.")
        return
    
    #
    # verifico che il dump esista
    #
    if not os.path.exists(PAGELINKS_DUMP_FILE):
        print(f"\nERRORE: File '{PAGELINKS_DUMP_FILE}' non trovato.")
        print("Assicurati di avere il dump SQL scompattato nella cartella 'data/'.")
        return
    
    
    print("\n=== Inizializzazione Spark ===")
    spark = (
        SparkSession.builder
        .appName("WikiGraph_CommunityDetection")
        .config("spark.sql.shuffle.partitions", "8") # 8 = 4 worker x 2 core
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"✓ Spark Session creata")
    print(f"  Master: {spark.sparkContext.master}")
    print(f"  App ID: {spark.sparkContext.applicationId}")
    
    try:
        id_to_title = load_id_to_title_map(DB_FILE)
        if not id_to_title:
            print("Impossibile caricare la mappa ID->Titolo")
            return
        
        edges_list = parse_pagelinks_dump(PAGELINKS_DUMP_FILE, id_to_title, max_links=MAX_LINKS)
        if not edges_list:
            print("Nessun link estratto dal dump")
            return
        
        # creo il grafo distribuito
        vertices_df, edges_df = create_graph_dataframes(spark, edges_list)
        
        print("\n=== Creazione GraphFrame ===")
        graph = GraphFrame(vertices_df, edges_df)
        print("✓ GraphFrame creato e distribuito sui worker")
        
        # Eseguo community detection
        communities_df = run_community_detection(graph, max_iter=LABEL_PROPAGATION_ITERATIONS)
        
        save_results(vertices_df, edges_df, communities_df)
        
        print("\n" + "="*70)
        print("ELABORAZIONE COMPLETATA CON SUCCESSO")
        print("="*70)
        print(f"\nRisultati disponibili in: {OUTPUT_DIR}")
        print(f"  - community_map.parquet    → Mappa Title -> CommunityID")
        print(f"  - graph_vertices.parquet   → Tutti i vertici del grafo")
        print(f"  - graph_edges.parquet      → Tutti gli archi del grafo")
        print("\nUsa questi file per:")
        print("  - Stream processing (7_stream_processor.py)")
        print("  - Analisi aggiuntive senza ricaricare il grafo")
        
    except Exception as e:
        print(f"\n❌ ERRORE durante l'elaborazione: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        print("\n=== Chiusura Spark ===")
        spark.stop()
        print("✓ Spark Session chiusa")

if __name__ == "__main__":
    main()
