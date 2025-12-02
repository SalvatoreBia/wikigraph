import os
import sys
import csv
import time
from pathlib import Path
from sentence_transformers import SentenceTransformer
from neo4j import GraphDatabase
import numpy as np

# --- CONFIGURAZIONE ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CSV_FILE = DATA_DIR / "sample_content" / "sample_with_names_1_content.csv"

# Neo4j Config
URI = 'bolt://localhost:7687'
AUTH = ('neo4j', 'password')
INDEX_NAME = "trusted_sources_index"
VECTOR_DIM = 384

MODEL_NAME = 'paraphrase-multilingual-MiniLM-L12-v2'

def wait_for_connection(uri, auth):
    while True:
        try:
            driver = GraphDatabase.driver(uri, auth=auth)
            driver.verify_connectivity()
            print(f"✅ Connesso a Neo4j ({uri})")
            return driver
        except Exception as e:
            print(f"⏳ In attesa di Neo4j... ({e})")
            time.sleep(3)

def create_vector_index(driver):
    print(f"🛠️  Verifica/Creazione Indice Vettoriale: {INDEX_NAME}")
    query_check = "SHOW INDEXES"
    
    with driver.session() as session:
        indexes = session.run(query_check).data()
        exists = any(idx['name'] == INDEX_NAME for idx in indexes)
        
        if not exists:
            print(f"   Creazione indice {INDEX_NAME}...")
            # Neo4j 5.x syntax
            query_create = f"""
            CREATE VECTOR INDEX {INDEX_NAME} IF NOT EXISTS
            FOR (n:Node) ON (n.embedding)
            OPTIONS {{indexConfig: {{
                `vector.dimensions`: {VECTOR_DIM},
                `vector.similarity_function`: 'cosine'
            }}}}
            """
            try:
                session.run(query_create)
                print("   ✅ Indice creato.")
            except Exception as e:
                print(f"   ⚠️ Errore creazione indice (potrebbe essere una versione vecchia di Neo4j?): {e}")
        else:
            print("   ✅ Indice già esistente.")

def load_csv_content(filepath):
    documents = []
    if not filepath.exists():
        print(f"❌ File CSV non trovato: {filepath}")
        return []

    print(f"📂 Leggo file CSV da: {filepath}")
    try:
        csv.field_size_limit(sys.maxsize)
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                content = row.get('content', '')
                page_id = row.get('page_id')
                if content and page_id:
                    documents.append({
                        "id": page_id,
                        "text": content
                    })
    except Exception as e:
        print(f"⚠️ Errore lettura CSV: {e}")
    
    return documents

def main():
    print("--- 🧠 EMBEDDING TRUSTED SOURCES TO NEO4J ---")
    
    # 1. Connect
    driver = wait_for_connection(URI, AUTH)
    
    # 2. Create Index
    create_vector_index(driver)
    
    # 3. Load Data
    docs = load_csv_content(CSV_FILE)
    if not docs:
        driver.close()
        return

    print(f"📄 Trovati {len(docs)} documenti.")

    # 4. Load Model
    print(f"🚀 Caricamento Modello: {MODEL_NAME}...")
    model = SentenceTransformer(MODEL_NAME)
    
    # 5. Process & Update Neo4j
    print("⚙️  Calcolo Embeddings e Aggiornamento Neo4j...")
    
    with driver.session() as session:
        count = 0
        for doc in docs:
            # Usa i primi 1000 caratteri per l'embedding (rappresentativi)
            text_for_embedding = doc['text'][:1000]
            embedding = model.encode(text_for_embedding).tolist()
            
            # Update Query
            query = """
            MATCH (n:Node {id: $id})
            SET n.content = $content,
                n.embedding = $embedding
            RETURN count(n) as updated
            """
            
            result = session.run(query, {
                "id": doc['id'],
                "content": doc['text'][:2000], # Salviamo un po' di testo per context (non tutto per non appesantire troppo se enorme)
                "embedding": embedding
            })
            
            updated = result.single()['updated']
            if updated > 0:
                count += 1
            
            if count % 100 == 0:
                print(f"   Aggiornati {count} nodi...", end='\r')
                
    print(f"\n✅ Completato. {count} nodi aggiornati con embedding.")
    driver.close()

if __name__ == "__main__":
    main()
