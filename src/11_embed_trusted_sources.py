import os
import sys
import csv
import time
from pathlib import Path
from sentence_transformers import SentenceTransformer
from neo4j import GraphDatabase
import numpy as np

# --- CONFIGURAZIONE ---
from config_loader import load_config

CONFIG = load_config()

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CSV_FILE = DATA_DIR / "sample_content" / "sample_with_names_1_content.csv"

# Neo4j Config
# Neo4j Config
URI = CONFIG['neo4j']['uri']
AUTH = tuple(CONFIG['neo4j']['auth'])
INDEX_NAME = "trusted_sources_index"

# Load from Config
VECTOR_DIM = CONFIG['embedding']['dimension']
MODEL_NAME = CONFIG['embedding']['model_name']
TEXT_LIMIT = CONFIG['processing']['text_limit']
BATCH_SIZE = CONFIG['processing']['batch_size']

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
                
                # TRUNCATE START
                if content:
                    content = content[:TEXT_LIMIT]
                # TRUNCATE END

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
    print(f"   Config: Text Limit={TEXT_LIMIT}, Batch={BATCH_SIZE}")
    print(f"   Model: {MODEL_NAME}")
    
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
    
    total_updated = 0
    
    with driver.session() as session:
        # Process in batches
        for i in range(0, len(docs), BATCH_SIZE):
            batch_docs = docs[i : i + BATCH_SIZE]
            
            # Prepare batch for Neo4j UNWIND
            batch_data = []
            texts_to_embed = []
            
            for doc in batch_docs:
                text_content = doc['text'] # Already truncated load_csv_content
                texts_to_embed.append(text_content)
            
            # Batch embedding
            embeddings = model.encode(texts_to_embed).tolist()
            
            for j, doc in enumerate(batch_docs):
                batch_data.append({
                    "id": doc['id'],
                    "content": doc['text'],
                    "embedding": embeddings[j]
                })
            
            # Update Query (Batch)
            query = """
            UNWIND $batch AS row
            MATCH (n:Node {id: row.id})
            SET n.content = row.content,
                n.embedding = row.embedding
            RETURN count(n) as updated
            """
            
            result = session.run(query, {"batch": batch_data})
            updated = result.single()['updated']
            total_updated += updated
            
            print(f"   Processati {min(i + BATCH_SIZE, len(docs))}/{len(docs)}... (Aggiornati: {updated})", end='\r')
                
    print(f"\n✅ Completato. {total_updated} nodi aggiornati con embedding.")
    driver.close()

if __name__ == "__main__":
    main()
