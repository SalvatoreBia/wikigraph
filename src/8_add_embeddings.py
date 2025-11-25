import sys
import csv
import time
import torch
from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer

# --- CONFIGURAZIONE ---
URI = 'bolt://localhost:7687'
AUTH = ('neo4j', 'password')
MODEL_NAME = 'paraphrase-multilingual-MiniLM-L12-v2' 

# FIX ERRORE CSV: Aumentiamo il limite della dimensione del campo al massimo possibile
csv.field_size_limit(sys.maxsize)

# BATCH SIZES (Ottimizzazione Velocità)
# AI_BATCH_SIZE: Quante frasi mandare al modello INSIEME. 
# Se hai GPU: metti 256 o 512. Se CPU: 64 o 128 (per evitare blocchi).
AI_BATCH_SIZE = 128 

# DB_BATCH_SIZE: Quanti nodi aggiornare su Neo4j in una singola transazione
DB_BATCH_SIZE = 500

# Query Neo4j
UPDATE_QUERY = """
UNWIND $batch AS row
MATCH (n:Node {id: row.id})
SET n.title = row.title,
    n.textEmbedding = row.embedding
"""

def get_device():
    """Rileva automaticamente l'acceleratore hardware"""
    if torch.cuda.is_available():
        print("--- 🚀 Rilevata GPU NVIDIA (CUDA). Modalità TURBO attivata! ---")
        return 'cuda'
    elif torch.backends.mps.is_available():
        print("--- ⚡ Rilevato Apple Silicon (MPS). Accelerazione attivata! ---")
        return 'mps'
    else:
        print("--- 🐢 Nessuna GPU rilevata. Esecuzione su CPU standard. ---")
        return 'cpu'

def wait_for_connection(uri, auth):
    while True:
        try:
            driver = GraphDatabase.driver(uri, auth=auth)
            driver.verify_connectivity()
            print(f"Connessione a {uri} riuscita.")
            return driver
        except Exception as e:
            print(f"Errore connessione: {e}. Riprovo tra 3s...")
            time.sleep(3)

def create_vector_index(driver):
    """Crea l'indice vettoriale su Neo4j"""
    print("\n--- Creazione Indice Vettoriale ---")
    index_query = """
    CREATE VECTOR INDEX text_embeddings IF NOT EXISTS
    FOR (n:Node) ON (n.textEmbedding)
    OPTIONS {indexConfig: {
      `vector.dimensions`: 384,
      `vector.similarity_function`: 'cosine'
    }}
    """
    try:
        with driver.session() as session:
            session.run(index_query)
        print("✓ Indice 'text_embeddings' (384 dim) verificato.")
    except Exception as e:
        print(f"⚠ Attenzione (Indice): {e}")

def process_and_write(driver, model, batch_buffer):
    """
    Processa un blocco di dati:
    1. Prende i testi dal buffer
    2. Calcola embeddings in parallelo (Vettorizzazione)
    3. Scrive su Neo4j
    """
    if not batch_buffer:
        return 0

    # 1. Estrai i testi da vettorizzare
    texts = [item['text_for_ai'] for item in batch_buffer]
    
    # 2. Bulk Inference (Molto più veloce del ciclo for)
    embeddings = model.encode(texts, batch_size=AI_BATCH_SIZE, show_progress_bar=False, convert_to_numpy=True)

    # 3. Prepara i dati per Neo4j
    neo4j_payload = []
    for i, item in enumerate(batch_buffer):
        neo4j_payload.append({
            'id': item['id'],
            'title': item['title'],
            'embedding': embeddings[i].tolist() # Converte da numpy a list
        })

    # 4. Scrittura su DB
    with driver.session() as session:
        session.run(UPDATE_QUERY, batch=neo4j_payload)
    
    return len(batch_buffer)

def main(csv_file):
    device = get_device()
    print(f"--- Caricamento modello {MODEL_NAME} su {device} ...")
    embedder = SentenceTransformer(MODEL_NAME, device=device)
    
    driver = wait_for_connection(URI, AUTH)
    
    # Creiamo subito l'indice prima di caricare
    create_vector_index(driver)
    
    print(f"\n--- Inizio elaborazione file: {csv_file} ---")
    
    batch_buffer = [] # Buffer per accumulare righe
    total_processed = 0
    start_time = time.time()

    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            # Verifica colonne
            required = ['page_id', 'page_title', 'content']
            if not all(col in reader.fieldnames for col in required):
                print(f"ERRORE: Colonne mancanti. Richieste: {required}")
                return

            for row in reader:
                content = row['content']
                title = row['page_title']
                
                # Se il contenuto è troppo breve o vuoto, usiamo il titolo raddoppiato per dare contesto
                text_to_embed = content if content and len(content.strip()) > 10 else f"{title} {title}"

                # Aggiungi al buffer
                batch_buffer.append({
                    'id': row['page_id'],
                    'title': title,
                    'text_for_ai': text_to_embed
                })

                # Quando il buffer raggiunge la dimensione ottimale, processiamo
                if len(batch_buffer) >= DB_BATCH_SIZE:
                    count = process_and_write(driver, embedder, batch_buffer)
                    total_processed += count
                    batch_buffer = [] # Svuota
                    
                    # Stats in tempo reale
                    elapsed = time.time() - start_time
                    rate = total_processed / elapsed
                    sys.stdout.write(f"\r🚀 Nodi processati: {total_processed} | Velocità: {rate:.1f} nodi/sec")
                    sys.stdout.flush()

            # Processa gli ultimi rimasti nel buffer
            if batch_buffer:
                count = process_and_write(driver, embedder, batch_buffer)
                total_processed += count

        total_time = time.time() - start_time
        print(f"\n\n[OK] Completato! Totale nodi aggiornati: {total_processed}")
        print(f"Tempo totale: {total_time:.1f} secondi")

    except FileNotFoundError:
        print(f"Errore: File {csv_file} non trovato.")
    except Exception as e:
        print(f"\nErrore critico: {e}")
    finally:
        driver.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python3 6_add_embeddings_fast.py <file_content.csv>")
        sys.exit(1)

    csv_path = sys.argv[1]
    main(csv_path)