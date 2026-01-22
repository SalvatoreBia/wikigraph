import csv
import sys
import time
from neo4j import GraphDatabase

URI = 'bolt://localhost:7687'
AUTH = ('neo4j', 'password')

DB_BATCH_SIZE = 500

csv.field_size_limit(sys.maxsize)

UPDATE_QUERY = """
UNWIND $batch AS row
MATCH (n:Node {id: row.id})
SET n.title = row.title,
    n.content = row.content
"""

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

def process_and_write(driver, batch_buffer):
    if not batch_buffer:
        return 0

    with driver.session() as session:
        session.run(UPDATE_QUERY, batch=batch_buffer)
    
    return len(batch_buffer)

def main(csv_file):
    driver = wait_for_connection(URI, AUTH)
    
    print(f"\n--- Inizio importazione contenuti da: {csv_file} ---")
    
    batch_buffer = [] 
    total_processed = 0
    start_time = time.time()

    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            required = ['page_id', 'page_title', 'content']
            if not all(col in reader.fieldnames for col in required):
                print(f"ERRORE: Colonne mancanti nel CSV. Richieste: {required}")
                return

            for row in reader:
                item = {
                    'id': row['page_id'],
                    'title': row['page_title'],
                    'content': row['content']
                }

                batch_buffer.append(item)

                if len(batch_buffer) >= DB_BATCH_SIZE:
                    count = process_and_write(driver, batch_buffer)
                    total_processed += count
                    batch_buffer = [] 
                    
                    elapsed = time.time() - start_time
                    rate = total_processed / elapsed if elapsed > 0 else 0
                    sys.stdout.write(f"\r- Nodi aggiornati: {total_processed} | Velocità: {rate:.0f} nodi/sec")
                    sys.stdout.flush()

            if batch_buffer:
                count = process_and_write(driver, batch_buffer)
                total_processed += count

        total_time = time.time() - start_time
        print(f"\n\n[OK] Completato! Totale nodi aggiornati con testo: {total_processed}")
        print(f"Tempo totale: {total_time:.1f} secondi")

    except FileNotFoundError:
        print(f"Errore: File {csv_file} non trovato.")
    except Exception as e:
        print(f"\nErrore critico: {e}")
    finally:
        driver.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python3 8_add_node_info.py <sample_number>")
        print("Esempio: python3 8_add_node_info.py 1")
        sys.exit(1)

    sample_num = sys.argv[1]
    csv_path = f"../data/sample_content/sample_with_names_{sample_num}_content.csv"
    print(f"📂 File selezionato: {csv_path}")
    main(csv_path)