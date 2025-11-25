import csv
import sys
import time

from neo4j import GraphDatabase

URI = 'bolt://localhost:7687'
AUTH = ('neo4j', 'password')

QUERY_BATCH = """
UNWIND $batch AS row
MERGE (a:Node {id: row.src})
MERGE (b:Node {id: row.dest})
MERGE (a)-[:LINKED_TO]->(b)
"""

def wait_for_connection(uri, auth):
    """
    Tenta di connettersi a Neo4j. Se fallisce (es. DB in avvio),
    riprova ogni 3 secondi all'infinito.
    Restituisce un driver verificato e connesso.
    """
    while True:
        driver = None
        try:
            driver = GraphDatabase.driver(uri, auth=auth)
            driver.verify_connectivity()
            print(f"Connessione a {uri} riuscita.")
            return driver
        except Exception as e:
            print(f"Errore di connessione: {e}")
            print("Riprovo la connessione tra 3 secondi...")
            
            # Se il driver è stato istanziato ma la verifica è fallita, chiudiamolo
            if driver:
                driver.close()
            
            time.sleep(3)

def create_constraints(driver):
    """
    Crea un constraint di unicità sulla proprietà 'id' dei nodi :Node.
    """
    print('\n--- Creating constraints and indexes ---')
    
    constraint_query = """
    CREATE CONSTRAINT node_id_unique IF NOT EXISTS 
    FOR (n:Node) REQUIRE n.id IS UNIQUE
    """
    
    try:
        with driver.session() as session:
            session.run(constraint_query)
            print('✓ Constraint di unicità creato su Node.id')
            print('  (Indice automatico attivo per lookup O(1))')
    
    except Exception as e:
        print(f'⚠ Errore durante la creazione del constraint: {e}')

def run_batch(tx, batch_data):
    tx.run(QUERY_BATCH, batch=batch_data)

def load_batch_data(driver, filename):
    print(f'\n--- Loading data from {filename} ---')
    batch_size = 1000
    batch = []
    total_rows = 0

    try:
        with open(filename, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)

            print('Checking for headers...')
            if 'src_page' not in reader.fieldnames or 'dest_page' not in reader.fieldnames:
                print('--- ERROR: CSV headers not found ---')
                print('\tThe CSV file should contain the following columns: src_page,dest_page')
                return
            
            with driver.session() as session:
                for row in reader:
                    row_data = {
                        'src' : row['src_page'],
                        'dest': row['dest_page']
                    }
                    batch.append(row_data)
                    total_rows += 1

                    if len(batch) >= batch_size:
                        session.execute_write(run_batch, batch)
                        print(f'  ... Inserting batch. Current total elaborated rows: {total_rows}')
                        batch = []

                if batch:
                    session.execute_write(run_batch, batch)
                    print(f"  ... Inserting Final batch. Total rows elaborated: {total_rows}")
    
    except FileNotFoundError:
        print(f'--- ERROR: file {filename} not found. ---')
    except Exception as e:
        print(f'--- ERROR: data loading failed. ---')
        print(f'Error details: {e}')

if __name__ == '__main__':
    # Controllo argomenti input
    if len(sys.argv) < 2:
        print("Errore: Devi specificare il numero del sample.")
        print("Esempio uso: python 4_load_graph.py 2")
        sys.exit(1)

    sample_number = sys.argv[1]
    sample_file_path = f'../data/sample/sample_{sample_number}.csv'

    # Ottieni il driver con logica di retry
    driver = wait_for_connection(URI, AUTH)

    # Usa il driver all'interno di un blocco try/finally per assicurare la chiusura
    try:
        # Crea l'indice PRIMA di caricare i dati
        create_constraints(driver)
        
        # Carica i dati dal file specifico
        load_batch_data(driver, sample_file_path)
    
    except Exception as e:
        print(f"Errore durante l'esecuzione: {e}")
    finally:
        if driver:
            driver.close()
            print("\nDriver chiuso.")