from neo4j import GraphDatabase
import csv

URI = 'bolt://localhost:7687'
AUTH = ('neo4j', 'password')
SAMPLE_FILE = './sample_3.csv'


QUERY_BATCH = """
UNWIND $batch AS row
MERGE (a:Node {id: row.src})
MERGE (b:Node {id: row.dest})
MERGE (a)-[:LINKED_TO]->(b)
"""


def test_connection(driver):
    try:
        driver.verify_connectivity()
        print(f"Connessione a {URI} riuscita.")

    except Exception as e:
        print(f"Errore di connessione: {e}")
        return False
    
    return True


def create_constraints(driver):
    """
    Crea un constraint di unicità sulla proprietà 'id' dei nodi :Node.
    Questo crea automaticamente un indice, trasformando le operazioni MERGE
    da scansioni lineari O(N) a lookup indicizzati O(1), riducendo drasticamente
    i tempi di caricamento da ore a minuti.
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
    print('\n--- Loading data ---')
    batch_size = 1000
    batch = []
    total_rows = 0

    try:
        with open(SAMPLE_FILE, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)

            print('Checking for headers...')
            if 'src_page' not in reader.fieldnames or 'dest_page' not in reader.fieldnames:
                print('--- ERROR: CSV headers not found ---')
                print('\tThe SAMPLE_FILE should contain the following columns: src_page,dest_page')
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
        print(f'--- ERROR: file {SAMPLE_FILE} not found. ---')
    except Exception as e:
        print(f'--- ERROR: data loading failed. ---')
        print(f'Error details: {e}')



if __name__ == '__main__':
    try:
        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            is_connected = test_connection(driver)
            
            if is_connected:
                print("Test connection passed.")
                
                # Crea l'indice PRIMA di caricare i dati
                # Questo trasforma MERGE da O(N) a O(1) per ogni riga
                create_constraints(driver)
                
                load_batch_data(driver, SAMPLE_FILE)
    
    except Exception as e:
        print(f"Impossibile creare il driver Neo4j: {e}")