import csv
import sys
from neo4j import GraphDatabase

# --- CONFIGURAZIONE ---
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "password")
PAGEMAP_FILE = "pagemap.csv"

def get_top_communities(driver):
    """
    Ottiene le 10 community più grandi e i 5 nodi 'Hub' (più connessi) per ognuna.
    """
    query = """
    MATCH (n:Node)
    WHERE n.community IS NOT NULL
    WITH n.community AS comm_id, n
    // Conta i collegamenti per trovare i nodi centrali
    WITH comm_id, n, COUNT { (n)--() } AS degree
    ORDER BY degree DESC
    // Raggruppa per community prendendo i top 5 nodi
    WITH comm_id, collect(n.id)[0..5] AS top_nodes, count(*) as size
    ORDER BY size DESC
    LIMIT 10
    RETURN comm_id, size, top_nodes
    """
    
    print("--- Interrogazione Neo4j per trovare i Leader delle Community ---")
    with driver.session() as session:
        result = session.run(query)
        data = [record.data() for record in result]
        return data

def resolve_names(communities_data, map_file):
    """
    Scansiona il file pagemap.csv per trovare i nomi corrispondenti agli ID target.
    """
    # 1. Raccogli tutti gli ID che ci servono in un set per ricerca veloce O(1)
    target_ids = set()
    for item in communities_data:
        for node_id in item['top_nodes']:
            target_ids.add(str(node_id)) # Assicuriamoci siano stringhe per il confronto CSV
            
    print(f"--- Ricerca nomi per {len(target_ids)} nodi nel file {map_file} ---")
    
    id_to_name = {}
    
    try:
        with open(map_file, 'r', encoding='utf-8', errors='replace') as f:
            # Usiamo un parsing manuale veloce o csv reader
            # Formato atteso: id,title (o id,0,'title'...) dal tuo script 1_parse_file.sh
            # Il tuo script 1_parse generava: id,title (senza quote extra se pulito)
            
            for line in f:
                parts = line.strip().split(',', 1) # Splitta solo alla prima virgola
                if len(parts) < 2: continue
                
                curr_id = parts[0].strip()
                
                if curr_id in target_ids:
                    # Rimuovi eventuali apici residui dal titolo
                    title = parts[1].replace("'", "").strip()
                    id_to_name[curr_id] = title
                    
                    # Ottimizzazione: se li abbiamo trovati tutti, possiamo uscire
                    if len(id_to_name) == len(target_ids):
                        break
                        
    except FileNotFoundError:
        print(f"ERRORE: File {map_file} non trovato.")
        sys.exit(1)

    return id_to_name

def print_report(communities_data, id_to_name):
    print("\n" + "="*60)
    print("REPORT TEMI COMMUNITY")
    print("="*60)
    
    for comm in communities_data:
        c_id = comm['comm_id']
        size = comm['size']
        nodes = comm['top_nodes']
        
        # Traduci ID in Nomi
        node_names = [id_to_name.get(str(nid), f"ID_{nid}") for nid in nodes]
        
        print(f"\n📂 COMMUNITY {c_id} (Nodi: {size})")
        print(f"   Argomenti principali (Hubs):")
        for name in node_names:
            print(f"   - {name}")

if __name__ == "__main__":
    try:
        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            driver.verify_connectivity()
            
            # 1. Ottieni dati grezzi da Neo4j
            comm_data = get_top_communities(driver)
            
            if not comm_data:
                print("Nessuna community trovata. Hai eseguito lo script 4_community_detection.py?")
                sys.exit(0)

            # 2. Risolvi i nomi dal CSV
            mapping = resolve_names(comm_data, PAGEMAP_FILE)
            
            # 3. Stampa
            print_report(comm_data, mapping)
            
            print("\n" + "="*60)
            print("SUGGERIMENTO PER DEMO")
            print("Scegli una community con argomenti chiari (es. Storia, Scienza, Videogiochi)")
            print("e annota il suo ID per il passo successivo.")
            
    except Exception as e:
        print(f"Errore connessione Neo4j: {e}")