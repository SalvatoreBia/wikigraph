from graphdatascience import GraphDataScience
import time

N_SERVERS = 4
NEO4J_SERVERS = {
    0: "bolt://localhost:7687",
    1: "bolt://localhost:7688",
    2: "bolt://localhost:7689",
    3: "bolt://localhost:7690",
}
NEO4J_USER = "neo4j"
NEO4J_PASS = "password"

def run_community_detection_on_server(server_uri, server_id):
    """
    Si connette a un singolo server Neo4j ed esegue l'algoritmo GDS.
    """
    print(f"\n--- Inizio elaborazione Server {server_id} ({server_uri}) ---")
    gds = None
    try:
        gds = GraphDataScience(server_uri, auth=(NEO4J_USER, NEO4J_PASS))
        gds.set_database("neo4j") 
        print(f"[Server {server_id}] Connesso a GDS.")

        graph_name = f'wiki_shard_{server_id}'

        if gds.graph.exists(graph_name).exists:
            print(f"[Server {server_id}] Grafo GDS '{graph_name}' esistente, lo elimino.")
            gds.graph.drop(graph_name)
        
        print(f"[Server {server_id}] Creazione proiezione grafo '{graph_name}' in memoria...")
        G, result = gds.graph.project(
            graph_name,
            'Page',          
            'LINKS_TO'       
        )
        print(f"[Server {server_id}] Proiezione creata: {result.nodeCount} nodi, {result.relationshipCount} relazioni.")

        print(f"[Server {server_id}] Esecuzione Label Propagation...")
        result = gds.labelPropagation.write(
            G,
            writeProperty='communityId' 
        )
        
        print(f"[Server {server_id}] Comunità calcolate: {result.communityCount}")
        print(f"[Server {server_id}] Risultati salvati sui nodi con proprietà 'communityId'.")

        gds.graph.drop(G)
        print(f"[Server {server_id}] Proiezione grafo GDS eliminata dalla memoria.")
        
    except Exception as e:
        print(f"ERRORE [Server {server_id}]: {e}")
    finally:
        if gds:
            gds.close()
            print(f"[Server {server_id}] Connessione chiusa.")

if __name__ == "__main__":
    start_time = time.time()
    print("Avvio calcolo comunità GDS su tutti gli shard...")
    
    for i in range(N_SERVERS):
        run_community_detection_on_server(NEO4J_SERVERS[i], i)
    
    elapsed = time.time() - start_time
    print(f"\n--- Elaborazione GDS completata in {elapsed:.2f} secondi ---")