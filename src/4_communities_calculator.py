#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
    print(f"\n--- Inizio elaborazione Server {server_id} ({server_uri}) ---")
    gds = None
    try:
        gds = GraphDataScience(server_uri, auth=(NEO4J_USER, NEO4J_PASS))
        gds.set_database("neo4j")
        print(f"[Server {server_id}] Connesso a GDS.")

        # Pre-creo communityId = -1 se assente
        print(f"[Server {server_id}] Inizializzo 'communityId' a -1 dove mancante...")
        cypher_query = """
        MATCH (n:Page)
        WHERE n.communityId IS NULL
        SET n.communityId = -1
        RETURN count(n) AS nodes_updated
        """
        result = gds.run_cypher(cypher_query)
        print(f"[Server {server_id}] Impostato 'communityId = -1' per {result['nodes_updated'][0]} nodi.")

        graph_name = f'wiki_graph_shard_{server_id}'
        if gds.graph.exists(graph_name).exists:
            print(f"[Server {server_id}] Grafo GDS '{graph_name}' esistente, lo elimino.")
            gds.graph.drop(graph_name)

        print(f"[Server {server_id}] Proiezione grafo '{graph_name}' in memoria...")
        G, proj = gds.graph.project(graph_name, 'Page', 'LINKS_TO')
        print(f"[Server {server_id}] Proiezione: {proj.nodeCount} nodi, {proj.relationshipCount} relazioni.")

        print(f"[Server {server_id}] Esecuzione Label Propagation...")
        res = gds.labelPropagation.write(G, writeProperty='communityId')
        print(f"[Server {server_id}] Comunità locali: {res.communityCount}")

        gds.graph.drop(G)
        print(f"[Server {server_id}] Proiezione rimossa.")
    except Exception as e:
        print(f"ERRORE [Server {server_id}]: {e}")
    finally:
        if gds:
            gds.close()
            print(f"[Server {server_id}] Connessione chiusa.")

if __name__ == "__main__":
    start_time = time.time()
    print("=" * 70)
    print("Calcolo comunità GDS per shard (communityId locale)...")
    print("=" * 70)

    for server_id in range(N_SERVERS):
        run_community_detection_on_server(NEO4J_SERVERS[server_id], server_id)

    elapsed = time.time() - start_time
    print("\n" + "=" * 70)
    print(f"Completato in {elapsed:.2f} secondi")
    print("=" * 70)
    print("\nNOTA: 'communityId' è locale allo shard; l'unificazione globale avverrà con 'globalCommunityId'.")
