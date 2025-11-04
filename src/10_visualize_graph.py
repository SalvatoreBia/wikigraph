#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script per visualizzare il grafo Wikipedia e le comunità.
Crea una visualizzazione HTML interattiva usando Pyvis.

Esecuzione:
    python3 src/10_visualize_graph.py [--max-nodes 1000] [--community-id 12345]
"""

import argparse
import pandas as pd
from pyvis.network import Network
import webbrowser
import os

# Percorsi
OUTPUT_DIR = "spark_output"
COMMUNITY_MAP_FILE = os.path.join(OUTPUT_DIR, "community_map.parquet")
GRAPH_EDGES_FILE = os.path.join(OUTPUT_DIR, "graph_edges.parquet")
GRAPH_VERTICES_FILE = os.path.join(OUTPUT_DIR, "graph_vertices.parquet")
HTML_OUTPUT = "graph_visualization.html"

def load_parquet_data(max_nodes=1000, community_id=None):
    """
    Carica i dati Parquet e filtra per nodi ben connessi.
    Questa versione parte dagli archi per trovare i nodi più connessi.
    """
    print("🔍 Caricamento dati dal Parquet...")
    
    # Carica prima gli archi per trovare i nodi più connessi
    edges_df = pd.read_parquet(GRAPH_EDGES_FILE)
    print(f"  ✓ Archi totali: {len(edges_df):,}")
    
    # Carica comunità
    communities_df = pd.read_parquet(COMMUNITY_MAP_FILE)
    print(f"  ✓ Comunità caricate: {len(communities_df):,} nodi")
    
    # Filtra per comunità specifica se richiesto
    if community_id is not None:
        communities_df = communities_df[communities_df['globalCommunityId'] == community_id]
        print(f"  📌 Filtrato per comunità {community_id}: {len(communities_df):,} nodi")
        if len(communities_df) == 0:
            print("  ❌ Nessun nodo trovato per questa comunità")
            return None, None
        
        # Filtra anche gli archi per questa comunità
        node_titles = set(communities_df['title'].values)
        edges_df = edges_df[
            edges_df['src'].isin(node_titles) & 
            edges_df['dst'].isin(node_titles)
        ]
        print(f"  ✓ Archi nella comunità: {len(edges_df):,}")
    
    # Trova i nodi più connessi (quelli che appaiono più spesso negli archi)
    if max_nodes and (community_id is None):
        print(f"  � Ricerca dei {max_nodes:,} nodi più connessi...")
        
        # Conta le occorrenze di ogni nodo negli archi
        src_counts = edges_df['src'].value_counts()
        dst_counts = edges_df['dst'].value_counts()
        node_counts = src_counts.add(dst_counts, fill_value=0).sort_values(ascending=False)
        
        # Seleziona i top N nodi più connessi
        top_nodes = set(node_counts.head(max_nodes).index)
        
        # Filtra le comunità e gli archi
        communities_df = communities_df[communities_df['title'].isin(top_nodes)]
        edges_df = edges_df[
            edges_df['src'].isin(top_nodes) & 
            edges_df['dst'].isin(top_nodes)
        ]
        
        print(f"  ✓ Selezionati {len(communities_df):,} nodi più connessi")
        print(f"  ✓ Archi tra questi nodi: {len(edges_df):,}")
    
    elif max_nodes and len(communities_df) > max_nodes:
        # Se c'è una comunità specifica, limita comunque
        communities_df = communities_df.head(max_nodes)
        node_titles = set(communities_df['title'].values)
        edges_df = edges_df[
            edges_df['src'].isin(node_titles) & 
            edges_df['dst'].isin(node_titles)
        ]
        print(f"  📊 Limitato a {max_nodes:,} nodi")
        print(f"  ✓ Archi filtrati: {len(edges_df):,}")
    
    return communities_df, edges_df

def create_network_graph(communities_df, edges_df):
    """
    Crea un grafo interattivo con Pyvis.
    """
    print("\n🎨 Creazione visualizzazione...")
    
    # Crea la rete Pyvis
    net = Network(
        height="800px",
        width="100%",
        bgcolor="#222222",
        font_color="white",
        directed=True
    )
    
    # Configurazioni per migliorare la fisica del grafo
    net.set_options("""
    {
      "physics": {
        "enabled": true,
        "barnesHut": {
          "gravitationalConstant": -8000,
          "centralGravity": 0.3,
          "springLength": 95,
          "springConstant": 0.04,
          "damping": 0.09
        }
      },
      "nodes": {
        "font": {
          "size": 12
        }
      },
      "edges": {
        "arrows": {
          "to": {
            "enabled": true,
            "scaleFactor": 0.5
          }
        },
        "color": {
          "inherit": "from"
        },
        "smooth": {
          "type": "continuous"
        }
      }
    }
    """)
    
    # Mappa comunità a colori
    unique_communities = communities_df['globalCommunityId'].unique()
    color_map = {}
    colors = [
        "#e74c3c", "#3498db", "#2ecc71", "#f39c12", "#9b59b6",
        "#1abc9c", "#34495e", "#e67e22", "#95a5a6", "#d35400"
    ]
    for i, comm_id in enumerate(unique_communities):
        color_map[comm_id] = colors[i % len(colors)]
    
    # Aggiungi nodi
    print(f"  📍 Aggiunta {len(communities_df):,} nodi...")
    for idx, row in communities_df.iterrows():
        node_id = row['title']
        community_id = row['globalCommunityId']
        server_id = row['serverId']
        color = color_map.get(community_id, "#95a5a6")
        
        net.add_node(
            node_id,
            label=node_id[:30] + ("..." if len(node_id) > 30 else ""),
            title=f"<b>{node_id}</b><br>Community: {community_id}<br>Server: {server_id}",
            color=color,
            size=20
        )
    
    # Aggiungi archi
    print(f"  🔗 Aggiunta {len(edges_df):,} archi...")
    for idx, row in edges_df.iterrows():
        net.add_edge(row['src'], row['dst'])
    
    print(f"  ✓ Grafo creato: {len(communities_df):,} nodi, {len(edges_df):,} archi")
    
    return net

def show_community_stats(communities_df):
    """
    Mostra statistiche sulle comunità.
    """
    print("\n📊 Top 10 Comunità per dimensione:")
    top_communities = (
        communities_df.groupby('globalCommunityId')
        .size()
        .sort_values(ascending=False)
        .head(10)
    )
    for comm_id, size in top_communities.items():
        print(f"   Comunità {comm_id}: {size:,} nodi")

def main():
    parser = argparse.ArgumentParser(
        description="Visualizza il grafo Wikipedia con comunità",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  # Visualizza i primi 500 nodi
  python3 src/10_visualize_graph.py --max-nodes 500
  
  # Visualizza solo una comunità specifica
  python3 src/10_visualize_graph.py --community-id 51539607717
  
  # Visualizza una comunità con limite di nodi
  python3 src/10_visualize_graph.py --community-id 51539607717 --max-nodes 200
        """
    )
    parser.add_argument(
        '--max-nodes',
        type=int,
        default=1000,
        help='Numero massimo di nodi da visualizzare (default: 1000)'
    )
    parser.add_argument(
        '--community-id',
        type=int,
        default=None,
        help='Visualizza solo una comunità specifica (opzionale)'
    )
    parser.add_argument(
        '--no-browser',
        action='store_true',
        help='Non aprire automaticamente il browser'
    )
    
    args = parser.parse_args()
    
    print("="*70)
    print("  WIKIPEDIA GRAPH VISUALIZER")
    print("="*70)
    
    # Verifica che i file esistano
    if not os.path.exists(COMMUNITY_MAP_FILE):
        print(f"\n❌ ERRORE: File '{COMMUNITY_MAP_FILE}' non trovato.")
        print("Esegui prima '3_load_graph_spark.py' per generare i dati.")
        return
    
    # Carica i dati
    communities_df, edges_df = load_parquet_data(
        max_nodes=args.max_nodes,
        community_id=args.community_id
    )
    
    if communities_df is None or len(communities_df) == 0:
        print("❌ Nessun dato da visualizzare")
        return
    
    # Mostra statistiche
    show_community_stats(communities_df)
    
    # Crea il grafo
    net = create_network_graph(communities_df, edges_df)
    
    # Salva HTML
    print(f"\n💾 Salvataggio in '{HTML_OUTPUT}'...")
    net.save_graph(HTML_OUTPUT)
    print(f"  ✓ File creato con successo")
    
    print("\n" + "="*70)
    print(f"  ✅ Visualizzazione pronta!")
    print("="*70)
    print(f"\n📂 Apri il file: {os.path.abspath(HTML_OUTPUT)}")
    print("\n💡 Suggerimenti:")
    print("  - Trascina i nodi per riorganizzare il grafo")
    print("  - Zoom con la rotellina del mouse")
    print("  - Hover sui nodi per vedere i dettagli")
    print("  - I colori rappresentano comunità diverse")
    
    # Apri nel browser
    if not args.no_browser:
        print("\n🌐 Apertura nel browser...")
        webbrowser.open('file://' + os.path.abspath(HTML_OUTPUT))

if __name__ == "__main__":
    main()
