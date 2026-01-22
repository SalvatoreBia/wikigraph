import argparse
import csv
import os
import sys
import time

from neo4j import GraphDatabase

URI = 'bolt://localhost:7687'
AUTH = ('neo4j', 'password')

def test_connection(driver):
    try:
        driver.verify_connectivity()
        print(f"Connessione a {URI} riuscita.")
        return True
    except Exception as e:
        print(f"Errore di connessione: {e}")
        return False

def get_graph_stats(driver):
    print('\n--- Statistiche Grafo ---')
    with driver.session() as session:
        result = session.run("MATCH (n:Node) RETURN count(n) AS count")
        node_count = result.single()["count"]
        print(f'  Totale nodi: {node_count}')
        
        result = session.run("MATCH ()-[r:LINKED_TO]->() RETURN count(r) AS count")
        rel_count = result.single()["count"]
        print(f'  Totale relazioni: {rel_count}')
        
        return node_count, rel_count

def create_graph_projection(driver):
    print('\n--- Creazione Proiezione Grafo ---')
    
    with driver.session() as session:
        check_query = """
        CALL gds.graph.exists('wikiGraph')
        YIELD exists
        RETURN exists
        """
        
        result = session.run(check_query)
        exists = result.single()["exists"]
        
        if exists:
            print('  La proiezione esiste già. Elimino la vecchia proiezione...')
            session.run("CALL gds.graph.drop('wikiGraph') YIELD graphName")
            print('  Vecchia proiezione eliminata.')
        
        projection_query = """
        CALL gds.graph.project(
            'wikiGraph',
            'Node',
            {
                LINKED_TO: {
                    orientation: 'UNDIRECTED'
                }
            }
        )
        YIELD graphName, nodeCount, relationshipCount
        RETURN graphName, nodeCount, relationshipCount
        """
        
        print('  Creazione proiezione...')
        result = session.run(projection_query)
        record = result.single()
        
        print(f'  Proiezione creata: {record["graphName"]}')
        print(f'    Nodi: {record["nodeCount"]}')
        print(f'    Relazioni: {record["relationshipCount"]}')
        
        return True

def run_louvain(driver, max_iterations=10, tolerance=0.0001):
    print('\n--- Esecuzione Algoritmo Louvain ---')
    print(f'  Parametri:')
    print(f'    Max iterazioni: {max_iterations}')
    print(f'    Tolleranza: {tolerance}')
    
    with driver.session() as session:
        stats_query = """
        CALL gds.louvain.stats('wikiGraph', {
            maxIterations: $maxIterations,
            tolerance: $tolerance
        })
        YIELD communityCount, modularity, modularities
        RETURN communityCount, modularity, modularities
        """
        
        print('\n  Calcolo statistiche...')
        start_time = time.time()
        result = session.run(stats_query, maxIterations=max_iterations, tolerance=tolerance)
        record = result.single()
        stats_time = time.time() - start_time
        
        print(f'\n  Statistiche calcolate in {stats_time:.2f} secondi:')
        print(f'    Numero comunità: {record["communityCount"]}')
        print(f'    Modularità: {record["modularity"]:.4f}')
        print(f'    Iterazioni: {len(record["modularities"])}')
        
        write_query = """
        CALL gds.louvain.write('wikiGraph', {
            writeProperty: 'community',
            maxIterations: $maxIterations,
            tolerance: $tolerance
        })
        YIELD communityCount, modularity, nodePropertiesWritten
        RETURN communityCount, modularity, nodePropertiesWritten
        """
        
        print('\n  Scrittura risultati nel grafo...')
        start_time = time.time()
        result = session.run(write_query, maxIterations=max_iterations, tolerance=tolerance)
        record = result.single()
        write_time = time.time() - start_time
        
        print(f'  Risultati scritti in {write_time:.2f} secondi')
        print(f'    Proprietà scritte: {record["nodePropertiesWritten"]}')
        
        return record['communityCount']

def run_leiden(driver, max_iterations=10, tolerance=0.0001, gamma=1.0):
    print('\n--- Esecuzione Algoritmo Leiden ---')
    print(f'  Parametri:')
    print(f'    Max iterazioni: {max_iterations}')
    print(f'    Tolleranza: {tolerance}')
    print(f'    Gamma (risoluzione): {gamma}')
    
    with driver.session() as session:
        stats_query = """
        CALL gds.leiden.stats('wikiGraph', {
            maxLevels: $maxIterations,
            tolerance: $tolerance,
            gamma: $gamma
        })
        YIELD communityCount, modularity, modularities
        RETURN communityCount, modularity, modularities
        """
        
        print('\n  Calcolo statistiche...')
        start_time = time.time()
        result = session.run(stats_query, maxIterations=max_iterations, tolerance=tolerance, gamma=gamma)
        record = result.single()
        stats_time = time.time() - start_time
        
        print(f'\n  Statistiche calcolate in {stats_time:.2f} secondi:')
        print(f'    Numero comunità: {record["communityCount"]}')
        print(f'    Modularità: {record["modularity"]:.4f}')
        print(f'    Livelli: {len(record["modularities"])}')
        
        write_query = """
        CALL gds.leiden.write('wikiGraph', {
            writeProperty: 'community',
            maxLevels: $maxIterations,
            tolerance: $tolerance,
            gamma: $gamma
        })
        YIELD communityCount, modularity, nodePropertiesWritten
        RETURN communityCount, modularity, nodePropertiesWritten
        """
        
        print('\n  Scrittura risultati nel grafo...')
        start_time = time.time()
        result = session.run(write_query, maxIterations=max_iterations, tolerance=tolerance, gamma=gamma)
        record = result.single()
        write_time = time.time() - start_time
        
        print(f'  Risultati scritti in {write_time:.2f} secondi')
        print(f'    Proprietà scritte: {record["nodePropertiesWritten"]}')
        
        return record['communityCount']

def run_lpa(driver, max_iterations=10):
    print('\n--- Esecuzione Algoritmo Label Propagation (LPA) ---')
    print(f'  Parametri:')
    print(f'    Max iterazioni: {max_iterations}')
    
    with driver.session() as session:
        stats_query = """
        CALL gds.labelPropagation.stats('wikiGraph', {
            maxIterations: $maxIterations
        })
        YIELD communityCount, ranIterations, didConverge
        RETURN communityCount, ranIterations, didConverge
        """
        
        print('\n  Calcolo statistiche...')
        start_time = time.time()
        result = session.run(stats_query, maxIterations=max_iterations)
        record = result.single()
        stats_time = time.time() - start_time
        
        print(f'\n  Statistiche calcolate in {stats_time:.2f} secondi:')
        print(f'    Numero comunità: {record["communityCount"]}')
        print(f'    Iterazioni: {record["ranIterations"]}')
        print(f'    Convergenza: {record["didConverge"]}')
        
        write_query = """
        CALL gds.labelPropagation.write('wikiGraph', {
            writeProperty: 'community',
            maxIterations: $maxIterations
        })
        YIELD communityCount, ranIterations, didConverge, nodePropertiesWritten
        RETURN communityCount, ranIterations, didConverge, nodePropertiesWritten
        """
        
        print('\n  Scrittura risultati nel grafo...')
        start_time = time.time()
        result = session.run(write_query, maxIterations=max_iterations)
        record = result.single()
        write_time = time.time() - start_time
        
        print(f'  Risultati scritti in {write_time:.2f} secondi')
        print(f'    Proprietà scritte: {record["nodePropertiesWritten"]}')
        
        return record['communityCount']

def export_communities(driver, output_file):
    print(f'\n--- Esportazione Comunità in {output_file} ---')
    
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        print(f'  Directory creata: {output_dir}')
    
    with driver.session() as session:
        query = """
        MATCH (n:Node)
        WHERE n.community IS NOT NULL
        RETURN n.id AS node_id, n.community AS community_id
        ORDER BY n.community, n.id
        """
        
        result = session.run(query)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['node_id', 'community_id'])
            
            count = 0
            for record in result:
                writer.writerow([record['node_id'], record['community_id']])
                count += 1
        
        print(f'  Esportati {count} nodi in {output_file}')
        
        stats_query = """
        MATCH (n:Node)
        WHERE n.community IS NOT NULL
        WITH n.community AS community, count(n) AS size
        RETURN 
            count(community) AS total_communities,
            min(size) AS min_size,
            max(size) AS max_size,
            avg(size) AS avg_size
        """
        
        result = session.run(stats_query)
        stats = result.single()
        
        print(f'\n  Statistiche Comunità:')
        print(f'    Totale comunità: {stats["total_communities"]}')
        print(f'    Dimensione minima: {stats["min_size"]}')
        print(f'    Dimensione massima: {stats["max_size"]}')
        print(f'    Dimensione media: {stats["avg_size"]:.2f}')
        
        top_query = """
        MATCH (n:Node)
        WHERE n.community IS NOT NULL
        WITH n.community AS community, count(n) AS size
        RETURN community, size
        ORDER BY size DESC
        LIMIT 10
        """
        
        result = session.run(top_query)
        print(f'\n  Top 10 Comunità più grandi:')
        for i, record in enumerate(result, 1):
            print(f'    {i}. Comunità {record["community"]}: {record["size"]} nodi')

def export_graph_with_communities(driver, output_file='graph_with_communities.csv'):
    print(f'\n--- Esportazione Grafo con Comunità in {output_file} ---')
    
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        print(f'  Directory creata: {output_dir}')
    
    with driver.session() as session:
        query = """
        MATCH (a:Node)-[r:LINKED_TO]->(b:Node)
        WHERE a.community IS NOT NULL AND b.community IS NOT NULL
        RETURN a.id AS src_id, a.community AS src_community, 
               b.id AS dest_id, b.community AS dest_community
        ORDER BY a.id, b.id
        """
        
        result = session.run(query)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['src_id', 'src_community', 'dest_id', 'dest_community'])
            
            count = 0
            for record in result:
                writer.writerow([
                    record['src_id'], 
                    record['src_community'],
                    record['dest_id'], 
                    record['dest_community']
                ])
                count += 1
        
        print(f'  Esportati {count} archi con info sulle comunità in {output_file}')

def cleanup_projection(driver):
    print('\n--- Pulizia ---')
    with driver.session() as session:
        try:
            session.run("CALL gds.graph.drop('wikiGraph') YIELD graphName")
            print('  Proiezione eliminata.')
        except Exception as e:
            print(f'  Nota: {e}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Community Detection su Grafo Neo4j',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  python3 5_community_detection.py --louvain          # Esegue algoritmo Louvain
  python3 5_community_detection.py --leiden           # Esegue algoritmo Leiden (Louvain migliorato)
  python3 5_community_detection.py --lpa              # Esegue algoritmo Label Propagation
  python3 5_community_detection.py --leiden --gamma 1.5 --max-iterations 20
        """
    )
    
    algorithm_group = parser.add_mutually_exclusive_group(required=True)
    algorithm_group.add_argument('--louvain', action='store_true',
                                 help='Usa algoritmo Louvain')
    algorithm_group.add_argument('--leiden', action='store_true',
                                 help='Usa algoritmo Leiden')
    algorithm_group.add_argument('--lpa', action='store_true',
                                 help='Usa algoritmo Label Propagation')
    
    parser.add_argument('--max-iterations', type=int, default=10,
                       help='Numero massimo di iterazioni (default: 10)')
    parser.add_argument('--tolerance', type=float, default=0.0001,
                       help='Tolleranza per algoritmi Louvain/Leiden (default: 0.0001)')
    parser.add_argument('--gamma', type=float, default=1.0,
                       help='Parametro di risoluzione per Leiden (default: 1.0, alto=più comunità)')
    parser.add_argument('--output', type=str, default='communities.csv',
                       help='Nome file output (default: communities.csv)')
    
    args = parser.parse_args()
    
    communities_dir = '../data/communities'
    if args.louvain:
        algorithm_name = 'Louvain'
        output_file = args.output if args.output != 'communities.csv' else os.path.join(communities_dir, 'louvain_communities.csv')
    elif args.leiden:
        algorithm_name = 'Leiden'
        output_file = args.output if args.output != 'communities.csv' else os.path.join(communities_dir, 'leiden_communities.csv')
    else:
        algorithm_name = 'Label Propagation'
        output_file = args.output if args.output != 'communities.csv' else os.path.join(communities_dir, 'lpa_communities.csv')
    
    print(f'--- Community Detection con {algorithm_name} ---')
    print(f'  File output: {output_file}')
    
    try:
        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            is_connected = test_connection(driver)
            
            if not is_connected:
                print('\n--- ERRORE: Impossibile connettersi al database ---')
                sys.exit(1)
            
            print('Test connessione superato.')
            
            node_count, rel_count = get_graph_stats(driver)
            
            if node_count == 0:
                print('\n--- ERRORE: Il grafo è vuoto. Caricare i dati prima ---')
                sys.exit(1)
            
            create_graph_projection(driver)
            
            if args.louvain:
                community_count = run_louvain(driver, 
                                             max_iterations=args.max_iterations, 
                                             tolerance=args.tolerance)
            elif args.leiden:
                community_count = run_leiden(driver,
                                            max_iterations=args.max_iterations,
                                            tolerance=args.tolerance,
                                            gamma=args.gamma)
            else:
                community_count = run_lpa(driver, 
                                         max_iterations=args.max_iterations)
            
            export_communities(driver, output_file)
            
            graph_output = output_file.replace('.csv', '_graph.csv')
            export_graph_with_communities(driver, graph_output)
            
            cleanup_projection(driver)
            
            print(f'\n--- Community detection completata con successo ---')
            print(f'  Algoritmo: {algorithm_name}')
            print(f'  Comunità trovate: {community_count}')
            print(f'  File output:')
            print(f'    - {output_file}')
            print(f'    - {graph_output}')
    
    except Exception as e:
        print(f'\n--- ERRORE: Esecuzione fallita ---')
        print(f'Dettagli errore: {e}')
        sys.exit(1)
