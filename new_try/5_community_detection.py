from neo4j import GraphDatabase
import csv
import time
import argparse
import sys

URI = 'bolt://localhost:7687'
AUTH = ('neo4j', 'password')


def test_connection(driver):
    try:
        driver.verify_connectivity()
        print(f"Connection to {URI} successful.")
        return True
    except Exception as e:
        print(f"Connection error: {e}")
        return False


def get_graph_stats(driver):
    print('\n--- Graph Statistics ---')
    with driver.session() as session:
        result = session.run("MATCH (n:Node) RETURN count(n) AS count")
        node_count = result.single()["count"]
        print(f'  Total nodes: {node_count}')
        
        result = session.run("MATCH ()-[r:LINKED_TO]->() RETURN count(r) AS count")
        rel_count = result.single()["count"]
        print(f'  Total relationships: {rel_count}')
        
        return node_count, rel_count


def create_graph_projection(driver):
    print('\n--- Creating Graph Projection ---')
    
    with driver.session() as session:
        check_query = """
        CALL gds.graph.exists('wikiGraph')
        YIELD exists
        RETURN exists
        """
        
        result = session.run(check_query)
        exists = result.single()["exists"]
        
        if exists:
            print('  Projection already exists. Dropping old projection...')
            session.run("CALL gds.graph.drop('wikiGraph')")
            print('  Old projection dropped.')
        
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
        
        print('  Creating projection...')
        result = session.run(projection_query)
        record = result.single()
        
        print(f'  Projection created: {record["graphName"]}')
        print(f'    Nodes: {record["nodeCount"]}')
        print(f'    Relationships: {record["relationshipCount"]}')
        
        return True


def run_louvain(driver, max_iterations=10, tolerance=0.0001):
    print('\n--- Running Louvain Algorithm ---')
    print(f'  Parameters:')
    print(f'    Max iterations: {max_iterations}')
    print(f'    Tolerance: {tolerance}')
    
    with driver.session() as session:
        stats_query = """
        CALL gds.louvain.stats('wikiGraph', {
            maxIterations: $maxIterations,
            tolerance: $tolerance
        })
        YIELD communityCount, modularity, modularities
        RETURN communityCount, modularity, modularities
        """
        
        print('\n  Computing statistics...')
        start_time = time.time()
        result = session.run(stats_query, maxIterations=max_iterations, tolerance=tolerance)
        record = result.single()
        stats_time = time.time() - start_time
        
        print(f'\n  Statistics computed in {stats_time:.2f} seconds:')
        print(f'    Community count: {record["communityCount"]}')
        print(f'    Modularity: {record["modularity"]:.4f}')
        print(f'    Iterations: {len(record["modularities"])}')
        
        write_query = """
        CALL gds.louvain.write('wikiGraph', {
            writeProperty: 'community',
            maxIterations: $maxIterations,
            tolerance: $tolerance
        })
        YIELD communityCount, modularity, nodePropertiesWritten
        RETURN communityCount, modularity, nodePropertiesWritten
        """
        
        print('\n  Writing results to graph...')
        start_time = time.time()
        result = session.run(write_query, maxIterations=max_iterations, tolerance=tolerance)
        record = result.single()
        write_time = time.time() - start_time
        
        print(f'  Results written in {write_time:.2f} seconds')
        print(f'    Properties written: {record["nodePropertiesWritten"]}')
        
        return record['communityCount']


def run_leiden(driver, max_iterations=10, tolerance=0.0001, gamma=1.0):
    print('\n--- Running Leiden Algorithm ---')
    print(f'  Parameters:')
    print(f'    Max iterations: {max_iterations}')
    print(f'    Tolerance: {tolerance}')
    print(f'    Gamma (resolution): {gamma}')
    
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
        
        print('\n  Computing statistics...')
        start_time = time.time()
        result = session.run(stats_query, maxIterations=max_iterations, tolerance=tolerance, gamma=gamma)
        record = result.single()
        stats_time = time.time() - start_time
        
        print(f'\n  Statistics computed in {stats_time:.2f} seconds:')
        print(f'    Community count: {record["communityCount"]}')
        print(f'    Modularity: {record["modularity"]:.4f}')
        print(f'    Levels: {len(record["modularities"])}')
        
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
        
        print('\n  Writing results to graph...')
        start_time = time.time()
        result = session.run(write_query, maxIterations=max_iterations, tolerance=tolerance, gamma=gamma)
        record = result.single()
        write_time = time.time() - start_time
        
        print(f'  Results written in {write_time:.2f} seconds')
        print(f'    Properties written: {record["nodePropertiesWritten"]}')
        
        return record['communityCount']


def run_lpa(driver, max_iterations=10):
    print('\n--- Running Label Propagation Algorithm ---')
    print(f'  Parameters:')
    print(f'    Max iterations: {max_iterations}')
    
    with driver.session() as session:
        stats_query = """
        CALL gds.labelPropagation.stats('wikiGraph', {
            maxIterations: $maxIterations
        })
        YIELD communityCount, ranIterations, didConverge
        RETURN communityCount, ranIterations, didConverge
        """
        
        print('\n  Computing statistics...')
        start_time = time.time()
        result = session.run(stats_query, maxIterations=max_iterations)
        record = result.single()
        stats_time = time.time() - start_time
        
        print(f'\n  Statistics computed in {stats_time:.2f} seconds:')
        print(f'    Community count: {record["communityCount"]}')
        print(f'    Iterations: {record["ranIterations"]}')
        print(f'    Converged: {record["didConverge"]}')
        
        write_query = """
        CALL gds.labelPropagation.write('wikiGraph', {
            writeProperty: 'community',
            maxIterations: $maxIterations
        })
        YIELD communityCount, ranIterations, didConverge, nodePropertiesWritten
        RETURN communityCount, ranIterations, didConverge, nodePropertiesWritten
        """
        
        print('\n  Writing results to graph...')
        start_time = time.time()
        result = session.run(write_query, maxIterations=max_iterations)
        record = result.single()
        write_time = time.time() - start_time
        
        print(f'  Results written in {write_time:.2f} seconds')
        print(f'    Properties written: {record["nodePropertiesWritten"]}')
        
        return record['communityCount']


def export_communities(driver, output_file):
    print(f'\n--- Exporting Communities to {output_file} ---')
    
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
        
        print(f'  Exported {count} nodes to {output_file}')
        
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
        
        print(f'\n  Community Statistics:')
        print(f'    Total communities: {stats["total_communities"]}')
        print(f'    Min size: {stats["min_size"]}')
        print(f'    Max size: {stats["max_size"]}')
        print(f'    Avg size: {stats["avg_size"]:.2f}')
        
        top_query = """
        MATCH (n:Node)
        WHERE n.community IS NOT NULL
        WITH n.community AS community, count(n) AS size
        RETURN community, size
        ORDER BY size DESC
        LIMIT 10
        """
        
        result = session.run(top_query)
        print(f'\n  Top 10 Largest Communities:')
        for i, record in enumerate(result, 1):
            print(f'    {i}. Community {record["community"]}: {record["size"]} nodes')


def export_graph_with_communities(driver, output_file='graph_with_communities.csv'):
    print(f'\n--- Exporting Graph with Communities to {output_file} ---')
    
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
        
        print(f'  Exported {count} edges with community information to {output_file}')


def cleanup_projection(driver):
    print('\n--- Cleanup ---')
    with driver.session() as session:
        try:
            session.run("CALL gds.graph.drop('wikiGraph')")
            print('  Projection dropped.')
        except Exception as e:
            print(f'  Note: {e}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Community Detection on Neo4j Graph',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 4_community_detection.py --louvain          # Run Louvain algorithm
  python3 4_community_detection.py --leiden           # Run Leiden algorithm (improved Louvain)
  python3 4_community_detection.py --lpa              # Run Label Propagation algorithm
  python3 4_community_detection.py --leiden --gamma 1.5 --max-iterations 20
        """
    )
    
    algorithm_group = parser.add_mutually_exclusive_group(required=True)
    algorithm_group.add_argument('--louvain', action='store_true',
                                 help='Use Louvain algorithm for community detection')
    algorithm_group.add_argument('--leiden', action='store_true',
                                 help='Use Leiden algorithm for community detection (improved Louvain)')
    algorithm_group.add_argument('--lpa', action='store_true',
                                 help='Use Label Propagation Algorithm for community detection')
    
    parser.add_argument('--max-iterations', type=int, default=10,
                       help='Maximum number of iterations (default: 10)')
    parser.add_argument('--tolerance', type=float, default=0.0001,
                       help='Tolerance for Louvain/Leiden algorithm (default: 0.0001)')
    parser.add_argument('--gamma', type=float, default=1.0,
                       help='Resolution parameter for Leiden algorithm (default: 1.0, higher=more communities)')
    parser.add_argument('--output', type=str, default='communities.csv',
                       help='Output file name (default: communities.csv)')
    
    args = parser.parse_args()
    
    # Determine algorithm name and output file
    if args.louvain:
        algorithm_name = 'Louvain'
        output_file = args.output if args.output != 'communities.csv' else 'louvain_communities.csv'
    elif args.leiden:
        algorithm_name = 'Leiden'
        output_file = args.output if args.output != 'communities.csv' else 'leiden_communities.csv'
    else:
        algorithm_name = 'Label Propagation'
        output_file = args.output if args.output != 'communities.csv' else 'lpa_communities.csv'
    
    print(f'--- Community Detection with {algorithm_name} ---')
    print(f'  Output file: {output_file}')
    
    try:
        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            is_connected = test_connection(driver)
            
            if not is_connected:
                print('\n--- ERROR: Unable to connect to database ---')
                sys.exit(1)
            
            print('Connection test passed.')
            
            node_count, rel_count = get_graph_stats(driver)
            
            if node_count == 0:
                print('\n--- ERROR: Graph is empty. Load data first ---')
                sys.exit(1)
            
            create_graph_projection(driver)
            
            # Run selected algorithm
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
            
            print(f'\n--- Community detection completed successfully ---')
            print(f'  Algorithm: {algorithm_name}')
            print(f'  Communities found: {community_count}')
            print(f'  Output files:')
            print(f'    - {output_file}')
            print(f'    - {graph_output}')
    
    except Exception as e:
        print(f'\n--- ERROR: Execution failed ---')
        print(f'Error details: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)
