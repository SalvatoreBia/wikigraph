import time
from neo4j import GraphDatabase
from config_loader import load_config

CONFIG = load_config()
URI = CONFIG['neo4j']['uri']
AUTH = tuple(CONFIG['neo4j']['auth'])

WIKI_INDEX_NAME = "wiki_chunk_index"
TRUSTED_INDEX_NAME = "trusted_chunk_index"

def wait_for_connection(uri, auth):
    while True:
        try:
            driver = GraphDatabase.driver(uri, auth=auth)
            driver.verify_connectivity()
            print(f"- Connesso a Neo4j ({uri})")
            return driver
        except Exception as e:
            print(f"- In attesa di Neo4j... ({e})")
            time.sleep(3)

def reset_embeddings(driver):
    print("- Inizio pulizia dei dati degli embedding...")

    with driver.session() as session:
        print("   Eliminazione Wiki Chunks...")
        query_delete_wiki = """
        MATCH (c:Chunk)
        DETACH DELETE c
        """
        result = session.run("MATCH (c:Chunk) RETURN count(c) as count")
        count = result.single()["count"]
        session.run(query_delete_wiki)
        print(f"   [!] Eliminati {count} nodi Wiki Chunk.")

    with driver.session() as session:
        print("   Eliminazione Trusted Chunks...")
        query_delete_trusted = """
        MATCH (c:TrustedChunk)
        DETACH DELETE c
        """
        result = session.run("MATCH (c:TrustedChunk) RETURN count(c) as count")
        count = result.single()["count"]
        session.run(query_delete_trusted)
        print(f"   [!] Eliminati {count} nodi Trusted Chunk.")

    with driver.session() as session:
        print("   Eliminazione Indici Vector...")
        try:
            session.run(f"DROP INDEX {WIKI_INDEX_NAME} IF EXISTS")
            print(f"   - Indice eliminato: {WIKI_INDEX_NAME}")
        except Exception as e:
             print(f"   ! Impossibile eliminare l'indice {WIKI_INDEX_NAME}: {e}")

        try:
            session.run(f"DROP INDEX {TRUSTED_INDEX_NAME} IF EXISTS")
            print(f"   - Indice eliminato: {TRUSTED_INDEX_NAME}")
        except Exception as e:
            print(f"   ! Impossibile eliminare l'indice {TRUSTED_INDEX_NAME}: {e}")
            
    print("- Pulizia completata!")

def main():
    driver = wait_for_connection(URI, AUTH)
    try:
        reset_embeddings(driver)
    finally:
        driver.close()

if __name__ == "__main__":
    main()
