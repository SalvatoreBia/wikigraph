import time
from neo4j import GraphDatabase
from config_loader import load_config

# Load configuration
CONFIG = load_config()
URI = CONFIG['neo4j']['uri']
AUTH = tuple(CONFIG['neo4j']['auth'])

# Index names to drop
WIKI_INDEX_NAME = "wiki_chunk_index"
TRUSTED_INDEX_NAME = "trusted_chunk_index"

def wait_for_connection(uri, auth):
    """Wait for Neo4j to be available."""
    while True:
        try:
            driver = GraphDatabase.driver(uri, auth=auth)
            driver.verify_connectivity()
            print(f"✅ Connected to Neo4j ({uri})")
            return driver
        except Exception as e:
            print(f"⏳ Waiting for Neo4j... ({e})")
            time.sleep(3)

def reset_embeddings(driver):
    """Delete chunks and drop vector indexes."""
    print("🧹 Starting cleanup of embedding data...")

    with driver.session() as session:
        # 1. Delete Wiki Chunks
        print("   Deleting Wiki Chunks...")
        query_delete_wiki = """
        MATCH (c:Chunk)
        DETACH DELETE c
        """
        result = session.run("MATCH (c:Chunk) RETURN count(c) as count")
        count = result.single()["count"]
        session.run(query_delete_wiki)
        print(f"   🗑️  Deleted {count} Wiki Chunk nodes.")

        # 2. Delete Trusted Chunks
        print("   Deleting Trusted Chunks...")
        query_delete_trusted = """
        MATCH (c:TrustedChunk)
        DETACH DELETE c
        """
        result = session.run("MATCH (c:TrustedChunk) RETURN count(c) as count")
        count = result.single()["count"]
        session.run(query_delete_trusted)
        print(f"   🗑️  Deleted {count} Trusted Chunk nodes.")

        # 3. Drop Indices
        print("   Dropping Vector Indices...")
        try:
            session.run(f"DROP INDEX {WIKI_INDEX_NAME} IF EXISTS")
            print(f"   ❌ Dropped index {WIKI_INDEX_NAME}")
        except Exception as e:
             print(f"   ⚠️ Could not drop index {WIKI_INDEX_NAME}: {e}")

        try:
            session.run(f"DROP INDEX {TRUSTED_INDEX_NAME} IF EXISTS")
            print(f"   ❌ Dropped index {TRUSTED_INDEX_NAME}")
        except Exception as e:
            print(f"   ⚠️ Could not drop index {TRUSTED_INDEX_NAME}: {e}")
            
    print("✨ Cleanup complete!")

def main():
    driver = wait_for_connection(URI, AUTH)
    try:
        reset_embeddings(driver)
    finally:
        driver.close()

if __name__ == "__main__":
    main()
