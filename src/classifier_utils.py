import os
import numpy as np
from neo4j import GraphDatabase
from sklearn.metrics.pairwise import cosine_similarity

# Neo4j Config
URI = 'bolt://localhost:7687'
AUTH = ('neo4j', 'password')
INDEX_NAME = "trusted_sources_index"
VECTOR_DIM = 384



def get_neo4j_driver():
    try:
        driver = GraphDatabase.driver(URI, auth=AUTH)
        driver.verify_connectivity()
        return driver
    except Exception as e:
        print(f"❌ Errore connessione Neo4j: {e}")
        return None

def get_trusted_embedding(driver, edit_embedding):
    """
    Recupera l'embedding della fonte affidabile più simile da Neo4j.
    Restituisce: (embedding, score)
    """
    # Check for zero vector (e.g. empty text) to avoid Neo4j error
    if np.all(edit_embedding == 0):
        return np.zeros(VECTOR_DIM), 0.0

    query = f"""
    CALL db.index.vector.queryNodes('{INDEX_NAME}', 1, $embedding)
    YIELD node, score
    RETURN node.embedding AS embedding, score
    """
    
    with driver.session() as session:
        result = session.run(query, {
            "embedding": edit_embedding.tolist()
        })
        record = result.single()
        if record:
            return np.array(record['embedding']), record['score']
        else:
            return np.zeros(VECTOR_DIM), 0.0

def get_features(edit, embedder, driver):
    """
    Genera il vettore di feature per il classificatore.
    Input:
      - edit: dict con 'new_text', 'original_text', 'comment', 'title'
      - embedder: SentenceTransformer model
      - driver: Neo4j driver
    Output:
      - np.array di feature
    """
    
    new_text = edit.get('new_text', '')
    original_text = edit.get('original_text', '')
    comment = edit.get('comment', '')
    
    # 1. Embeddings
    # Usiamo new_text per cercare il contesto, ma calcoliamo anche il delta
    if new_text:
        new_emb = embedder.encode(new_text, convert_to_numpy=True)
    else:
        new_emb = np.zeros(VECTOR_DIM)
        
    if original_text:
        old_emb = embedder.encode(original_text, convert_to_numpy=True)
    else:
        old_emb = np.zeros(VECTOR_DIM)
        
    if comment:
        comment_emb = embedder.encode(comment, convert_to_numpy=True)
    else:
        comment_emb = np.zeros(VECTOR_DIM) # Commento vuoto
        
    # 2. Semantic Delta
    semantic_delta = new_emb - old_emb
    
    # 3. Trusted Context & Truth Score
    # Cerchiamo il contesto basandoci sul NUOVO testo (o titolo?)
    # Meglio usare il testo per trovare la pagina corrispondente o simile.
    # Se l'edit è un vandalismo totale, potrebbe non matchare bene, ma è quello che vogliamo misurare.
    trusted_emb, truth_score = get_trusted_embedding(driver, new_emb)
    
    # 4. Concatenazione
    # [Semantic Delta (384), Comment Intent (384), Truth Score (1)]
    # Totale: 384 + 384 + 1 = 769
    features = np.concatenate([
        semantic_delta,
        comment_emb,
        [truth_score]
    ])
    
    return features
