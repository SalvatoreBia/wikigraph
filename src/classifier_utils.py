import os
import numpy as np
from neo4j import GraphDatabase
from sklearn.metrics.pairwise import cosine_similarity
from config_loader import load_config

CONFIG = load_config()

# Neo4j Config
URI = 'bolt://localhost:7687'
AUTH = ('neo4j', 'password')
INDEX_NAME = "trusted_sources_index"
VECTOR_DIM = CONFIG['embedding']['dimension']



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
        comment_emb = np.zeros(VECTOR_DIM)
        
    # 2. Semantic Delta (differenza vettoriale)
    semantic_delta = new_emb - old_emb
    
    # 3. Cosine Similarity (quanto sono simili old e new)
    # Score alto = testi simili (es. sinonimi) = probabilmente LEGITTIMO
    # Score basso = testi diversi = potenziale VANDALISMO
    if np.all(old_emb == 0) or np.all(new_emb == 0):
        text_similarity = 0.0
    else:
        text_similarity = cosine_similarity([old_emb], [new_emb])[0][0]
    
    # 4. Length ratio (rapporto lunghezze)
    # Cancellazioni totali = ratio basso = vandalismo
    old_len = len(original_text)
    new_len = len(new_text)
    if old_len > 0:
        length_ratio = new_len / old_len
    else:
        length_ratio = 1.0 if new_len == 0 else 10.0  # Aggiunta da vuoto
    
    # 5. Trusted Context & Truth Score
    trusted_emb, truth_score = get_trusted_embedding(driver, new_emb)
    
    # 6. Truth Similarity OLD vs Trusted (il testo originale era affidabile?)
    if np.all(old_emb == 0):
        old_truth_score = 0.0
    else:
        _, old_truth_score = get_trusted_embedding(driver, old_emb)
    
    # 7. Concatenazione delle feature
    # [Semantic Delta (384), Comment Emb (384), 
    #  Text Similarity (1), Length Ratio (1), 
    #  Truth Score New (1), Truth Score Old (1)]
    # Totale: 384 + 384 + 4 = 772
    features = np.concatenate([
        semantic_delta,      # 384
        comment_emb,         # 384
        [text_similarity],   # 1 - NUOVA: robusta ai sinonimi
        [length_ratio],      # 1 - NUOVA: rileva cancellazioni
        [truth_score],       # 1 - Somiglianza new con trusted
        [old_truth_score]    # 1 - NUOVA: Somiglianza old con trusted
    ])
    
    return features
