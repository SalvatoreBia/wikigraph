import os
import numpy as np
from neo4j import GraphDatabase
from sklearn.metrics.pairwise import cosine_similarity
from config_loader import load_config

CONFIG = load_config()

URI = CONFIG['neo4j']['uri']
AUTH = tuple(CONFIG['neo4j']['auth'])
VECTOR_DIM = CONFIG['embedding']['dimension']

WIKI_INDEX_NAME = "wiki_chunk_index"
TRUSTED_INDEX_NAME = "trusted_chunk_index"

def get_neo4j_driver():
    try:
        driver = GraphDatabase.driver(URI, auth=AUTH)
        driver.verify_connectivity()
        return driver
    except Exception as e:
        print(f"! Errore connessione Neo4j: {e}")
        return None

def get_best_match(driver, index_name, embedding):
    if np.all(embedding == 0):
        return np.zeros(VECTOR_DIM), 0.0

    query = f"""
    CALL db.index.vector.queryNodes('{index_name}', 1, $embedding)
    YIELD node, score
    RETURN node.embedding AS embedding, score
    """
    
    with driver.session() as session:
        try:
            result = session.run(query, {"embedding": embedding.tolist()})
            record = result.single()
            if record:
                return np.array(record['embedding']), record['score']
        except Exception as e:
            pass
            
    return np.zeros(VECTOR_DIM), 0.0

def get_features(edit, embedder, driver):
    new_text = edit.get('new_text', '')
    original_text = edit.get('original_text', '')
    comment = edit.get('comment', '')
    
    new_emb = embedder.encode(new_text, convert_to_numpy=True) if new_text else np.zeros(VECTOR_DIM)
    old_emb = embedder.encode(original_text, convert_to_numpy=True) if original_text else np.zeros(VECTOR_DIM)
    comment_emb = embedder.encode(comment, convert_to_numpy=True) if comment else np.zeros(VECTOR_DIM)
        
    semantic_delta = new_emb - old_emb
    
    if np.all(old_emb == 0) or np.all(new_emb == 0):
        text_similarity = 0.0
    else:
        text_similarity = cosine_similarity([old_emb], [new_emb])[0][0]
    
    old_len = len(original_text)
    new_len = len(new_text)
    if old_len > 0:
        length_ratio = new_len / old_len
    else:
        length_ratio = 1.0 if new_len == 0 else 10.0
    
    _, score_new_wiki = get_best_match(driver, WIKI_INDEX_NAME, new_emb)
    _, score_new_trusted = get_best_match(driver, TRUSTED_INDEX_NAME, new_emb)
    
    _, score_old_wiki = get_best_match(driver, WIKI_INDEX_NAME, old_emb)
    _, score_old_trusted = get_best_match(driver, TRUSTED_INDEX_NAME, old_emb)
    
    features = np.concatenate([
        semantic_delta,      
        comment_emb,         
        [text_similarity],   
        [length_ratio],      
        [score_new_wiki],    
        [score_new_trusted], 
        [score_old_wiki],    
        [score_old_trusted]  
    ])
    
    return features

