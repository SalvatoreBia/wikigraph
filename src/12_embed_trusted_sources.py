import os
import sys
import csv
import time
import re
from pathlib import Path
from sentence_transformers import SentenceTransformer
from neo4j import GraphDatabase
import numpy as np

from config_loader import load_config

CONFIG = load_config()

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CSV_FILE = DATA_DIR / "sample_content" / "sample_with_names_1_content.csv"
HTML_DIR = DATA_DIR / "trusted_html_pages"

URI = CONFIG['neo4j']['uri']
AUTH = tuple(CONFIG['neo4j']['auth'])

WIKI_INDEX_NAME = "wiki_chunk_index"
TRUSTED_INDEX_NAME = "trusted_chunk_index"

VECTOR_DIM = CONFIG['embedding']['dimension']
MODEL_NAME = CONFIG['embedding']['model_name']
BATCH_SIZE = CONFIG['processing']['batch_size']
CHUNK_SIZE = CONFIG['processing']['chunk_size']
CHUNK_OVERLAP = CONFIG['processing']['chunk_overlap']
TEXT_LIMIT = CONFIG['processing']['text_limit']

def chunk_text(text, chunk_size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    if not text:
        return []
    
    text = text[:TEXT_LIMIT]
    
    chunks = []
    start = 0
    text_len = len(text)
    
    while start < text_len:
        end = min(start + chunk_size, text_len)
        chunks.append(text[start:end])
        
        if end == text_len:
            break
            
        start += (chunk_size - overlap)
        
    return chunks

def clean_html(html_content):
    cleaned = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', html_content, flags=re.DOTALL)
    cleaned = re.sub(r'<!--.*?-->', '', cleaned, flags=re.DOTALL)
    cleaned = re.sub(r'<[^>]+>', ' ', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned

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

def get_processed_ids(driver):
    print("- Verifica documenti già processati...")
    query = """
    MATCH (n:Node)-[:HAS_CHUNK]->() RETURN n.id as id
    UNION
    MATCH (s:TrustedSource)-[:HAS_TRUSTED_CHUNK]->() RETURN s.id as id
    """
    processed_ids = set()
    with driver.session() as session:
        result = session.run(query)
        for record in result:
            processed_ids.add(str(record["id"]))
            
    print(f"- Trovati {len(processed_ids)} documenti già processati.")
    return processed_ids

def create_vector_indexes(driver):
    print(f"- Verifica/Creazione Indici Vettoriali...")
    
    indices_to_create = [
        (WIKI_INDEX_NAME, "Chunk"),
        (TRUSTED_INDEX_NAME, "TrustedChunk")
    ]
    
    with driver.session() as session:
        existing_indexes = session.run("SHOW INDEXES").data()
        existing_names = [idx['name'] for idx in existing_indexes]
        
        for index_name, label in indices_to_create:
            if index_name not in existing_names:
                print(f"   Creazione indice {index_name} su :{label}...")
                query = f"""
                CREATE VECTOR INDEX {index_name} IF NOT EXISTS
                FOR (n:{label}) ON (n.embedding)
                OPTIONS {{indexConfig: {{
                    `vector.dimensions`: {VECTOR_DIM},
                    `vector.similarity_function`: 'cosine'
                }}}}
                """
                try:
                    session.run(query)
                    print(f"   - Indice {index_name} creato.")
                except Exception as e:
                    print(f"   ! Errore creazione indice {index_name}: {e}")
            else:
                print(f"   - Indice {index_name} già esistente.")

def load_csv_documents(filepath, processed_ids):
    print(f"- Leggo CSV Wikipedia: {filepath}")
    if not filepath.exists():
        print(f"! File CSV non trovato: {filepath}")
        return []

    documents = []
    try:
        csv.field_size_limit(sys.maxsize)
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                page_id = row.get('page_id')
                
                if page_id and str(page_id) in processed_ids:
                    continue

                content = row.get('content', '')
                title = row.get('title', 'Sconosciuto')
                
                if content and page_id:
                    documents.append({
                        "id": page_id,
                        "title": title,
                        "text": content,
                        "type": "wiki"
                    })
    except Exception as e:
        print(f"! Errore lettura CSV: {e}")
    return documents

def load_trusted_documents(directory, processed_ids):
    print(f"- Leggo HTML Trusted Sources da: {directory}")
    if not directory.exists():
        print(f"! Directory non trovata: {directory}")
        return []
    
    documents = []
    for fpath in directory.glob("*.html"):
        doc_id = fpath.stem
        
        if str(doc_id) in processed_ids:
            continue

        try:
            with open(fpath, "r", encoding="utf-8") as f:
                raw_html = f.read()
                clean_text = clean_html(raw_html)
                
                if clean_text:
                    documents.append({
                        "id": doc_id,
                        "title": doc_id.replace("trusted_", "").replace("_", " "),
                        "text": clean_text,
                        "type": "trusted"
                    })
        except Exception as e:
            print(f"! Errore lettura {fpath.name}: {e}")
            
    return documents

def process_and_embed(driver, model, documents, batch_size=BATCH_SIZE):
    total_chunks = 0
    
    with driver.session() as session:
        for i in range(0, len(documents), batch_size):
            batch_docs = documents[i : i + batch_size]
            
            all_chunks_text = []
            batch_metadata = []
            
            for doc in batch_docs:
                doc_chunks = chunk_text(doc['text'])
                
                for idx, chunk_text_content in enumerate(doc_chunks):
                    all_chunks_text.append(chunk_text_content)
                    batch_metadata.append({
                        "parent_id": doc['id'],
                        "parent_title": doc['title'],
                        "chunk_id": f"{doc['id']}_{idx}",
                        "chunk_index": idx,
                        "type": doc['type'],
                        "text": chunk_text_content
                    })
            
            if not all_chunks_text:
                continue
                
            print(f"   Embed batch {i//batch_size + 1}... ({len(all_chunks_text)} chunks)")
            embeddings = model.encode(all_chunks_text).tolist()
            
            wiki_params = []
            trusted_params = []
            
            for j, meta in enumerate(batch_metadata):
                meta['embedding'] = embeddings[j]
                if meta['type'] == 'wiki':
                    wiki_params.append(meta)
                else:
                    trusted_params.append(meta)
            
            if wiki_params:
                query_wiki = """
                UNWIND $batch AS row
                MERGE (p:Node {id: row.parent_id})
                ON CREATE SET p.title = row.parent_title, p.full_content = row.text 
                
                MERGE (c:Chunk {id: row.chunk_id})
                SET c.text = row.text,
                    c.embedding = row.embedding,
                    c.index = row.chunk_index
                
                MERGE (p)-[:HAS_CHUNK]->(c)
                """
                session.run(query_wiki, {"batch": wiki_params})
            
            if trusted_params:
                query_trusted = """
                UNWIND $batch AS row
                MERGE (s:TrustedSource {id: row.parent_id})
                SET s.title = row.parent_title
                
                MERGE (c:TrustedChunk {id: row.chunk_id})
                SET c.text = row.text,
                    c.embedding = row.embedding,
                    c.index = row.chunk_index
                
                MERGE (s)-[:HAS_TRUSTED_CHUNK]->(c)
                """
                session.run(query_trusted, {"batch": trusted_params})
                
            total_chunks += len(all_chunks_text)
            print(f"   Salvati {len(all_chunks_text)} chunks su Neo4j.")
            
    return total_chunks

def main():
    print("--- PIPELINE RAG: CHUNKING & EMBEDDINGS ---")
    
    driver = wait_for_connection(URI, AUTH)
    
    processed_ids = get_processed_ids(driver)
    
    create_vector_indexes(driver)
    
    print(f"- Caricamento Modello: {MODEL_NAME}...")
    model = SentenceTransformer(MODEL_NAME)
    
    print("\n--- PROCESSAMENTO WIKIPEDIA ---")
    wiki_docs = load_csv_documents(CSV_FILE, processed_ids)
    if wiki_docs:
        print(f"- Trovati {len(wiki_docs)} nuovi articoli Wikipedia da processare.")
        chunks_wiki = process_and_embed(driver, model, wiki_docs)
        print(f"\n- Wikipedia completata: {chunks_wiki} chunks totali.")
    else:
        print("- Tutti gli articoli Wikipedia sono già processati.")
    
    print("\n--- PROCESSAMENTO TRUSTED SOURCES ---")
    trusted_docs = load_trusted_documents(HTML_DIR, processed_ids)
    if trusted_docs:
        print(f"- Trovate {len(trusted_docs)} nuove fonti affidabili.")
        chunks_trusted = process_and_embed(driver, model, trusted_docs)
        print(f"\n- Trusted Sources completate: {chunks_trusted} chunks totali.")
    else:
        print("- Tutte le fonti affidabili sono già processate.")
    
    driver.close()
    print("\n- PIPELINE RAG COMPLETATA CON SUCCESSO.")

if __name__ == "__main__":
    main()
