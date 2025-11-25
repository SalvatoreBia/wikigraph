import os
import pickle
import sys
from pathlib import Path
from bs4 import BeautifulSoup
from sentence_transformers import SentenceTransformer
import numpy as np

# --- CONFIGURAZIONE ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
HTML_DIR = DATA_DIR / "trusted_html_pages"
INDEX_FILE = DATA_DIR / "trusted_sources_index.pkl"

MODEL_NAME = 'paraphrase-multilingual-MiniLM-L12-v2'
CHUNK_SIZE = 500  # Caratteri per chunk
OVERLAP = 50      # Sovrapposizione

def load_html_files(directory):
    """Carica tutti i file HTML dalla directory e ne estrae il testo."""
    documents = []
    if not directory.exists():
        print(f"❌ Directory non trovata: {directory}")
        return []

    print(f"📂 Leggo file HTML da: {directory}")
    for file_path in directory.glob("*.html"):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                soup = BeautifulSoup(f.read(), "html.parser")
                text = soup.get_text(separator=" ", strip=True)
                documents.append({
                    "filename": file_path.name,
                    "text": text
                })
                print(f"  - Caricato: {file_path.name} ({len(text)} chars)")
        except Exception as e:
            print(f"⚠️ Errore lettura {file_path.name}: {e}")
    
    return documents

def chunk_text(text, chunk_size=CHUNK_SIZE, overlap=OVERLAP):
    """Divide il testo in chunk sovrapposti."""
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        chunks.append(chunk)
        start += (chunk_size - overlap)
    return chunks

def create_index():
    print("--- 🏗️ CREAZIONE INDICE RAG ---")
    
    # 1. Carica Documenti
    docs = load_html_files(HTML_DIR)
    if not docs:
        print("❌ Nessun documento trovato. Genera prima i mock con 10_generate_mocks.py")
        return

    # 2. Chunking
    all_chunks = []
    chunk_metadata = [] # Tiene traccia da quale file viene il chunk
    
    print(f"\n✂️  Chunking (Size: {CHUNK_SIZE}, Overlap: {OVERLAP})...")
    for doc in docs:
        chunks = chunk_text(doc['text'])
        for chunk in chunks:
            all_chunks.append(chunk)
            chunk_metadata.append({
                "filename": doc['filename'],
                "text": chunk
            })
    
    print(f"✅ Generati {len(all_chunks)} chunks totali.")

    # 3. Embedding
    print(f"\n🧠 Caricamento Modello: {MODEL_NAME}...")
    model = SentenceTransformer(MODEL_NAME)
    
    print("🚀 Calcolo Embeddings...")
    embeddings = model.encode(all_chunks, show_progress_bar=True, convert_to_numpy=True)
    
    # 4. Salvataggio
    data = {
        "embeddings": embeddings,
        "metadata": chunk_metadata
    }
    
    with open(INDEX_FILE, "wb") as f:
        pickle.dump(data, f)
        
    print(f"\n💾 Indice salvato in: {INDEX_FILE}")
    print("--- FINE ---")

if __name__ == "__main__":
    create_index()
