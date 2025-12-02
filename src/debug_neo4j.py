import classifier_utils
import numpy as np
from sentence_transformers import SentenceTransformer

def main():
    print("--- DEBUG NEO4J VECTOR SEARCH ---")
    driver = classifier_utils.get_neo4j_driver()
    if not driver:
        return

    print("Driver connected.")
    
    # Create embedding from empty string
    model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
    vec = model.encode('', convert_to_numpy=True)
    print(f"Empty string vector norm: {np.linalg.norm(vec)}")
    
    print("Querying...")
    try:
        emb, score = classifier_utils.get_trusted_embedding(driver, vec)
        print(f"Success! Score: {score}")
        print(f"Embedding shape: {emb.shape}")
    except Exception as e:
        print(f"Error: {e}")
    
    driver.close()

if __name__ == "__main__":
    main()
