import json
from pathlib import Path

# --- CONFIGURAZIONE ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
ORACLE_RESULTS = DATA_DIR / "oracle_results.json"
CLASSIFIER_RESULTS = DATA_DIR / "classifier_results.json"

def load_results(filepath):
    if not filepath.exists():
        return None
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)

def main():
    print("--- 📊 CONFRONTO MODELLI ---")
    
    oracle_data = load_results(ORACLE_RESULTS)
    classifier_data = load_results(CLASSIFIER_RESULTS)
    
    if not oracle_data:
        print(f"❌ Dati Oracle mancanti ({ORACLE_RESULTS})")
    if not classifier_data:
        print(f"❌ Dati Classifier mancanti ({CLASSIFIER_RESULTS})")
        
    if not oracle_data or not classifier_data:
        return

    acc_oracle = oracle_data['accuracy']
    acc_classifier = classifier_data['accuracy']
    
    print("\n" + "="*40)
    print(f"{'MODELLO':<20} | {'ACCURATEZZA':<10}")
    print("-" * 40)
    print(f"{'Gemini (Oracle)':<20} | {acc_oracle:>9.2f}%")
    print(f"{'Logistic Regression':<20} | {acc_classifier:>9.2f}%")
    print("="*40)
    
    diff = acc_oracle - acc_classifier
    if diff > 0:
        print(f"\n🏆 Gemini vince di {diff:.2f} punti percentuali.")
    elif diff < 0:
        print(f"\n🏆 Il Classificatore ML vince di {abs(diff):.2f} punti percentuali.")
    else:
        print("\n🤝 Pareggio perfetto!")

if __name__ == "__main__":
    main()
