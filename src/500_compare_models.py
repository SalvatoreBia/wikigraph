import json
from pathlib import Path

# --- CONFIGURAZIONE ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SCORES_DIR = DATA_DIR / "scores"
LLM_RESULTS = SCORES_DIR / "LLM_results.json"
BC_RESULTS = SCORES_DIR / "BC_results.json"

def load_results(filepath):
    if not filepath.exists():
        return None
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)

def main():
    print("--- 📊 CONFRONTO MODELLI (Vandalism Detection) ---")
    
    llm_data = load_results(LLM_RESULTS)
    bc_data = load_results(BC_RESULTS)
    
    if not llm_data:
        print(f"❌ Dati LLM mancanti ({LLM_RESULTS})")
    if not bc_data:
        print(f"❌ Dati BC mancanti ({BC_RESULTS})")
        
    if not llm_data or not bc_data:
        return

    acc_llm = llm_data.get('accuracy', 0)
    time_llm = llm_data.get('avg_time', 0)
    
    acc_bc = bc_data.get('accuracy', 0)
    time_bc = bc_data.get('avg_time', 0)
    
    print("\n" + "="*65)
    print(f"{'MODELLO':<25} | {'ACCURATEZZA':<12} | {'TEMPO MEDIO (s)':<15}")
    print("-" * 65)
    print(f"{'Gemini (AI Judge)':<25} | {acc_llm:>11.2f}% | {time_llm:>14.4f}s")
    print(f"{'Binary Classifier (ML)':<25} | {acc_bc:>11.2f}% | {time_bc:>14.4f}s")
    print("="*65)
    
    diff_acc = acc_llm - acc_bc
    
    print("\n🏆 VERDETTO:")
    if diff_acc > 0:
        print(f"  Gemini è più accurato di {diff_acc:.2f} punti percentuali.")
    elif diff_acc < 0:
        print(f"  Il Classificatore ML è più accurato di {abs(diff_acc):.2f} punti percentuali.")
    else:
        print("  Pareggio perfetto in accuratezza!")
        
    if time_bc < time_llm:
        speedup = time_llm / time_bc if time_bc > 0 else 0
        print(f"  ⚡ Il Classificatore è {speedup:.1f}x più veloce.")
    
    print("\nNota: Assicurati che entrambi abbiano processato lo stesso stream di eventi.")

if __name__ == "__main__":
    main()
