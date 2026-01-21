import json
from pathlib import Path

# --- CONFIGURAZIONE ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SCORES_DIR = DATA_DIR / "scores"
LLM_RESULTS = SCORES_DIR / "LLM_results.json"
BC_RESULTS = SCORES_DIR / "BC_results.json"
BC_NO_RAG_RESULTS = SCORES_DIR / "BC_results_no_rag.json"

def load_results(filepath):
    if not filepath.exists():
        return None
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)

def main():
    print("--- 📊 CONFRONTO MODELLI (Vandalism Detection) ---")
    
    llm_data = load_results(LLM_RESULTS)
    bc_data = load_results(BC_RESULTS)
    bc_no_rag_data = load_results(BC_NO_RAG_RESULTS)
    
    # Verifica disponibilità dati
    missing = []
    if not llm_data:
        missing.append(f"LLM ({LLM_RESULTS})")
    if not bc_data:
        missing.append(f"BC with RAG ({BC_RESULTS})")
    if not bc_no_rag_data:
        missing.append(f"BC without RAG ({BC_NO_RAG_RESULTS})")
    
    if missing:
        print("⚠️  Dati mancanti:")
        for m in missing:
            print(f"   ❌ {m}")
    
    # Costruisci lista dei modelli disponibili
    models = []
    
    if llm_data:
        models.append({
            "name": "Gemini (AI Judge)",
            "accuracy": llm_data.get('accuracy', 0),
            "time": llm_data.get('avg_time', 0),
            "count": len(llm_data.get('results', []))
        })
    
    if bc_data:
        models.append({
            "name": "Neural Classifier (with RAG)",
            "accuracy": bc_data.get('accuracy', 0),
            "time": bc_data.get('avg_time', 0),
            "count": len(bc_data.get('results', []))
        })
    
    if bc_no_rag_data:
        models.append({
            "name": "Neural Classifier (no RAG)",
            "accuracy": bc_no_rag_data.get('accuracy', 0),
            "time": bc_no_rag_data.get('avg_time', 0),
            "count": len(bc_no_rag_data.get('results', []))
        })
    
    if len(models) < 2:
        print("\n❌ Servono almeno 2 modelli per il confronto!")
        return
    
    # Stampa tabella comparativa
    print("\n" + "="*80)
    print(f"{'MODELLO':<32} | {'ACCURATEZZA':>12} | {'TEMPO MEDIO':>12} | {'SAMPLES':>8}")
    print("-" * 80)
    
    for m in models:
        print(f"{m['name']:<32} | {m['accuracy']:>11.2f}% | {m['time']:>11.4f}s | {m['count']:>8}")
    
    print("="*80)
    
    # Analisi confronto RAG vs No RAG (se entrambi disponibili)
    if bc_data and bc_no_rag_data:
        acc_rag = bc_data.get('accuracy', 0)
        acc_no_rag = bc_no_rag_data.get('accuracy', 0)
        time_rag = bc_data.get('avg_time', 0)
        time_no_rag = bc_no_rag_data.get('avg_time', 0)
        
        print("\n📈 CONFRONTO RAG vs NO RAG:")
        diff_rag = acc_rag - acc_no_rag
        if diff_rag > 0:
            print(f"   ✅ RAG migliora l'accuratezza di {diff_rag:.2f} punti percentuali")
        elif diff_rag < 0:
            print(f"   ⚠️  NO RAG è più accurato di {abs(diff_rag):.2f} punti percentuali")
        else:
            print("   🔄 Stessa accuratezza")
        
        if time_no_rag < time_rag and time_no_rag > 0:
            speedup = time_rag / time_no_rag
            print(f"   ⚡ NO RAG è {speedup:.2f}x più veloce")
        elif time_rag < time_no_rag and time_rag > 0:
            speedup = time_no_rag / time_rag
            print(f"   🐢 RAG è {speedup:.2f}x più veloce")
    
    # Verdetto finale
    print("\n🏆 VERDETTO FINALE:")
    
    # Trova il modello più accurato
    best_acc = max(models, key=lambda x: x['accuracy'])
    print(f"   🎯 Modello più accurato: {best_acc['name']} ({best_acc['accuracy']:.2f}%)")
    
    # Trova il modello più veloce
    fastest = min(models, key=lambda x: x['time'])
    print(f"   ⚡ Modello più veloce: {fastest['name']} ({fastest['time']:.4f}s)")
    
    # Confronto LLM vs ML (se disponibili)
    if llm_data and (bc_data or bc_no_rag_data):
        acc_llm = llm_data.get('accuracy', 0)
        time_llm = llm_data.get('avg_time', 0)
        
        best_ml_acc = max(
            [m for m in models if "Neural" in m['name']],
            key=lambda x: x['accuracy']
        )
        
        if acc_llm > best_ml_acc['accuracy']:
            diff = acc_llm - best_ml_acc['accuracy']
            print(f"\n   📊 LLM batte il miglior ML di {diff:.2f} punti in accuratezza")
        else:
            diff = best_ml_acc['accuracy'] - acc_llm
            print(f"\n   📊 ML batte LLM di {diff:.2f} punti in accuratezza")
        
        if time_llm > 0 and fastest['time'] > 0:
            if "Neural" in fastest['name']:
                speedup = time_llm / fastest['time']
                print(f"   ⚡ ML più veloce è {speedup:.1f}x più rapido di LLM")
    
    print("\n📝 Nota: Assicurati che tutti i modelli abbiano processato lo stesso stream di eventi.")
    
    # Mostra sample count warning se diversi
    counts = [m['count'] for m in models]
    if len(set(counts)) > 1:
        details = [f"{m['name']}: {m['count']}" for m in models]
        print(f"⚠️  ATTENZIONE: I modelli hanno sample count diversi: {details}")

if __name__ == "__main__":
    main()
