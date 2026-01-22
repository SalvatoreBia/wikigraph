"""
700_compare_models.py
Confronto delle performance tra tutti i modelli di rilevamento vandalismo.
Legge dinamicamente tutti i file BC_results*.json per supportare
versioni future dei modelli.
"""

import json
from pathlib import Path
import glob

# --- CONFIGURAZIONE ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SCORES_DIR = DATA_DIR / "scores"
LLM_RESULTS = SCORES_DIR / "LLM_results.json"

# Mappa dei nomi file ai nomi leggibili
MODEL_NAMES = {
    "LLM_results.json": "GPT OSS 20B (AI Judge)",
    "BC_results.json": "Neural Complete (with RAG)",
    "BC_results_no_rag.json": "Neural No RAG",
    "BC_results_no_comment.json": "Neural No Comment",
    "BC_results_only_new.json": "Neural Only New",
    "BC_results_minimal.json": "Neural Minimal (baseline)"
}

def load_results(filepath):
    if not filepath.exists():
        return None
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)

def get_model_name(filename):
    """Ottiene il nome leggibile del modello dal nome file."""
    return MODEL_NAMES.get(filename, filename.replace("_", " ").replace(".json", "").title())

def main():
    print("--- 📊 CONFRONTO MODELLI (Vandalism Detection) ---")
    
    # Trova tutti i file di risultati
    all_result_files = list(SCORES_DIR.glob("*.json"))
    
    # Costruisci lista dei modelli disponibili
    models = []
    missing = []
    
    # Prima carica LLM
    llm_data = load_results(LLM_RESULTS)
    if llm_data:
        models.append({
            "name": "GPT OSS 20B (AI Judge)",
            "accuracy": llm_data.get('accuracy', 0),
            "time": llm_data.get('avg_time', 0),
            "count": len(llm_data.get('results', [])),
            "file": "LLM_results.json"
        })
    else:
        missing.append(f"LLM ({LLM_RESULTS})")
    
    # Carica tutti i modelli BC_results*.json
    bc_files = sorted(SCORES_DIR.glob("BC_results*.json"))
    
    for bc_file in bc_files:
        data = load_results(bc_file)
        if data:
            models.append({
                "name": get_model_name(bc_file.name),
                "accuracy": data.get('accuracy', 0),
                "time": data.get('avg_time', 0),
                "count": len(data.get('results', [])),
                "file": bc_file.name
            })
        else:
            missing.append(str(bc_file))
    
    if missing:
        print("⚠️  Dati mancanti:")
        for m in missing:
            print(f"   ❌ {m}")
    
    if len(models) < 2:
        print("\n❌ Servono almeno 2 modelli per il confronto!")
        print("   Assicurati di aver trainato i modelli e eseguito i test.")
        return
    
    # Stampa tabella comparativa
    print("\n" + "=" * 90)
    print(f"{'MODELLO':<35} | {'ACCURATEZZA':>12} | {'TEMPO MEDIO':>12} | {'SAMPLES':>8}")
    print("-" * 90)
    
    # Ordina per accuratezza decrescente
    models_sorted = sorted(models, key=lambda x: x['accuracy'], reverse=True)
    
    for i, m in enumerate(models_sorted):
        medal = ""
        if i == 0:
            medal = "🥇 "
        elif i == 1:
            medal = "🥈 "
        elif i == 2:
            medal = "🥉 "
            
        name_display = f"{medal}{m['name']}"
        print(f"{name_display:<35} | {m['accuracy']:>11.2f}% | {m['time']:>11.4f}s | {m['count']:>8}")
    
    print("=" * 90)
    
    # Analisi progressiva: Mostra come degrada la qualità rimuovendo feature
    neural_models = [m for m in models_sorted if "Neural" in m["name"]]
    
    if len(neural_models) >= 2:
        print("\n📉 ANALISI DEGRADAZIONE PROGRESSIVA:")
        print("-" * 60)
        
        # Ordina per nome file (che riflette la progressione)
        neural_by_complexity = sorted(neural_models, 
            key=lambda x: ["BC_results.json", "BC_results_no_rag.json", 
                          "BC_results_no_comment.json", "BC_results_only_new.json",
                          "BC_results_minimal.json"].index(x["file"]) 
                          if x["file"] in ["BC_results.json", "BC_results_no_rag.json", 
                                          "BC_results_no_comment.json", "BC_results_only_new.json",
                                          "BC_results_minimal.json"] else 99)
        
        if len(neural_by_complexity) >= 2:
            best = neural_by_complexity[0]
            for model in neural_by_complexity[1:]:
                diff = best['accuracy'] - model['accuracy']
                if diff > 0:
                    print(f"   {model['name']}: -{diff:.2f}% rispetto a {best['name']}")
                else:
                    print(f"   {model['name']}: +{abs(diff):.2f}% rispetto a {best['name']}")
    
    # Verdetto finale
    print("\n🏆 VERDETTO FINALE:")
    
    # Trova il modello più accurato
    best_acc = max(models, key=lambda x: x['accuracy'])
    print(f"   🎯 Modello più accurato: {best_acc['name']} ({best_acc['accuracy']:.2f}%)")
    
    # Trova il modello più veloce
    fastest = min(models, key=lambda x: x['time'])
    print(f"   ⚡ Modello più veloce: {fastest['name']} ({fastest['time']:.4f}s)")
    
    # Confronto LLM vs ML
    if llm_data:
        neural_models = [m for m in models if "Neural" in m['name']]
        if neural_models:
            best_neural = max(neural_models, key=lambda x: x['accuracy'])
            acc_llm = llm_data.get('accuracy', 0)
            time_llm = llm_data.get('avg_time', 0)
            
            if acc_llm > best_neural['accuracy']:
                diff = acc_llm - best_neural['accuracy']
                print(f"\n   💡 LLM batte il miglior Neural di {diff:.2f} punti in accuratezza")
            else:
                diff = best_neural['accuracy'] - acc_llm
                print(f"\n   💡 Neural batte LLM di {diff:.2f} punti in accuratezza")
            
            # Analisi costo/beneficio
            if time_llm > 0 and fastest['time'] > 0:
                speedup = time_llm / fastest['time']
                if speedup > 1:
                    print(f"   ⏱️  Il modello più veloce è {speedup:.1f}x più rapido di LLM")
    
    # Trova il punto di breakeven (dove conviene usare LLM)
    if llm_data and neural_models:
        llm_acc = llm_data.get('accuracy', 0)
        worse_than_llm = [m for m in models if m['accuracy'] < llm_acc and "Neural" in m['name']]
        
        if worse_than_llm:
            print(f"\n   ⚠️  Modelli peggiori di LLM: {', '.join([m['name'] for m in worse_than_llm])}")
            print("      👉 Per questi casi, conviene usare l'LLM!")
    
    print("\n📝 Nota: Assicurati che tutti i modelli abbiano processato lo stesso stream di eventi.")
    
    # Mostra sample count warning se diversi
    counts = [m['count'] for m in models]
    if len(set(counts)) > 1:
        details = [f"{m['name']}: {m['count']}" for m in models]
        print(f"⚠️  ATTENZIONE: I modelli hanno sample count diversi:")
        for d in details:
            print(f"      {d}")

if __name__ == "__main__":
    main()
