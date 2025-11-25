import json
import os
import random
import re
import time
from pathlib import Path

import google.generativeai as genai
from dotenv import load_dotenv
from google.api_core import exceptions

# --- CONFIGURAZIONE PATH E ENV ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
HTML_DIR = DATA_DIR / "trusted_html_pages"
MOCK_DIR = DATA_DIR / "mocked_edits"
ENV_PATH = BASE_DIR / ".env"

# Carica .env
load_dotenv(dotenv_path=ENV_PATH)
API_KEY = os.getenv("GEMINI_API_KEY")

if not API_KEY:
    raise ValueError("❌ ERRORE: GEMINI_API_KEY non trovata nel file .env")

# Configura Gemini
genai.configure(api_key=API_KEY)

# --- LISTA MODELLI (Priorità) ---
MODELS_PRIORITY = [
    "gemini-3-pro",      
    "gemini-2.5-pro",     
]

HTML_FILENAME = "source_tennis_ao2018.html"
JSON_FILENAME = "dataset_tennis_200.json"

# --- FUNZIONI DI UTILITÀ ---

def clean_json_string(text):
    """Rimuove i backtick del markdown se presenti (es. ```json ... ```)"""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```\w*\n", "", text) # Rimuove la prima riga ```json
        text = re.sub(r"\n```$", "", text)    # Rimuove l'ultima riga ```
    return text.strip()

def call_gemini_robust(prompt, task_name="Generazione"):
    """
    Gestisce la chiamata all'API con logica di Retry e Fallback su modelli diversi.
    """
    max_retries = 3
    base_wait = 10  # Secondi di attesa base

    for attempt in range(max_retries):
        for model_name in MODELS_PRIORITY:
            try:
                print(f"   🤖 Tentativo {attempt+1} con modello: {model_name}...")
                model = genai.GenerativeModel(model_name)
                
                # Configurazione per forzare output JSON dove possibile (sui modelli nuovi)
                generation_config = {"temperature": 0.7}
                if "json" in task_name.lower() and "1.5" in model_name:
                    generation_config["response_mime_type"] = "application/json"

                response = model.generate_content(prompt, generation_config=generation_config)
                return response.text

            except exceptions.ResourceExhausted:
                wait_time = base_wait * (attempt + 1) + random.randint(1, 5)
                print(f"   ⚠️ Quota superata per {model_name}. Attendo {wait_time}s...")
                time.sleep(wait_time)
            except Exception as e:
                print(f"   ❌ Errore generico con {model_name}: {e}")
                # Prova il prossimo modello nella lista
                continue
    
    raise Exception(f"❌ Impossibile completare il task '{task_name}' dopo tutti i tentativi.")

# --- STEP 1: GENERAZIONE HTML ---

def generate_trusted_html():
    print(f"\n--- 📄 STEP 1: Generazione HTML 'Trusted' ({HTML_FILENAME}) ---")
    HTML_DIR.mkdir(parents=True, exist_ok=True)
    
    prompt = """
    Agisci come un giornalista sportivo esperto.
    Genera il codice HTML completo (<body> incluso, ma senza CSS/JS esterni) per un articolo dettagliato sulla finale del **Doppio Misto agli Australian Open 2018**.
    
    FATTI OBBLIGATORI DA INCLUDERE (Ground Truth):
    - Vincitori: Gabriela Dabrowski (Canada) e Mate Pavić (Croazia).
    - Sconfitti: Tímea Babos (Ungheria) e Rohan Bopanna (India).
    - Punteggio esatto: 2-6, 6-4, [11-9].
    - Dettaglio cruciale: La coppia vincente ha salvato un match point nel super tie-break.
    - Luogo: Rod Laver Arena, Melbourne.
    - Data: 28 Gennaio 2018.
    
    L'HTML deve essere ricco di testo discorsivo, suddiviso in paragrafi <p>, liste <ul> e titoli <h2>.
    Non includere markdown, solo codice HTML puro.
    """
    
    html_content = call_gemini_robust(prompt, task_name="HTML Generation")
    
    # Pulizia base se l'LLM ha messo markdown
    if "```html" in html_content:
        html_content = html_content.split("```html")[1].split("```")[0]
    
    output_path = HTML_DIR / HTML_FILENAME
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    
    print(f"✅ HTML salvato in: {output_path}")
    return html_content # Ritorniamo il contenuto per usarlo come contesto nel passo 2

# --- STEP 2: GENERAZIONE EDIT ---

def generate_edits_batch(context_html, is_vandalism, count=100):
    """Genera un batch di edit (o tutti legit o tutti vandalici)"""
    label_type = "VANDALICI" if is_vandalism else "LEGITTIMI"
    label_val = 1 if is_vandalism else 0
    
    print(f"   ⚙️ Generazione batch: {count} edit {label_type}...")

    prompt = f"""
    Ho questo articolo HTML di riferimento (Ground Truth):
    ---
    {context_html[:4000]} ... (contenuto troncato per brevità)
    ---
    
    Il tuo compito è generare un JSON contenente ESATTAMENTE {count} modifiche simulate (edits) fatte da utenti su Wikipedia relative a questi fatti.
    
    TIPO DI MODIFICHE RICHIESTE: **{label_type}**
    
    {"- Devono contraddire i fatti (es. vincitori sbagliati, punteggi inventati), contenere insulti, opinioni personali o spam." if is_vandalism else "- Devono essere coerenti con i fatti dell'articolo, aggiungere dettagli veri, correggere punteggi in modo accurato, riformulare frasi."}
    
    Output atteso (JSON array puro):
    [
        {{ "text": "testo del commento o della modifica", "user": "NomeUtente" }},
        ...
    ]
    
    Usa nomi utenti verosimili (IP, Nickname). Varia la lunghezza dei testi.
    RISPONDI SOLO CON IL JSON.
    """

    response_text = call_gemini_robust(prompt, task_name=f"JSON {label_type}")
    cleaned_json = clean_json_string(response_text)
    
    try:
        data = json.loads(cleaned_json)
        # Aggiungiamo i metadati mancanti post-generazione per sicurezza
        for i, item in enumerate(data):
            item['id'] = i + (1000 if is_vandalism else 0)
            item['label'] = label_val
            item['timestamp'] = int(time.time())
        return data
    except json.JSONDecodeError:
        print(f"   ❌ Errore parsing JSON per {label_type}. Riprovo raw...")
        # Fallback estremo: ritorna lista vuota o riprova (qui semplifico)
        return []

def generate_mock_dataset(html_context):
    print(f"\n--- 💾 STEP 2: Generazione Dataset JSON ({JSON_FILENAME}) ---")
    MOCK_DIR.mkdir(parents=True, exist_ok=True)

    # Dividiamo in due chiamate per evitare limiti di output token e confusione
    legit_edits = generate_edits_batch(html_context, is_vandalism=False, count=50) # Chiediamo 50 per sicurezza
    time.sleep(2) # Respiro per l'API
    vandal_edits = generate_edits_batch(html_context, is_vandalism=True, count=50)
    
    # Se ne vogliamo 100 e 100, potremmo dover fare un loop, ma per ora 50+50 è un buon dataset di test.
    # Uniamo le liste
    all_edits = legit_edits + vandal_edits
    random.shuffle(all_edits) # Mischiamo tutto
    
    # Riassegnamo ID sequenziali
    final_data = []
    for idx, item in enumerate(all_edits):
        item['id'] = idx
        final_data.append(item)
    
    output_path = MOCK_DIR / JSON_FILENAME
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({"edits": final_data}, f, indent=4, ensure_ascii=False)
        
    print(f"✅ Dataset salvato in: {output_path}")
    print(f"   Totale Edits: {len(final_data)} (Legit: {len(legit_edits)}, Vandal: {len(vandal_edits)})")

# --- MAIN ---

if __name__ == "__main__":
    print("🚀 Avvio Generatore Mock con Gemini AI...")
    try:
        # 1. Genera HTML
        html_content = generate_trusted_html()
        
        # 2. Genera JSON usando l'HTML come contesto
        generate_mock_dataset(html_content)
        
        print("\n🎉 TUTTO COMPLETATO CON SUCCESSO.")
        
    except Exception as e:
        print(f"\n❌ ERRORE CRITICO: {e}")