import json
import os
from pathlib import Path

import google.generativeai as genai
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from kafka import KafkaConsumer

# --- CARICAMENTO CONFIGURAZIONE ---
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
KAFKA_BROKER = 'localhost:9092'
TOPIC_IN = 'to-be-judged'
SOURCE_FILE = '../data/web_source_tennis.html'
GEMINI_MODEL = 'gemini-2.5-flash' 

def load_ground_truth():
    if not os.path.exists(SOURCE_FILE):
        return "Nessuna fonte trovata."
    with open(SOURCE_FILE, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')
        return soup.get_text(separator=' ', strip=True)

def analyze_with_gemini(edit_comment, context):
    if not GEMINI_API_KEY:
        return "ERRORE: API KEY MANCANTE"

    model = genai.GenerativeModel(GEMINI_MODEL)
    
    prompt = f"""
    Sei un moderatore di Wikipedia esperto.
    
    CONTESTO REALE (Fonte Ufficiale Verificata): 
    "{context}"
    
    EVENTO DA ANALIZZARE (Commento dell'Edit Utente): 
    "{edit_comment}"
    
    TASK:
    Confronta il commento dell'utente con la fonte ufficiale.
    - Se il commento è coerente con i fatti (es. riporta la squalifica o l'innocenza della Dabrowski corretta), è LEGITTIMO.
    - Se il commento contraddice i fatti (es. accusa chi è stato assolto, nega la squalifica, insulta), è VANDALISMO.
    
    Rispondi SOLO con una parola: "LEGITTIMO" oppure "VANDALISMO".
    """
    
    try:
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        return f"Errore AI: {e}"

def main():
    print("--- AI JUDGE AVVIATO (Il Giudice) ---")

    if not GEMINI_API_KEY:
        print(f"❌ ERRORE CRITICO: Impossibile trovare GEMINI_API_KEY in {env_path}")
        return

    genai.configure(api_key=GEMINI_API_KEY)
    
    ground_truth = load_ground_truth()
    print(f"✅ API Key caricata. Modello: {GEMINI_MODEL}")
    print(f"📚 Contesto caricato. In attesa...")

    # --- CORREZIONE QUI: Aggiunto group_id univoco ---
    consumer = KafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='ai_judge_group'  # Questo assicura che riceva una copia di tutti i messaggi
    )

    for message in consumer:
        event = message.value
        comment = event['comment']
        user = event['user']
        
        print(f"\nAnalisi edit di [{user}]:")
        print(f"  Commento: \"{comment}\"")
        
        verdict = analyze_with_gemini(comment, ground_truth)
        
        if "VANDALISMO" in verdict.upper():
            color = "\033[91m" # Rosso
            icon = "🚨"
        elif "LEGITTIMO" in verdict.upper():
            color = "\033[92m" # Verde
            icon = "✅"
        else:
            color = "\033[93m" # Giallo
            icon = "⚠️"
            
        reset = "\033[0m"
        
        print(f"  Verdetto: {color}{icon} {verdict}{reset}")
        print("-" * 50)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nSpegnimento Judge.")