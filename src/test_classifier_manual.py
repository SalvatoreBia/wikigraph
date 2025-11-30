import json
import random
import time
import requests
from kafka import KafkaProducer

# --- CONFIG ---
KAFKA_BROKER = 'localhost:9092'
TOPIC_OUT = 'to-be-judged' # Topic ascoltato da 202_bc_judge.py

def download_wikipedia_page(page_title, lang="en"):
    """Scarica il contenuto (wikitext) di una pagina Wikipedia."""
    print(f"📥 Scaricamento pagina: {page_title} ({lang})...")
    url = f"https://{lang}.wikipedia.org/w/api.php"
    headers = {
        "User-Agent": "WikiGraphBot/1.0 (https://github.com/yourusername/wikigraph; ribben@example.com)"
    }
    params = {
        "action": "query",
        "prop": "revisions",
        "titles": page_title,
        "rvprop": "content",
        "rvslots": "*",
        "format": "json",
        "formatversion": "2"
    }
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status() # Check for HTTP errors
        
        try:
            data = response.json()
        except json.JSONDecodeError:
            print(f"❌ Errore: La risposta non è un JSON valido.")
            print(f"Status Code: {response.status_code}")
            print(f"Response Text: {response.text[:500]}...") 
            return None
            
        if "query" not in data or "pages" not in data["query"]:
            print(f"❌ Risposta inattesa dall'API: {data}")
            return None
            
        pages = data["query"]["pages"]
        if not pages or "missing" in pages[0]:
             print(f"❌ Pagina '{page_title}' non trovata su {lang}.wikipedia.org.")
             return None
             
        # formatversion=2 returns pages as a list
        page = pages[0]
        revision = page["revisions"][0]
        content = revision["slots"]["main"]["content"]
        
        print(f"✅ Pagina scaricata ({len(content)} caratteri).")
        return content
    except Exception as e:
        print(f"❌ Errore durante il download: {e}")
        return None

def create_manual_event(page_title, original_content, new_content, comment, user, lang="it"):
    """Crea un evento JSON con il contenuto modificato manualmente."""
    
    # Semplice logica per is_vandalism (opzionale, solo per debug)
    is_vandalism = False 
    
    event = {
        "title": page_title,
        "user": user,
        "comment": comment,
        "timestamp": int(time.time()),
        "is_vandalism": is_vandalism, 
        "diff_url": f"https://{lang}.wikipedia.org/w/index.php?title={page_title}&diff=prev&oldid=000000",
        "server_name": f"{lang}.wikipedia.org",
        "wiki": f"{lang}wiki",
        # Campi extra per compatibilità futura o debug
        "original_content_snippet": original_content[:100],
        "new_content_snippet": new_content[:100]
    }
    return event

def main():
    print("--- 🧪 TEST MANUAL CLASSIFIER (FILE MODE) ---")
    
    # 1. Configurazione (Hardcoded come richiesto, ma modificabile)
    lang = "it"
    page_title = "Gaio_Giulio_Cesare"
    
    print(f"Target: {page_title} ({lang})")
    
    # 2. Scarica pagina
    content = download_wikipedia_page(page_title, lang)
    if not content:
        return

    # 3. Salva su file temporaneo
    filename = "draft_edit.txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(content)
        
    print(f"\n✅ Contenuto salvato in '{filename}'.")
    print(f"👉 ORA MODIFICA IL FILE '{filename}' con il tuo editor preferito.")
    print("   Salva il file e poi premi INVIO qui per continuare...")
    input()
    
    # 4. Leggi file modificato
    try:
        with open(filename, "r", encoding="utf-8") as f:
            new_content = f.read()
    except FileNotFoundError:
        print(f"❌ File '{filename}' non trovato!")
        return

    if content == new_content:
        print("⚠️ Nessuna modifica rilevata nel file. Procedo comunque...")
    else:
        print("✅ Modifiche rilevate!")

    # 5. Richiedi Dati Edit
    user = input("Nome Utente [ManualUser]: ").strip()
    if not user: user = "ManualUser"
    
    comment = input("Commento dell'edit [Test edit]: ").strip()
    if not comment: comment = "Test edit"
    
    # 6. Crea Evento
    event = create_manual_event(page_title, content, new_content, comment, user, lang)
    
    # 7. Invia a Kafka
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print(f"\n📤 Invio evento a topic '{TOPIC_OUT}':")
    print(json.dumps(event, indent=2, ensure_ascii=False))
    
    producer.send(TOPIC_OUT, value=event)
    producer.flush()
    print("\n✅ Evento inviato! Controlla l'output di 202_bc_judge.py")

if __name__ == "__main__":
    main()
