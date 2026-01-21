import json
import random
import time
import requests
from kafka import KafkaProducer

# --- CONFIG ---
KAFKA_BROKER = 'localhost:9094'
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

def create_manual_event(page_title, original_text, new_text, comment, user, is_vandalism, lang="it"):
    """Crea un evento JSON con il contenuto modificato manualmente."""
    
    event = {
        "title": page_title,
        "user": user,
        "comment": comment,
        "timestamp": int(time.time()),
        "is_vandalism": is_vandalism, 
        "diff_url": f"https://{lang}.wikipedia.org/w/index.php?title={page_title}&diff=prev&oldid=000000",
        "server_name": f"{lang}.wikipedia.org",
        "wiki": f"{lang}wiki",
        # Campi corretti per il BC Judge
        "original_text": original_text,
        "new_text": new_text
    }
    return event

def select_window(content, window_size=600):
    """Permette all'utente di selezionare una finestra di testo o ne prende una a caso."""
    if len(content) <= window_size:
        return content
        
    print(f"\n--- SELEZIONE FINESTRA TESTO ({len(content)} chars) ---")
    print("1. Inizio pagina")
    print("2. Metà pagina")
    print("3. Fine pagina")
    print("4. Casuale")
    choice = input("Scelta [4]: ").strip()
    
    if choice == "1":
        start = 0
    elif choice == "2":
        start = len(content) // 2
    elif choice == "3":
        start = len(content) - window_size
    else:
        start = random.randint(0, len(content) - window_size)
        
    # Adjust start to space
    if start > 0:
        while start < len(content) and content[start] not in (' ', '\n'):
            start += 1
            
    window = content[start : start + window_size]
    return window

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

    # 3. Seleziona Finestra
    original_window = select_window(content)
    
    # 4. Salva su file temporaneo
    filename = "500_draft_edit.txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(original_window)
        
    print(f"\n✅ Finestra di testo salvata in '{filename}'.")
    print(f"👉 ORA MODIFICA IL FILE '{filename}' con il tuo editor preferito.")
    print("   Salva il file e poi premi INVIO qui per continuare...")
    input()
    
    # 5. Leggi file modificato
    try:
        with open(filename, "r", encoding="utf-8") as f:
            new_window = f.read()
    except FileNotFoundError:
        print(f"❌ File '{filename}' non trovato!")
        return

    if original_window == new_window:
        print("⚠️ Nessuna modifica rilevata nel file. Procedo comunque...")
    else:
        print("✅ Modifiche rilevate!")

    # 5. Richiedi Dati Edit
    user = input("Nome Utente [ManualUser]: ").strip()
    if not user: user = "ManualUser"
    
    comment = input("Commento dell'edit [Test edit]: ").strip()
    if not comment: comment = "Test edit"

    is_vandalism_input = input("È vandalismo? (s/N): ").strip().lower()
    is_vandalism = is_vandalism_input in ['s', 'y', 'si', 'yes']
    
    # 6. Crea Evento
    event = create_manual_event(page_title, original_window, new_window, comment, user, is_vandalism, lang)
    
    # 7. Invia a Kafka (con fallback su file se Kafka non è disponibile)
    print(f"\n📤 Invio evento a topic '{TOPIC_OUT}':")
    print(json.dumps(event, indent=2, ensure_ascii=False))

    # Save to file independently of Kafka
    with open("500_manual_edit.json", "w", encoding="utf-8") as f:
        json.dump(event, f, indent=2, ensure_ascii=False)
    print(f"📁 Evento salvato in '500_manual_edit.json'")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        producer.send(TOPIC_OUT, value=event)
        producer.flush()
        producer.close()
        print("\n✅ Evento inviato! Controlla l'output di 202_bc_judge.py")
    except Exception as e:
        print(f"\n⚠️ Kafka non disponibile: {e}")
        # Salva su file come fallback
        fallback_file = "manual_event.json"
        with open(fallback_file, "w", encoding="utf-8") as f:
            json.dump(event, f, indent=2, ensure_ascii=False)
        print(f"📁 Evento salvato in '{fallback_file}' per uso successivo.")

if __name__ == "__main__":
    main()
