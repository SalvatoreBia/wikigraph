import json
import random
import time
import requests
from kafka import KafkaProducer

# --- CONFIG ---
KAFKA_BROKER = 'localhost:9092'
TOPIC_OUT = 'to-be-judged' # Topic ascoltato da 202_bc_judge.py

def download_wikipedia_page(page_title):
    """Scarica il contenuto (wikitext) di una pagina Wikipedia."""
    print(f"📥 Scaricamento pagina: {page_title}...")
    url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "prop": "revisions",
        "titles": page_title,
        "rvprop": "content",
        "format": "json"
    }
    try:
        response = requests.get(url, params=params)
        data = response.json()
        pages = data["query"]["pages"]
        page_id = list(pages.keys())[0]
        if page_id == "-1":
            print(f"❌ Pagina '{page_title}' non trovata.")
            return None
        content = pages[page_id]["revisions"][0]["*"]
        print(f"✅ Pagina scaricata ({len(content)} caratteri).")
        return content
    except Exception as e:
        print(f"❌ Errore durante il download: {e}")
        return None

def create_mock_edit(page_title, original_content, is_vandalism=False):
    """Crea un edit mockato (JSON) basato sulla pagina scaricata."""
    
    if is_vandalism:
        user = "VandalUser_" + str(random.randint(1000, 9999))
        comment = "REMOVED CONTENT HAHA"
        # In un caso reale modificheremmo il content, ma il classifier usa solo comment/title per ora.
        # Simulo comunque un cambiamento nel testo se servisse in futuro
        new_content = "VANDALIZED CONTENT" 
    else:
        user = "GoodUser_" + str(random.randint(1000, 9999))
        comment = "Fixed typo"
        new_content = original_content + "\n\n<!-- Small fix -->"

    event = {
        "title": page_title,
        "user": user,
        "comment": comment,
        "timestamp": time.time(),
        "is_vandalism": is_vandalism, # Per verifica lato judge
        "diff_url": f"https://en.wikipedia.org/w/index.php?title={page_title}&diff=prev&oldid=000000" # Fake URL
    }
    return event

def main():
    print("--- 🧪 TEST MANUAL CLASSIFIER ---")
    
    # 1. Chiedi input all'utente (o usa default)
    page_title = input("Inserisci il titolo della pagina Wikipedia da testare [Python (programming language)]: ").strip()
    if not page_title:
        page_title = "Python (programming language)"
        
    edit_type = input("Tipo di edit? (v=vandalismo, l=legittimo) [l]: ").strip().lower()
    is_vandalism = (edit_type == 'v')
    
    # 2. Scarica pagina
    content = download_wikipedia_page(page_title)
    if not content:
        return

    # 3. Crea Mock Edit
    event = create_mock_edit(page_title, content, is_vandalism)
    
    # 4. Invia a Kafka
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print(f"📤 Invio evento a topic '{TOPIC_OUT}':")
    print(json.dumps(event, indent=2))
    
    producer.send(TOPIC_OUT, value=event)
    producer.flush()
    print("✅ Evento inviato! Controlla l'output di 202_bc_judge.py")

if __name__ == "__main__":
    main()
