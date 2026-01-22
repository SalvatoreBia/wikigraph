import json
import os
import random
import time

import requests
from kafka import KafkaProducer

MODE = "auto"

KAFKA_BROKER = 'localhost:9094'
TOPIC_OUT = 'to-be-judged'
HISTORY_FILE = '501_manual_edits_history.json'


MOCKED_EDITS_DIR = '../data/mocked_edits'
LEGIT_EDITS_FILE = os.path.join(MOCKED_EDITS_DIR, 'legit_edits.json')
VANDAL_EDITS_FILE = os.path.join(MOCKED_EDITS_DIR, 'vandal_edits.json')
LEGIT_EDITS_TEST_FILE = os.path.join(MOCKED_EDITS_DIR, 'legit_edits_test.json')
VANDAL_EDITS_TEST_FILE = os.path.join(MOCKED_EDITS_DIR, 'vandal_edits_test.json')

def load_unique_titles_from_mocks():
    titles = set()
    files = [LEGIT_EDITS_FILE, VANDAL_EDITS_FILE, LEGIT_EDITS_TEST_FILE, VANDAL_EDITS_TEST_FILE]
    
    for filepath in files:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                edits = json.load(f)
                for edit in edits:
                    if "title" in edit:
                        titles.add(edit["title"])
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"! Impossibile caricare {filepath}: {e}")
    
    return sorted(list(titles))

def select_page_title():
    titles = load_unique_titles_from_mocks()
    
    if MODE == "auto":
        selected = random.choice(titles) if titles else "Gaio_Giulio_Cesare"
        print(f"\n[AUTO] Titolo selezionato: {selected}")
        return selected
    
    print("\n--- SELEZIONE PAGINA ---")
    print("0. Inserisci titolo manualmente")
    
    if len(titles) > 20:
        sample_titles = random.sample(titles, 20)
        sample_titles.sort()
    else:
        sample_titles = titles
    
    for i, title in enumerate(sample_titles, 1):
        print(f"{i}. {title}")
    
    if len(titles) > 20:
        print(f"... ({len(titles) - 20} altri titoli disponibili)")
        print(f"{len(sample_titles) + 1}. Mostra tutti i titoli")
        print(f"{len(sample_titles) + 2}. Titolo casuale")
    else:
        print(f"{len(sample_titles) + 1}. Titolo casuale")
    
    choice = input(f"\nScelta [casuale]: ").strip()
    
    if choice == "0":
        custom = input("Inserisci il titolo Wikipedia (es. 'Gaio_Giulio_Cesare'): ").strip()
        return custom if custom else "Gaio_Giulio_Cesare"
    
    if len(titles) > 20 and choice == str(len(sample_titles) + 1):
        print("\n--- TUTTI I TITOLI ---")
        for i, title in enumerate(titles, 1):
            print(f"{i}. {title}")
        choice = input(f"\nScelta [casuale]: ").strip()
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(titles):
                return titles[idx]
        except ValueError:
            pass
        return random.choice(titles)
    
    random_idx = len(sample_titles) + 1 if len(titles) <= 20 else len(sample_titles) + 2
    if not choice or choice == str(random_idx):
        selected = random.choice(titles) if titles else "Gaio_Giulio_Cesare"
        print(f"- Selezionato casualmente: {selected}")
        return selected
    
    try:
        idx = int(choice) - 1
        if 0 <= idx < len(sample_titles):
            return sample_titles[idx]
    except ValueError:
        pass
    
    selected = random.choice(titles) if titles else "Gaio_Giulio_Cesare"
    print(f"- Selezionato casualmente: {selected}")
    return selected

def load_history():
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def save_to_history(event):
    history = load_history()
    history.append(event)
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2, ensure_ascii=False)
    print(f"- Evento aggiunto allo storico ({len(history)} totali in '{HISTORY_FILE}').")

def download_wikipedia_page(page_title, lang="it"):
    print(f"- Scaricamento pagina: {page_title} ({lang})...")
    url = f"https://{lang}.wikipedia.org/w/api.php"
    headers = {
        "User-Agent": "WikiGraphBot/1.0"
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
        response.raise_for_status()
        
        try:
            data = response.json()
        except json.JSONDecodeError:
            print(f"! Errore: La risposta non è un JSON valido.")
            return None
            
        if "query" not in data or "pages" not in data["query"]:
            print(f"! Risposta inattesa dall'API.")
            return None
            
        pages = data["query"]["pages"]
        if not pages or "missing" in pages[0]:
             print(f"! Pagina '{page_title}' non trovata.")
             return None
             
        page = pages[0]
        revision = page["revisions"][0]
        content = revision["slots"]["main"]["content"]
        
        print(f"- Pagina scaricata ({len(content)} caratteri).")
        return content
    except Exception as e:
        print(f"! Errore durante il download: {e}")
        return None

def create_manual_event(page_title, original_text, new_text, comment, user, is_vandalism, lang="it"):
    event = {
        "title": page_title,
        "user": user,
        "comment": comment,
        "timestamp": int(time.time()),
        "is_vandalism": is_vandalism, 
        "diff_url": f"https://{lang}.wikipedia.org/w/index.php?title={page_title}&diff=prev&oldid=000000",
        "server_name": f"{lang}.wikipedia.org",
        "wiki": f"{lang}wiki",
        "original_text": original_text,
        "new_text": new_text
    }
    return event

def select_window(content, window_size=600):
    if len(content) <= window_size:
        return content
    
    if MODE == "auto":
        start = random.randint(0, len(content) - window_size)
        if start > 0:
            while start < len(content) and content[start] not in (' ', '\n'):
                start += 1
        window = content[start : start + window_size]
        print(f"[AUTO] Finestra casuale selezionata (da carattere {start})")
        return window
        
    print(f"\n- SELEZIONE FINESTRA TESTO ({len(content)} caratteri) -")
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
        
    if start > 0:
        while start < len(content) and content[start] not in (' ', '\n'):
            start += 1
            
    window = content[start : start + window_size]
    return window

def main():
    print("--- TEST MANUAL CLASSIFIER ---")
    
    lang = "it"
    page_title = select_page_title()
    
    print(f"\n- Target: {page_title} ({lang})")
    
    content = download_wikipedia_page(page_title, lang)
    if not content:
        return

    original_window = select_window(content)
    
    filename = "500_draft_edit.txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(original_window)
        
    print(f"\n- Finestra salvata in '{filename}'.")
    print("- MODIFICA IL FILE E SALVA, poi premi INVIO...")
    input()
    
    try:
        with open(filename, "r", encoding="utf-8") as f:
            new_window = f.read()
    except FileNotFoundError:
        print(f"! File '{filename}' non trovato.")
        return

    print("- Modifiche rilevate." if original_window != new_window else "! Nessuna modifica rilevata.")

    if MODE == "auto":
        user = "AlessandroBarbero"
        comment = "correzione imprecisione"
        is_vandalism = input("È vandalismo? (s/N): ").strip().lower() in ['s', 'y', 'si', 'yes']
        print(f"[AUTO] Utente: {user}, Commento: {comment}")
    else:
        user = input("Nome Utente [Manuale]: ").strip() or "Manuale"
        comment = input("Commento [Test]: ").strip() or "Test"
        is_vandalism = input("È vandalismo? (s/N): ").strip().lower() in ['s', 'y', 'si', 'yes']
    
    event = create_manual_event(page_title, original_window, new_window, comment, user, is_vandalism, lang)
    
    print(f"\n- Invio evento a topic '{TOPIC_OUT}':")
    print(json.dumps(event, indent=2, ensure_ascii=False))

    with open("500_manual_edit.json", "w", encoding="utf-8") as f:
        json.dump(event, f, indent=2, ensure_ascii=False)
    
    save_to_history(event)
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        producer.send(TOPIC_OUT, value=event)
        producer.flush()
        producer.close()
        print("\n- Evento inviato.")
    except Exception as e:
        print(f"\n! Kafka non disponibile: {e}")

if __name__ == "__main__":
    main()
