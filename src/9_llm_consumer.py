"""
9_llm_consumer.py - LLM Oracle Consumer

Questo script ascolta gli allarmi di hotspot dal topic 'cluster-alerts'
e usa un LLM (Large Language Model) per interpretare e analizzare gli eventi.

Funzionalità:
1. Riceve allarmi di hotspot da Kafka
2. Arricchisce i dati interrogando Neo4j (pagine importanti della comunità)
3. Costruisce un prompt contestuale per l'LLM
4. Interroga l'LLM per ottenere un'analisi dell'evento
5. Salva le analisi in un file di log

LLM supportati:
- Google Gemini 2.0 Flash (consigliato - veloce e ha accesso a informazioni recenti)
- LM Studio locale (gratuito, richiede configurazione)
"""

import json
import os
from datetime import datetime
from pathlib import Path
from kafka import KafkaConsumer
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Carica variabili d'ambiente dal file .env
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# --- Configurazione da .env ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'cluster-alerts')

# Neo4j configuration (per arricchire i dati)
N_SERVERS = 4
NEO4J_SERVERS = {
    0: "bolt://localhost:7687",
    1: "bolt://localhost:7688",
    2: "bolt://localhost:7689",
    3: "bolt://localhost:7690",
}
NEO4J_USER = os.getenv('NEO4J_USER', 'neo4j')
NEO4J_PASS = os.getenv('NEO4J_PASS', 'password')

# LLM Configuration
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
LM_STUDIO_MODEL = os.getenv('LM_STUDIO_MODEL', 'google/gemma-2-9b-it-GGUF')
LM_STUDIO_HOST = os.getenv('LM_STUDIO_HOST', 'http://127.0.0.1:1234')

# File di log per salvare le analisi
LOG_FILE = "llm_analysis.log"


def create_kafka_consumer():
    """Crea un consumer Kafka per ascoltare gli allarmi."""
    print(f"Connessione a Kafka su {KAFKA_BROKER}...")
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='latest',  # Inizia dagli eventi più recenti
            enable_auto_commit=True,
            group_id='llm-oracle-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print(f"✓ Consumer Kafka connesso al topic '{KAFKA_TOPIC}'")
        return consumer
    except Exception as e:
        print(f"✗ Errore connessione Kafka: {e}")
        return None


def get_neo4j_driver(server_id):
    """Crea una connessione a un server Neo4j specifico."""
    uri = NEO4J_SERVERS[server_id]
    return GraphDatabase.driver(uri, auth=(NEO4J_USER, NEO4J_PASS))


def enrich_alert_with_neo4j(alert_data):
    """
    Arricchisce i dati dell'allarme interrogando Neo4j.
    
    Ottiene:
    - Titolo delle pagine più importanti nella comunità (alto PageRank)
    - Dimensione totale della comunità
    - Categoria tematica (se disponibile)
    """
    community_id = alert_data.get('communityId')
    if not community_id:
        return alert_data
    
    # Determina il server Neo4j da interrogare
    server_id = int(community_id) % N_SERVERS
    
    print(f"  → Interrogazione Neo4j server {server_id} per comunità {community_id}...")
    
    driver = get_neo4j_driver(server_id)
    
    enriched_data = alert_data.copy()
    
    try:
        with driver.session() as session:
            # Query per ottenere info sulla comunità
            query = """
            MATCH (p:Page {globalCommunityId: $community_id})
            RETURN 
                p.title AS title,
                p.pagerank AS pagerank,
                COUNT(*) OVER() AS total_pages
            ORDER BY p.pagerank DESC
            LIMIT 10
            """
            
            result = session.run(query, community_id=community_id)
            records = list(result)
            
            if records:
                # Pagine più importanti (alto PageRank)
                top_pages = [
                    {
                        "title": r["title"],
                        "pagerank": round(r["pagerank"], 6) if r["pagerank"] else 0
                    }
                    for r in records[:5]
                ]
                
                enriched_data['topPages'] = top_pages
                enriched_data['communitySize'] = records[0]['total_pages']
                
                print(f"  ✓ Trovate {len(top_pages)} pagine top, comunità di {enriched_data['communitySize']} nodi")
            else:
                print(f"  ⚠ Nessun dato trovato per comunità {community_id}")
    
    except Exception as e:
        print(f"  ✗ Errore interrogazione Neo4j: {e}")
    finally:
        driver.close()
    
    return enriched_data


def build_llm_prompt(enriched_alert):
    """
    Costruisce un prompt strutturato per l'LLM.
    """
    community_id = enriched_alert.get('communityId', 'N/A')
    edit_count = enriched_alert.get('totalEditCount', 0)
    window_start = enriched_alert.get('windowStart', 'N/A')
    window_end = enriched_alert.get('windowEnd', 'N/A')
    
    # Pagine coinvolte nell'allarme (da 7_stream_processor.py)
    alert_pages = enriched_alert.get('pages', [])
    
    # Pagine importanti della comunità (da Neo4j)
    top_pages = enriched_alert.get('topPages', [])
    community_size = enriched_alert.get('communitySize', 'N/A')
    
    prompt = f"""Sei un esperto analista di Wikipedia che monitora attività sospette o significative.

**CONTESTO DEL SISTEMA:**
Ho rilevato un hotspot di attività su Wikipedia italiana tramite analisi in tempo reale.

**DETTAGLI DELL'ALLARME:**
- **Comunità ID**: {community_id}
- **Dimensione comunità**: {community_size} pagine correlate
- **Numero di modifiche**: {edit_count} modifiche in 2.5 minuti
- **Periodo**: da {window_start} a {window_end}
- **Soglia normale**: 5 modifiche (questo evento ha superato la soglia)

**PAGINE MODIFICATE DURANTE L'ALLARME:**
{chr(10).join([f"- {page}" for page in alert_pages]) if alert_pages else "- (non disponibili)"}

**PAGINE PIÙ IMPORTANTI NELLA COMUNITÀ** (per PageRank):
{chr(10).join([f"- {p['title']} (PageRank: {p['pagerank']})" for p in top_pages]) if top_pages else "- (non disponibili)"}

**DOMANDE PER L'ANALISI:**

1. **Interpretazione tematica**: Osservando le pagine coinvolte, quale sembra essere il tema principale di questa comunità? (es. politica, sport, scienza, cultura pop, geografia, ecc.)

2. **Causa probabile**: Basandoti sulla tua conoscenza e sul contesto attuale, qual è la spiegazione più probabile per questo picco di attività?
   - È un evento di cronaca recente?
   - È un pattern storico noto (es. anniversari, ricorrenze)?
   - Potrebbe essere vandalismo coordinato?
   - È legato a un dibattito culturale/sociale in corso?

3. **Livello di preoccupazione**: Su una scala da 1 a 10, quanto è preoccupante questo evento?
   - 1-3: Attività normale/prevedibile
   - 4-6: Merita attenzione ma non è critico
   - 7-10: Richiede intervento immediato (probabile vandalismo o manipolazione)

4. **Azioni consigliate**: Cosa dovrebbe fare un amministratore di Wikipedia?

**RISPONDI IN MODO CONCISO E STRUTTURATO.**
"""
    
    return prompt


def query_llm_gemini(prompt):
    """
    Interroga Google Gemini 2.0 Flash Experimental.
    Richiede: pip install google-generativeai
    E una API key da https://aistudio.google.com/app/apikey
    """
    try:
        import google.generativeai as genai
        
        if not GEMINI_API_KEY:
            return "❌ GEMINI_API_KEY non configurata. Controlla il file .env"
        
        genai.configure(api_key=GEMINI_API_KEY)
        
        # Usa il modello Gemini 2.0 Flash Experimental (veloce e potente)
        model = genai.GenerativeModel('gemini-2.0-flash-exp')
        
        print("  → Interrogazione Google Gemini 2.0 Flash Experimental...")
        response = model.generate_content(prompt)
        
        return response.text
    
    except ImportError:
        return "❌ Gemini non disponibile. Installa con: pip install google-generativeai"
    except Exception as e:
        return f"❌ Errore Gemini: {e}"


def query_llm_lmstudio(prompt):
    """
    Interroga LM Studio con endpoint compatibile OpenAI.
    LM Studio deve essere in esecuzione su http://127.0.0.1:1234
    
    Configurazione:
    1. Scarica LM Studio da https://lmstudio.ai/
    2. Carica un modello (es. gemma-2-9b-it, llama-3.1-8b)
    3. Avvia il server locale sulla porta 1234
    4. Configura LM_STUDIO_MODEL nel file .env
    """
    try:
        import requests
        
        print(f"  → Interrogazione LM Studio (locale) - Modello: {LM_STUDIO_MODEL}...")
        
        response = requests.post(
            f'{LM_STUDIO_HOST}/v1/chat/completions',
            headers={'Content-Type': 'application/json'},
            json={
                'model': LM_STUDIO_MODEL,
                'messages': [
                    {'role': 'system', 'content': 'Sei un esperto analista di Wikipedia italiana.'},
                    {'role': 'user', 'content': prompt}
                ],
                'temperature': 0.7,
                'max_tokens': 500,
                'stream': False
            },
            timeout=120
        )
        
        if response.status_code == 200:
            data = response.json()
            return data['choices'][0]['message']['content']
        else:
            return f"❌ Errore LM Studio: {response.status_code} - {response.text}"
    
    except requests.exceptions.ConnectionError:
        return "❌ LM Studio non in esecuzione. Avvia LM Studio e carica un modello su porta 1234"
    except Exception as e:
        return f"❌ Errore LM Studio: {e}"


def save_analysis_to_log(alert, prompt, llm_response):
    """Salva l'analisi in un file di log."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    log_entry = f"""
{'='*80}
TIMESTAMP: {timestamp}
COMMUNITY ID: {alert.get('communityId')}
EDIT COUNT: {alert.get('totalEditCount')}
{'='*80}

PROMPT INVIATO ALL'LLM:
{prompt}

{'='*80}

RISPOSTA LLM:
{llm_response}

{'='*80}

"""
    
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_entry)
    
    print(f"  ✓ Analisi salvata in {LOG_FILE}")


def process_alert(alert, llm_choice):
    """
    Processa un singolo allarme:
    1. Arricchisce con dati Neo4j
    2. Costruisce prompt
    3. Interroga LLM
    4. Salva risultato
    """
    print(f"\n{'='*80}")
    print(f"NUOVO ALLARME RICEVUTO")
    print(f"{'='*80}")
    print(f"Comunità: {alert.get('communityId')}")
    print(f"Modifiche: {alert.get('totalEditCount')}")
    print(f"Pagine: {', '.join(alert.get('pages', [])[:3])}...")
    
    # Step 1: Arricchisci con Neo4j
    enriched = enrich_alert_with_neo4j(alert)
    
    # Step 2: Costruisci prompt
    prompt = build_llm_prompt(enriched)
    
    # Step 3: Interroga LLM
    if llm_choice == "gemini":
        llm_response = query_llm_gemini(prompt)
    elif llm_choice == "lmstudio":
        llm_response = query_llm_lmstudio(prompt)
    else:
        llm_response = "❌ LLM non riconosciuto"
    
    # Step 4: Mostra e salva
    print(f"\n{'='*80}")
    print("ANALISI LLM:")
    print(f"{'='*80}")
    print(llm_response)
    print(f"{'='*80}\n")
    
    save_analysis_to_log(alert, prompt, llm_response)


def main():
    """Main loop: ascolta allarmi e li processa con LLM."""
    
    print("="*80)
    print("LLM ORACLE CONSUMER - Analisi Intelligente Hotspot")
    print("="*80)
    
    # Verifica configurazione .env
    if not os.path.exists(env_path):
        print("\n⚠️  ATTENZIONE: File .env non trovato!")
        print("   Crea il file .env copiando .env.example:")
        print("   cp .env.example .env")
        print("\n   Poi configura almeno GEMINI_API_KEY")
        return
    
    # Scegli quale LLM usare
    print("\nScegli il modello LLM da usare:")
    print("1. Google Gemini 2.0 Flash (consigliato - veloce con info recenti)")
    print("2. LM Studio locale (gratuito, richiede configurazione)")
    
    choice = input("\nScelta (1-2): ").strip()
    
    llm_map = {
        "1": "gemini",
        "2": "lmstudio"
    }
    
    llm_choice = llm_map.get(choice, "gemini")
    
    # Verifica configurazione specifica
    if llm_choice == "gemini" and not GEMINI_API_KEY:
        print("\n❌ ERRORE: GEMINI_API_KEY non configurata nel file .env")
        print("   Ottieni una chiave gratuita su: https://aistudio.google.com/app/apikey")
        return
    
    print(f"\n✓ Utilizzo: {llm_choice.upper()}\n")
    
    # Crea consumer
    consumer = create_kafka_consumer()
    
    if not consumer:
        print("Impossibile avviare il consumer. Verifica che Kafka sia in esecuzione.")
        return
    
    print(f"\n{'='*80}")
    print("IN ASCOLTO SUL TOPIC 'cluster-alerts'")
    print("Premi Ctrl+C per terminare")
    print(f"{'='*80}\n")
    
    try:
        for message in consumer:
            alert = message.value
            process_alert(alert, llm_choice)
    
    except KeyboardInterrupt:
        print("\n\n⚠ Interruzione da utente. Chiusura...")
    
    except Exception as e:
        print(f"\n✗ Errore: {e}")
    
    finally:
        consumer.close()
        print("✓ Consumer Kafka chiuso.")


if __name__ == "__main__":
    main()
