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
- Google Gemini (consigliato - ha accesso a informazioni recenti)
- OpenAI GPT (richiede API key)
- Ollama locale (gratuito ma limitato)
"""

import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from neo4j import GraphDatabase

# --- Configurazione ---
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'cluster-alerts'

# Neo4j configuration (per arricchire i dati)
N_SERVERS = 4
NEO4J_SERVERS = {
    0: "bolt://localhost:7687",
    1: "bolt://localhost:7688",
    2: "bolt://localhost:7689",
    3: "bolt://localhost:7690",
}
NEO4J_USER = "neo4j"
NEO4J_PASS = "password"

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
    Interroga Google Gemini (consigliato per informazioni recenti).
    Richiede: pip install google-generativeai
    E una API key da https://makersuite.google.com/app/apikey
    """
    try:
        import google.generativeai as genai
        import os
        
        # Leggi la API key da variabile d'ambiente
        api_key = os.environ.get('GEMINI_API_KEY')
        
        if not api_key:
            return "❌ GEMINI_API_KEY non configurata. Imposta con: export GEMINI_API_KEY='your-key'"
        
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-pro')
        
        print("  → Interrogazione Google Gemini...")
        response = model.generate_content(prompt)
        
        return response.text
    
    except ImportError:
        return "❌ Gemini non disponibile. Installa con: pip install google-generativeai"
    except Exception as e:
        return f"❌ Errore Gemini: {e}"


def query_llm_openai(prompt):
    """
    Interroga OpenAI GPT (richiede API key a pagamento).
    Richiede: pip install openai
    """
    try:
        import openai
        import os
        
        api_key = os.environ.get('OPENAI_API_KEY')
        
        if not api_key:
            return "❌ OPENAI_API_KEY non configurata."
        
        openai.api_key = api_key
        
        print("  → Interrogazione OpenAI GPT...")
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "Sei un esperto analista di Wikipedia."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=500
        )
        
        return response.choices[0].message.content
    
    except ImportError:
        return "❌ OpenAI non disponibile. Installa con: pip install openai"
    except Exception as e:
        return f"❌ Errore OpenAI: {e}"


def query_llm_ollama(prompt):
    """
    Interroga un modello locale via Ollama (gratuito).
    Richiede: Ollama installato e in esecuzione
    https://ollama.ai/
    
    Esegui prima: ollama pull llama2
    """
    try:
        import requests
        
        print("  → Interrogazione Ollama (locale)...")
        
        response = requests.post(
            'http://localhost:11434/api/generate',
            json={
                'model': 'llama2',
                'prompt': prompt,
                'stream': False
            },
            timeout=60
        )
        
        if response.status_code == 200:
            return response.json().get('response', 'Nessuna risposta')
        else:
            return f"❌ Errore Ollama: {response.status_code}"
    
    except requests.exceptions.ConnectionError:
        return "❌ Ollama non in esecuzione. Avvia con: ollama serve"
    except Exception as e:
        return f"❌ Errore Ollama: {e}"


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
    elif llm_choice == "openai":
        llm_response = query_llm_openai(prompt)
    elif llm_choice == "ollama":
        llm_response = query_llm_ollama(prompt)
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
    
    # Scegli quale LLM usare
    print("\nScegli il modello LLM da usare:")
    print("1. Google Gemini (consigliato - ha accesso a info recenti)")
    print("2. OpenAI GPT-4 (richiede API key a pagamento)")
    print("3. Ollama locale (gratuito, richiede installazione)")
    
    choice = input("\nScelta (1-3): ").strip()
    
    llm_map = {
        "1": "gemini",
        "2": "openai",
        "3": "ollama"
    }
    
    llm_choice = llm_map.get(choice, "gemini")
    print(f"\n✓ Utilizzo: {llm_choice}\n")
    
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
