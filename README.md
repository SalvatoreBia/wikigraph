# WikiGraph - Sistema di Rilevamento Hotspot Wikipedia

Sistema di analisi in tempo reale dello stream di modifiche di Wikipedia italiana, con rilevamento automatico di hotspot di attività e analisi tramite LLM.


## 🚀 Setup Iniziale

### 1. Download Dati Wikipedia

Sito coi dump di Wikipedia:
```
https://dumps.wikimedia.org/itwiki/latest/
```

**Opzione 1**: Download manuale
```bash
# Scarica e unzippa:
# - itwiki-latest-page.sql.gz
# - itwiki-latest-pagelinks.sql.gz
# Posizionali in /data/
```

**Opzione 2**: Script automatico
```bash
python src/0_update_wikipedia_files.py
```

**Struttura finale richiesta:**
```
data/
├── empty
├── itwiki-latest-page.sql
└── itwiki-latest-pagelinks.sql
```

### 2. Installazione Dipendenze

```bash
python -m venv .venv 
source .venv/bin/activate && pip install -r requirements.txt
```

---

## 🔄 Esecuzione Pipeline

**Esegui gli script in ordine dalla directory `/src/`:**

```bash
cd src

# 1. Costruisce mappa ID -> Titolo pagina
python 1_build_id_title_map.py

# 2. Avvia cluster Neo4j (4 istanze)
./2_start_docker.sh

# 3. Carica grafo in Neo4j
python 3_load_graph_data.py

# 4. Calcola comunità (Louvain)
python 4_communities_calculator.py

# 5. Unisce comunità tra server
python 5_merge_communities.py

# 6. Producer stream Wikipedia reale (opzionale)
python 6_producer.py

# 7. Stream processor (rileva hotspot)
python 7_stream_processor.py
```

---


**Test completo in 3 terminali:**

**Terminale 1** - Stream Processor:
```bash
python src/7_stream_processor.py
```

**Terminale 2** - LLM Consumer:
```bash
python src/9_llm_consumer.py
```

**Terminale 3** - Mock Producer (test):
```bash
python src/8_mock_producer.py
```

### Configurazione LLM

Scegli uno dei provider:

**Google Gemini (consigliato)**:
```bash
export GEMINI_API_KEY='your-api-key'
# Ottieni key gratuita: https://makersuite.google.com/app/apikey
```

**OpenAI GPT**:
```bash
export OPENAI_API_KEY='your-openai-key'
```

**Ollama (locale)**:
```bash
ollama pull llama2
ollama serve
```

---

## 🏗 Architettura

### Flusso Dati

```
Wikipedia Stream (EventStream API)
          ↓
    6_producer.py
          ↓
    Kafka Topic: 'wiki-changes'
          ↓
    7_stream_processor.py
       ↓         ↓
   Neo4j    Hotspot Detection
   Query    (window 2.5 min)
       ↓         ↓
    Kafka Topic: 'cluster-alerts'
          ↓
    9_llm_consumer.py
       ↓         ↓
   Neo4j    LLM Analysis
 Enrichment  (Gemini/GPT/Ollama)
       ↓
   llm_analysis.log
```

### Componenti

| Script | Funzione |
|--------|----------|
| `8_mock_producer.py` | Genera eventi Wikipedia di test |
| `7_stream_processor.py` | Rileva hotspot (>5 edit/2.5min) |
| `9_llm_consumer.py` | Analizza hotspot con AI |
| `check_system.py` | Verifica prerequisiti |

### Testing Tools

| File | Descrizione |
|------|-------------|
| `tests/simple_alert_consumer.py` | Monitor allarmi senza LLM |
| `tests/kafka_consumer.py` | Debug stream Kafka |
| `TESTING_GUIDE.md` | Guida testing completa |

---
