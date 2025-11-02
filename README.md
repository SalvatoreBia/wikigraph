# WikiGraph - Sistema di Rilevamento Hotspot Wikipedia

Sistema di analisi in tempo reale dello stream di modifiche di Wikipedia italiana, con rilevamento automatico di hotspot di attività e analisi tramite LLM.

## 📋 Indice

1. [Setup Iniziale](#setup-iniziale)
2. [Esecuzione Pipeline](#esecuzione-pipeline)
3. [Testing e Analisi LLM](#testing-e-analisi-llm)
4. [Architettura](#architettura)

---

## 🚀 Setup Iniziale

### 1. Download Dati Wikipedia

Sito coi dump di Wikipedia:
```
https://dumps.wikimedia.org/itwiki/latest/
```

**Scegli una delle opzioni:**

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
pip install -r requirements.txt
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

## 🧪 Testing e Analisi LLM

### Quick Start

**Verifica che tutto sia pronto:**
```bash
python src/check_system.py
```

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
# Scegli opzione 1: Edit War
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

### Guida Completa

Per dettagli su scenari di test, troubleshooting e metriche:

**📖 Leggi [TESTING_GUIDE.md](TESTING_GUIDE.md)**

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

## 📊 Esempi di Output

### Allarme Rilevato (7_stream_processor.py)
```
=== ALLARME: HOTSPOT RILEVATO ===
Comunità: 42
Numero modifiche: 18
Soglia: 5
Pagine: ['Carbonara', 'Amatriciana', 'Pasta alla gricia']
```

### Analisi LLM (9_llm_consumer.py)
```
ANALISI LLM:
1. Tema: Cucina italiana tradizionale romana
2. Causa: Dibattito ricorrente su ricette autentiche
3. Preoccupazione: 4/10 (normale dibattito culturale)
4. Azioni: Monitorare, verificare fonti
```

---

## 🛠 Troubleshooting

### Kafka non si connette
```bash
docker ps | grep kafka
docker-compose up -d
```

### Neo4j non trova comunità
```bash
# Verifica caricamento dati
docker exec -it neo4j-server-1 cypher-shell -u neo4j -p password
> MATCH (p:Page) WHERE p.community IS NOT NULL RETURN count(p);
```

### LLM non risponde
```bash
# Verifica API key
echo $GEMINI_API_KEY

# O usa Ollama locale
ollama serve
```

---

## 📝 File Generati

- `llm_analysis.log` - Analisi LLM di tutti gli hotspot
- `neo4j-data/` - Database Neo4j persistente
- `data/` - Dump Wikipedia e mappe ID

---

## 🎓 Per il Progetto

### Esperimenti Suggeriti

1. **Comparazione LLM**: Confronta Gemini vs GPT vs Ollama
2. **Tuning Soglia**: Prova diverse soglie di allarme (3, 5, 10 edit)
3. **False Positive Rate**: Quanti allarmi sono falsi positivi?
4. **Latenza**: Tempo medio tra evento e allarme

### Metriche da Raccogliere

- Numero allarmi/ora
- Precisione analisi LLM (validazione manuale)
- Tempo risposta end-to-end
- Dimensione comunità coinvolte

---

**Autori**: WikiGraph Team  
**Università**: [Università]  
**Corso**: Big Data  
**Anno**: 2025