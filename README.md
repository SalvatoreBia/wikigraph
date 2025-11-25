# WikiGraph - Procedura Operativa

Usa questi passaggi nell'ordine indicato per riprodurre l'ambiente e l'intero flusso.

## 0. Preparazione Ambiente

Esegui dalla root del repository:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cd src
```

## 1. Download / Presenza File Dati

Se i file necessari sono già presenti in `../data/` salta questo step. Altrimenti esegui:

```bash
sh 1_download_wikipedia_files.sh
```

Alla fine avrai i file `.sql` e i CSV richiesti dentro `../data/`.

## 2. Parsing dei Dump

```bash
sh 2_parse_file.sh
```

Genera le mappe e i CSV intermedi (in particolare `finalmap.csv`).

## 3. Compilazione e Esecuzione Snowball

Compila il C worker che arricchisce / trasforma i dati:

```bash
gcc 3_snowball.c -o snowball $(pkg-config --cflags --libs glib-2.0)
./snowball ../data/finalmap.csv
```

## 4. Avvio Stack Docker

Avvia i servizi (Neo4j / altro definito in `docker-compose.yml`):

```bash
docker compose up -d
```

## 5. Valutazione dei Sample

Conta le righe dei diversi sample generati:

```bash
wc -l ../data/sample/sample_*.csv
```

Output di esempio:

```
   105822 ../data/sample/sample_0.csv
   115333 ../data/sample/sample_1.csv
  3504657 ../data/sample/sample_2.csv
  4698711 ../data/sample/sample_3.csv
  8424523 total
```

Scegli il sample più piccolo o ragionevole da caricare (nell'esempio `0`).

## 6. Caricamento Grafo

Lo script riprova ogni 3 secondi se Docker/Neo4j non è pronto. Usa il numero del sample scelto (qui 0):

```bash
py 4_load_graph.py 0
```

Se non hai l'alias `py`, sostituisci con `python`.

## 7. Rilevazione Comunità

```bash
py 5_community_detection.py --leiden
```

Esegue la community detection (algoritmo Leiden se presente l'opzione).

## 8. Traduzione ID -> Titoli

```bash
sh 6_translate_ids.sh
```

Genera versioni con titoli leggibili.

## 9. (Opzionale) Panoramica Nomi Comunità

```bash
py 7_find_top_community_names.py
```

Serve solo per avere una lista sintetica dei nomi.

## 10. Pulizia / Normalizzazione File

```bash
py 8_clean_file 0 --no-clean --buffer-size 10_000
```

Il parametro `0` corrisponde al sample scelto. L'opzione `--no-clean` mantiene i dati grezzi; `--buffer-size` regola il batch.

## 11. Aggiunta Embeddings

```bash
py 9_add_embeddings.py
```

Arricchisce i dati con vettori (LLM / modello embeddings configurato nei requirements).

## 12. Avvio Componenti di Streaming e AI

Apri tre terminali separati (ordinati). Prima avvia Stream Processor e AI Judge, poi il Mock Producer.

Terminale 1:
```bash
py 11_stream_processory.py
```

Terminale 2:
```bash
py 12_ai_judge_gemini.py
```

Terminale 3:
```bash
py 10_mock_producer.py
```

## 13. Test Interattivo (Mock Producer)

Quando il `mock_producer` ti chiede cosa generare:
- Scegli prima lo scenario "legittimo": vedrai che lo Stream Processor riceve gli edit e al 4° edit scatta l'allarme passando gli eventi all'AI Judge, che con la pagina di contesto (`web_source_tennis.html`) valuta la legittimità.
- Poi prova lo scenario "vandalico": al 4° edit scatta l'allarme e Gemini rileva subito il tentativo di vandalismo.
- Puoi infine testare lo scenario "misto" (funziona in modo analogo).

## Note e Suggerimenti

1. Se Docker non è pronto al passo di caricamento grafo, non interrompere: il processo ritenta automaticamente.
2. Usa il sample 0 per rapidità, gli altri sono molto più grandi.
3. `py` è un alias comune; se non definito rimpiazza con `python`.
4. Mantieni attivi i tre terminali per osservare in tempo reale l'interazione tra producer, stream processor e AI judge.

## Riepilogo Rapido Comandi (sequenza sintetica)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cd src
sh 1_download_wikipedia_files.sh        # solo se mancano i file
sh 2_parse_file.sh
gcc 3_snowball.c -o snowball $(pkg-config --cflags --libs glib-2.0)
./snowball ../data/finalmap.csv
docker compose up -d
wc -l ../data/sample/sample_*.csv       # scegli N (es. 0)
py 4_load_graph.py 0
py 5_community_detection.py --leiden
sh 6_translate_ids.sh
py 7_find_top_community_names.py        # opzionale
py 8_clean_file 0 --no-clean --buffer-size 10_000
py 9_add_embeddings.py
py 11_stream_processory.py              # Terminale 1
py 12_ai_judge_gemini.py                # Terminale 2
py 10_mock_producer.py                  # Terminale 3
```

Segui pedissequamente e otterrai il comportamento previsto (trigger al 4° edit, analisi AI, distinzione legittimo/vandalico/misto).

