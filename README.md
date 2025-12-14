
L'obiettivo è quello di creare un sistema per rilevare in tempo reale se le modifiche su wikipedia sono modifiche legittime o vandaliche.

Non avendo le possibilità di hostare l'intera wikipedia sui nostri computer e testare le modifiche in tempo reale abbiamo scaricato i dump di wikipedia italia che comprendono un elenco dei collegamenti tra le pagine e titoli.
Script `1_download_wikipedia_files.sh` e `2_parse_file.sh`

Nello script `3_snowball.c` implementiamo lo snowball con una BFS limitata a K=2 partendo da 4 nodi casuali per catturare dei piccoli sottografi del dump appena scaricato così da avere una dimensione ragionevole da caricare in neo4j, con lo script `4_load_graph.py`, su cui lavorare mantenendo comunque i reali collegamenti e la sua struttura divisa in comunità

Una volta che i dati sono su neo4j calcoliamo le comunità con lo script `5_community_detection.py` usando `Leiden`.

lo script `6_translate_ids.sh` sfrutta l'hashmap in pagemap.csv per tradurre gli id del sample_X.csv selezionato dando quindi un titolo ai collegamenti del nostro sample

Nello script `7_clean_file.py` sfrtutiamo quei titoli per estrarre dal dump xml di wikipedia il contenuto vero e proprio delle pagine html corrispondenti a quei titoli, contenuto che nello script `8_add_node_info.py` viene caricato su neo4j al nodo corrispondente 

Con lo script `10_generate_mocks_from_nodes.py` chiamiamo l'api di google `gemma-3-27B-it` (l'unica affordable al momento) e generiamo:
- `N` "trusted sources"
- `TARGET_LEGIT_EDITS` edit legittimi
- `TARGET_VANDAL_EDITS` edit vandalici


Le trusted sources sarebbero le pagine html (nel nostro caso dei mock) di una testata giornalistica di cui wikipedia si fida e da affiancare ad un LLM per superare il problema del knowledge cutoff, usiamo quindi role prompting per dare a gemma il ruolo di giornalista e generare delle pagine html che rappresentano le pagine di una giornalista.

Le trusted sources nel nostro caso vengono scelte come segue:
- filtriamo tutti i nodi che non fanno parte di una community o che hanno un contenuto troppo corto (<100 caratteri)
- prendiamo gli `N` nodi più rilevanti del nostro sample, ovvero gli `N` (configurabile) nodi con grado più alto dalla community più popolosa del sample.
- chiediamo a gemma di generare una pagina HTML per ciascuno dei nodi selezionati. La Trusted Source è generata a partire dal Titolo.
- Per dare del contesto a gemma sul contenuto da scrivere usiamo i primi 2000 caratteri del contenuto reale della pagina.



Gli edit sono in formato json e seguono questa struttura:

```json
{
  "id": "",
  "type": "",
  "title": "",
  "user": "",
  "comment": "",
  "original_text": "",
  "new_text": "",
  "timestamp": 0,
  "length": {
      "old": 0,
      "new": 0
  },
  "is_vandalism": false,
  "meta": {
      "domain": "",
      "uri": ""
  }
}
```

L'LLM riceve come contesto una finestra casuale di 600 caratteri (configurabile) del contenuto reale della pagina (estratta dal grafo) e la usa per generare un edit.
Genererà in totale `TARGET_LEGIT_EDITS` e `TARGET_VANDAL_EDITS` divisi equamente per ciascuno degli `N` topic.


Nel file `11_embed_trusted_sources.py` leggiamo il contenuto delle pagine wikipedia dei nodi salvati su neo4j durante lo script `8_add_node_info.py`, dividiamo il testo in chunk di 1000 caratteri con un overlap di 100 caratteri e li embeddiamo con il modello di embedding configurato in `config.json`.
Gli embedding verranno salvati su neo4j nei nodi :Chunk e :TrustedChunk


Nel file `12_train_binary_classifier.py` addestriamo un classificatore binario classico testando contemporaneamente Regressione Logistica, Random Forest, SVM e Gradient Boosting per distinguere tra edit legittimi e vandalici. 
Il sistema carica il dataset generato ed estrae per ogni edit un vettore di feature ingegnerizzato (772 dimensioni totali) composto da:
- Semantic Delta (384 dim): La differenza vettoriale tra l'embedding del nuovo testo e quello vecchio (rappresenta la "direzione" del cambiamento semantico).
- Comment Embedding (384 dim): Il significato del commento lasciato dall'utente.
- Similarità del Testo & Length Ratio: Metriche statistiche sul cambiamento apportato.
- Truth Scores (RAG): Qui avviene la "Triangolazione". Il sistema interroga Neo4j per trovare i nodi 
    - :Chunk (contenuto della pagina di Wikipedia) 
    - :TrustedChunk (contenuto dei file html generati in `trusted_html_pages`) 
    più simili al testo modificato. Il punteggio di similarità (Cosine Similarity) diventa una feature: se il testo dell'utente si discosta troppo dalla fonte affidabile o dal contesto originale, il punteggio cala, segnalando un potenziale vandalismo.


Lo script esegue una Cross-Validation, seleziona il modello migliore e lo salva come `binary_classifier.pkl`.

In alternativa, lo script `13_train_neural_classifier.py` addestra una Rete Neurale con PyTorch. 
Prende in input gli embedding grezzi concatenati (Vecchio Testo + Nuovo Testo + Commento + Truth Scores) e il modello viene salvato come `neural_classifier.pth`.

Per iniziare a ricevere le modifiche in tempo reale, eseguiamo lo script `200_stream_processor.py` che mette kafka in ascolto sul topic `"wiki-changes"` su cui resta in ascolto contando quante modifiche appartenenti alla stessa community arrivano in un certo intervallo di tempo.
Se si supera una certa soglia Y di modifiche entro un tempo T allora si passa in uno "stato di allarme" in cui le modifiche vengono inviate contemporaneamente al `201_ai_judge_gemini.py` e al `202_bc_judge.py` che classificheranno l'edit e salveranno il risultato di ogni classificazione.

Con lo script `500_compare_models.py` possiamo confrontare le due classificazioni e vedere quale modello è più preciso.

Tutti gli edit vengono spediti sul topic kafka dallo script `203_mock_producer.py` che legge i file `/data/mocked_edits/legit_edits.json` e `/data/mocked_edits/vandal_edits.json` e li invia sul topic `wiki-changes`.

Per svuotare il topic kafka si può usare l'utility `199_reset_kafka.py`