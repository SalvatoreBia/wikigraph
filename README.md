
L'obiettivo è quello di creare un sistema per rilevare in tempo reale se le modifiche su wikipedia sono modifiche legittime o vandaliche.

Non avendo le possibilità di hostare l'intera wikipedia sui nostri computer e testare le modifiche in tempo reale abbiamo scaricato i dump di wikipedia italia che comprendono un elenco dei collegamenti tra le pagine e titoli.
Script `1_download_wikipedia_files.sh` e `2_parse_file.sh`

Nello script `3_snowball.c` implementiamo lo snowball sampling per catturare dei piccoli sottografi del dump appena scaricato così da avere una dimensione ragionevole da caricare in neo4j, con lo script `4_load_graph.py`, su cui lavorare mantenendo comunque i reali collegamenti e la sua struttura divisa in comunità

Una volta che i dati sono su neo4j calcoliamo le comunità con lo script `5_community_detection.py` usando `Leiden`.

lo script `6_translate_ids.sh` sfrutta l'hashmap in pagemap.csv per tradurre gli id del sample_X.csv selezionato dando quindi un titolo ai collegamenti del nostro sample

Nello script `7_clean_file.py` sfrtutiamo quei titoli per estrarre dal dump xml di wikipedia il contenuto vero e proprio delle pagine html corrispondenti a quei titoli, contenuto che nello script `8_add_node_info.py` viene caricato su neo4j al nodo corrispondente 

Con lo script `10_generate_mocks_from_nodes.py` chiamiamo l'api di google `gemma-3-27B` (l'unica affordable al momento) e generiamo:
- `N` "trusted sources"
- `TARGET_LEGIT_EDITS` edit legittimi
- `TARGET_VANDAL_EDITS` edit vandalici

Le trusted sources sarebbero le pagine html (nel nostro caso dei mock) di una testata giornalistica di cui wikipedia si fida e da affiancare ad un LLM per superare il problema del knowledge cutoff
Le trusted sources nel nostro caso vengono scelte come segue:
- filtriamo tutti i nodi che non fanno parte di una community o che hanno un contenuto troppo corto (<100 caratteri)
    - prendiamo gli `N` nodi più rilevanti del nostro sample, ovvero gli `N` (configurabile) nodi con grado più alto dalla community più popolosa del sample.
    - chiediamo a gemma di generare una pagina HTML per ciascuno dei nodi selezionati. La Trusted Source è generata a partire dal Titolo.


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

