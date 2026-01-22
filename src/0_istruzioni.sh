<< `
-----------------------------------------------------------------------------------
link dello stream di wikipedia:
https://stream.wikimedia.org/v2/stream/recentchange

non eseguire questo file, bastardo
ti scrivo cosa eseguire riga per riga

vai nella cartella root e esegui questi comandi:
-----------------------------------------------------------------------------------
`

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cd src


<< `
-----------------------------------------------------------------------------------
se hai già i file in ../data appost, sennò dai, scarica beneee
ora tutti i csv ecc. saranno in ../data
-----------------------------------------------------------------------------------
`


sh 1_download_wikipedia_files.sh 


sh 2_parse_file.sh


gcc 3_snowball.c -o snowball $(pkg-config --cflags --libs glib-2.0)
./snowball ../data/finalmap.csv


docker compose up -d

## per resettare i container invece:             docker compose down -v


<< `
-----------------------------------------------------------------------------------
Lo script 4 riprova ogni 3 secondi se docker sta ancora dormendo.

poi estraiamo il contenuto vero dal dump XML gigante.
Buffer size aumentato p'un ci minta na vita

il numero aglis cript indica quale sample usare.
-----------------------------------------------------------------------------------
`

py 4_load_graph.py 3


py 5_community_detection.py --leiden


sh 6_translate_ids.sh


py 7_clean_file.py 3 --buffer-size 10_000


py 8_add_node_info.py 3


<< `
-----------------------------------------------------------------------------------
Generiamo le pagine "trusted" e gli edit finti (legit e vandal).
-----------------------------------------------------------------------------------
`


py 10_generate_mocks_from_nodes.py

py 11_reset_embeddings.py

py 12_embed_trusted_sources.py


<< `
-----------------------------------------------------------------------------------
Addestriamo tutti i classificatori neurali, il primo è il migliore gli altri fanno progressivamente cacare
tocca capire dov'è che diventa inutile
-----------------------------------------------------------------------------------
`


py 13_train_neural_complete.py

py 14_train_neural_no_rag.py

py 15_train_neural_no_comment.py

py 16_train_neural_only_new.py

py 17_train_neural_minimal.py


<< `
-----------------------------------------------------------------------------------
o ti esegui in terminali separati uno ad uno:
# Terminale 1:
  - 199_reset_kafka.py (reset kafka topics)

# Terminale 2:
  - 200_stream_processor.py (stream processor)

# Terminale 3:
  - 202_ai_judge_gemini.py (LLM judge)

# Terminale 4:
  - 203_neural_judge.py (neural completo con RAG)

# Terminale 5:
  - 204_neural_judge_no_rag.py (neural senza RAG)

# Terminale 6:
  - 205_neural_judge_no_comment.py (neural senza commento)

# Terminale 7:
  - 206_neural_judge_only_new.py (neural solo new text)

# Terminale 8:
  - 207_neural_judge_minimal.py (neural baseline stupida)

# Terminale 9:
  - 300_mock_producer.py (producer mock edits)


oppure lancia lo script che fa tutto al posto tuo
-----------------------------------------------------------------------------------
`


sh 600_open_all_testing_terminals.sh


<< `
-----------------------------------------------------------------------------------
Il mock producer pusha modifiche ad hoc
appena supera la soglia di edit per community, scatta l'allarme nel Terminale ??
e i giudici iniziano a valutare.


Alla fine, per le comparazioni dei risultati:
-----------------------------------------------------------------------------------
`

py 700_compare_models.py