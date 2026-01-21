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
se hai già i file in ../data appost, sennò dai, scarica bene
ora tutti i csv ecc. saranno in ../data
-----------------------------------------------------------------------------------
`


sh 1_download_wikipedia_files.sh 


sh 2_parse_file.sh


gcc 3_snowball.c -o snowball $(pkg-config --cflags --libs glib-2.0)
./snowball ../data/finalmap.csv


docker compose up -d

## per resettare i container
## docker compose down -v


<< `
-----------------------------------------------------------------------------------
IMPORTANTE:
Lo script 11_embed_trusted_sources.py è hardcodato per usare il SAMPLE 1.

Lo script 4 riprova ogni 3 secondi se docker sta ancora dormendo.

poi estraiamo il contenuto vero dal dump XML gigante.
Buffer size aumentato p'un ci minta na vita
-----------------------------------------------------------------------------------
`

py 4_load_graph.py 3


py 5_community_detection.py --leiden


sh 6_translate_ids.sh


py 7_clean_file.py 3 --buffer-size 10_000


py 8_add_node_info.py 3


<< `
-----------------------------------------------------------------------------------
FASE AI & GENERAZIONE
Qui servono le API KEY nel .env.
Generiamo le pagine "trusted" e gli edit finti (legit e vandal).
-----------------------------------------------------------------------------------
`


py 10_generate_mocks_from_nodes.py

py 11_reset_embeddings.py

py 12_embed_trusted_sources.py


<< `
-----------------------------------------------------------------------------------
FASE TRAINING
Addestriamo il multi layer perceptron.
-----------------------------------------------------------------------------------
`


py 13_train_neural_classifier.py
py 14_train_neural_classifier_without_rag_scores.py


<< `
-----------------------------------------------------------------------------------
FASE RUNTIME
Adesso devi avviare prima lo stream processor e l'ai judge altrimenti 
non sono pronti a ricevere le modifiche.

puoi eseguire il file 700_open_all_testing_terminals.sh per avviare tutti i terminali separati.

oppure te li apri in ordine dal 200 al 204 a mano, CIASCUNO su un terminale diverso.
-----------------------------------------------------------------------------------
`


sh 700_open_all_testing_terminals.sh


<< `
-----------------------------------------------------------------------------------
Quando parte il mock producer (Terminale 3), vedrai il traffico scorrere.
Se superi la soglia di edit per community, scatta l'allarme nel Terminale 1
e i giudici nel Terminale 2 inizieranno a sputare sentenze.

Alla fine, per vedere chi ha vinto:
-----------------------------------------------------------------------------------
`


py 500_compare_models.py