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

sh 1_download_wikipedia_files.sh 

sh 2_parse_file.sh

gcc 3_snowball.c -o snowball $(pkg-config --cflags --libs glib-2.0)
./snowball ../data/finalmap.csv

docker compose up -d

python 4_load_graph.py 3

python 5_community_detection.py --leiden

sh 6_translate_ids.sh

python 7_clean_file.py 3 --buffer-size 10000

python 8_add_node_info.py 3

python 10_generate_mocks_from_nodes.py

python 11_reset_embeddings.py

python 12_embed_trusted_sources.py

python 13_train_neural_complete.py

python 14_train_neural_no_rag.py

python 15_train_neural_no_comment.py

python 16_train_neural_only_new.py

python 17_train_neural_minimal.py

sh 600_open_all_testing_terminals.sh

python 700_compare_models.py