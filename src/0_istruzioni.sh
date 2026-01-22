#!/bin/bash
# Guida all'esecuzione della pipeline WikiGraph

# 1. Configurazione ambiente
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cd src

# 2. Download e parsing dati Wikipedia
sh 1_download_wikipedia_files.sh 
sh 2_parse_file.sh

# 3. Generazione grafo e caricamento Neo4j
gcc 3_snowball.c -o snowball $(pkg-config --cflags --libs glib-2.0)
./snowball ../data/finalmap.csv
docker compose up -d
python 4_load_graph.py 3

# 4. Community Detection e pulizia
python 5_community_detection.py --leiden
sh 6_translate_ids.sh
python 7_clean_file.py 3 --buffer-size 10000
python 8_add_node_info.py 3

# 5. Generazione Mock ed Embedding
python 10_generate_mocks_from_nodes.py
python 11_reset_embeddings.py
python 12_embed_trusted_sources.py

# 6. Addestramento Classificatori
python 13_train_neural_complete.py
python 14_train_neural_no_rag.py
python 15_train_neural_no_comment.py
python 16_train_neural_only_new.py
python 17_train_neural_minimal.py

# 7. Esecuzione Test Real-time
sh 600_open_all_testing_terminals.sh

# 8. Analisi Risultati
python 700_compare_models.py