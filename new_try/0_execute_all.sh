
sh 1_download_wikipedia_files.sh 

sh 2_parse_file.sh

./snowball_app finalmap.csv

docker compose up -d

# ci mette un po' docker a svegliarsi ao
time 30

py 4_load_graph.py

py 5_community_detection.py --leiden

sh 6_translate_ids.sh

py 7_find_top_community_names.py



