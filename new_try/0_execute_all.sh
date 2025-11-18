
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


echo "Mo apri il file "0_execute_all.sh" e leggi i commenti da riga 24 in poi su come continuare, cugghiunazzo"

# adesso devi avviare prima lo stream processor e l'ai judge altrimenti non sono pronti a ricevere le modifiche 
# quindi, apriti 3 terminali separati (puoi farlo pure in vscode bastardo, che cazzo alzi gli occhi)
# ed esegui in Terminale1
# py 9_stream_processory.py

# esegui in Terminale2
# py 10_ai_judge_gemini.py

# esegui in Terminale3
# py 8_mock_producer.py

# quando ti viene chiesto dal mock producer cosa vuoi generare genera prima il legittimo e vedi che 
# prima parte lo stream processor a ricevere gli edit e appena scatta l'allarme (4 edit in poco tempo)
# passa ogni edit all'ai che, insieme alla pagina "web_source_tennis.html" decide se gli edit sono legittimi o meno

# poi fai l'altra scelta, quella del vandalico e vedi che, anche qui, al 4 edit si triggera e stavolta gemini rileva subito
# che sono tentativi di vandalismo
