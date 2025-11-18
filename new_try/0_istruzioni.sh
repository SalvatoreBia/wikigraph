<< '
-----------------------------------------------------------------------------------
non eseguire questo file, bastardo
ti scrivo cosa eseguire riga per riga

vai in root ed esegi questi comandi:
-----------------------------------------------------------------------------------
'

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cd new_try


<< '
-----------------------------------------------------------------------------------
se hai già i file in ../data appost, sennò dai, scarica bene
ora tutti i csv ecc. saranno in ../data
-----------------------------------------------------------------------------------
'

sh 1_download_wikipedia_files.sh 


sh 2_parse_file.sh


gcc 3_snowball.c -o snowball $(pkg-config --cflags --libs glib-2.0)
./snowball ../data/finalmap.csv


docker compose up -d


wc -l ../data/sample_*.csv
<< '
-----------------------------------------------------------------------------------
scegli il sample più piccolo/ragionevole da caricare
e chiama il 4_load_graph.py con il numero di quel file, per esempio:

wc -l ../data/sample_*.csv
   105822 ../data/sample_0.csv
   115333 ../data/sample_1.csv
  3504657 ../data/sample_2.csv
  4698711 ../data/sample_3.csv
  8424523 total

py 4_load_graph.py 0

sicuramente trovi che docker non è pronto, non fare nulla, 
lo script riprova ogni 3 secondi fino a quando docker non si da una svegliata
-----------------------------------------------------------------------------------
'

py 4_load_graph.py <id_del_sample_che_hai_scelto>


py 5_community_detection.py --leiden


sh 6_translate_ids.sh


# questo non è essenziale, ma se vuoi avere una panoramica delle community eseguilo pure
# py 7_find_top_community_names.py

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
