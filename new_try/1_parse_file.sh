
# scaricati i comandi ripgrep e xsv


# extract page id and title from itwiki-latest-page.sql
rg -oP "\(([0-9]+),0,'(\w+)',0," --replace '$1,$2' itwiki-latest-page.sql > id_name_pagemap.csv


# extract from itwiki-latest-pagelinks.sql the source and target page ids
rg -oP "\(([0-9]+),0,([0-9]+)" --replace '$1,$2' itwiki-latest-pagelinks.sql > id_to_id_pagemap.csv


# Estrae solo la prima colonna (gli ID) da a.csv e la salva
cut -d, -f1 a.csv > ids_validi.txt