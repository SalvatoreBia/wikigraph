
# scaricati i comandi ripgrep

# extract page id and title from itwiki-latest-page.sql
rg -oP "\(([0-9]+),0,'([^']+)',0," --replace '$1,$2' ../data/itwiki-latest-page.sql > pagemap.csv


# extract from itwiki-latest-pagelinks.sql the source and target page ids
rg -oP "\(([0-9]+),0,([0-9]+)" --replace '$1,$2' ../data/itwiki-latest-pagelinks.sql > linkmap.csv


# Estrae solo la prima colonna (gli ID) da pagemap.csv e la salva
cut -d, -f1 pagemap.csv > valid_ids.txt



# Filtra linkmap.csv per mantenere solo le righe con ID validi in entrambe le colonne
awk -F, '
    NR == FNR { 
        valid[$1] = 1; 
        next 
    } 

    ($1 in valid) && ($2 in valid)
' valid_ids.txt linkmap.csv > finalmap.csv