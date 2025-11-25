
# scaricati i comandi ripgrep

# extract page id and title from itwiki-latest-page.sql
rg -oP "\(([0-9]+),0,'([^']+)',0," --replace '$1,$2' ../data/itwiki-latest-page.sql > ../data/pagemap.csv


# extract from itwiki-latest-pagelinks.sql the source and target page ids
rg -oP "\(([0-9]+),0,([0-9]+)" --replace '$1,$2' ../data/itwiki-latest-pagelinks.sql > ../data/linkmap.csv


# Estrae solo la prima colonna (gli ID) da pagemap.csv e la salva
cut -d, -f1 ../data/pagemap.csv > ../data/valid_ids.txt



# Filtra linkmap.csv per mantenere solo le righe con ID validi in entrambe le colonne
awk -F, '
    NR == FNR { 
        valid[$1] = 1; 
        next 
    } 

    ($1 in valid) && ($2 in valid)
' ../data/valid_ids.txt ../data/linkmap.csv > ../data/finalmap.csv




# comando da eseguire post snowball per contare quanti nodi
# awk -F, 'NR > 1 { nodes[$1]++; nodes[$2]++ } END { print length(nodes) }' ../data/sample/sample_3.csv