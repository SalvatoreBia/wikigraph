#!/bin/bash

rg -oP "\(([0-9]+),0,'([^']+)',0," --replace '$1,$2' ../data/itwiki-latest-page.sql > ../data/pagemap.csv

rg -oP "\(([0-9]+),0,([0-9]+)" --replace '$1,$2' ../data/itwiki-latest-pagelinks.sql > ../data/linkmap.csv

cut -d, -f1 ../data/pagemap.csv > ../data/valid_ids.txt

awk -F, '
    NR == FNR { 
        valid[$1] = 1; 
        next 
    } 

    ($1 in valid) && ($2 in valid)
' ../data/valid_ids.txt ../data/linkmap.csv > ../data/finalmap.csv