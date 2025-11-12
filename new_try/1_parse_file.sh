
# extract page id and title from itwiki-latest-page.sql

rg -oP "\(([0-9]+),0,'(\w+)',0," --replace '$1,$2' itwiki-latest-page.sql > id_name_pagemap.csv


