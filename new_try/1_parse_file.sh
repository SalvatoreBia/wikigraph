
# extract page id and title from itwiki-latest-page.sql

rg -o "^INSERT INTO.* VALUES \(([0-9]+),0,'(\w+)',0," --replace '$1,$2' itwiki-latest-page.sql > page_nodes.csv