
sito coi dump di wikipedia
```
https://dumps.wikimedia.org/itwiki/latest/
```

---
Leggi sto readme coglione

Scegli:
1) Unzippa i file 

```
itwiki-latest-page.sql.gz
itwiki-latest-pagelinks.sql.gz
```

e lasciali in /data/   

2) eliminali e esegui ```0_update_wikipedia_files```

basta che alla fine il tree sia così
../data
|-- empty
|-- itwiki-latest-page.sql
`-- itwiki-latest-pagelinks.sql


poi esegui gli script trovandoti in /src/ in ordine