
L'obiettivo è quello di creare un sistema per rilevare in tempo reale se le modifiche su wikipedia sono modifiche legittime o vandaliche.

Non avendo le possibilità di hostare l'intera wikipedia sui nostri computer e testare le modifiche in tempo reale abbiamo scaricato i dump di wikipedia italia che comprendono un elenco dei collegamenti tra le pagine, titoli e contenuto.

Utilizziamo lo snowball sampling per catturare dei piccoli sottografi di tutta wikipedia italia così da avere una dimensione ragionevole su cui lavorare ma che mantenga comunque i reali collegamenti e la sua struttura divisa in comunità

Per calcolare le community calcoliamo il sample più piccolo generato dallo snowball su neo4j e usiamo Leiden.

Con lo script `10_generate_mocks_from_nodes.py` chiamiamo l'api di google gemma-3-27B (l'unica affordable al momento) e generiamo:
- N "trusted sources"
- 100 edit legittimi
- 100 edit vandalici

Le trusted sources sarebbero le pagine html (nel nostro caso dei mock) di una testata giornalistica di cui wikipedia si fida e da affiancare ad un LLM per superare il problema del knowledge cutoff
Le trusted sources nel nostro caso vengono scelte come segue:
- filtriamo tutti i nodi che non fanno parte di una community o che hanno un contenuto troppo corto (<100 caratteri)
- prendiamo gli N nodi più rilevanti del nostro sample, ovvero gli N nodi con grado più alto dalla community più popolosa del sample
- chiediamo a gemma di generare una pagina HTML per ciascuno dei nodi selezionati con delle informazioni chiare e precise, gemma ha come contesto solo 