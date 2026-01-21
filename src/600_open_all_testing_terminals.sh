#!/bin/zsh

# Array di script con flag di auto-chiusura
# Format: "script_name:autoclose" (autoclose=1 chiude automaticamente dopo esecuzione)
scripts=(
    "199_reset_kafka.py:1"
    "200_stream_processor.py:0"
    "201_ai_judge_gemini.py:0"
    "202_neural_judge.py:0"
    "203_neural_judge_without_rag_scores.py:0"
    "204_mock_producer.py:1"
)


if command -v konsole &> /dev/null; then
    TERMINAL_APP="konsole"
    echo "Rilevato terminale: Konsole"
elif command -v ptyxis &> /dev/null; then
    TERMINAL_APP="ptyxis"
    echo "Rilevato terminale: Ptyxis"
else
    echo "Errore: Nessun terminale supportato trovato (né Konsole né Ptyxis)."
    exit 1
fi

for entry in "${scripts[@]}"; do
    # Parse script name and autoclose flag
    script="${entry%%:*}"
    autoclose="${entry##*:}"
    
    echo "Lancio $script in $TERMINAL_APP..."
    
    if [[ "$autoclose" == "1" ]]; then
        # Auto-close: esegue e chiude automaticamente
        CMD="print -P '%F{green}Avvio $script...%f'; trap 'print -P \"%F{red}Terminato.%f\"; exit 0' SIGINT SIGTERM; python $script; print -P '%F{yellow}Script terminato. Chiusura automatica in 2 secondi...%f'; sleep 2; exit 0"
    else
        # Keep open: attende input utente
        CMD="print -P '%F{green}Avvio $script...%f'; trap 'print -P \"%F{red}Terminato.%f\"; exit 0' SIGINT SIGTERM; python $script; print -P '%F{yellow}Script terminato. Premi Enter per chiudere o usa il terminale.%f'; read; exit 0"
    fi

    if [[ "$TERMINAL_APP" == "konsole" ]]; then
        # --separate apre finestre separate invece di tab
        konsole --separate --workdir "$PWD" -e zsh -c "$CMD" &
    
    elif [[ "$TERMINAL_APP" == "ptyxis" ]]; then
        ptyxis --new-window --working-directory "$PWD" -- zsh -c "$CMD" &
    fi
    
    # Attesa differenziata: più tempo dopo 199 per far resetare Kafka
    if [[ "$script" == "199_reset_kafka.py" ]]; then
        echo "  ⏳ Attendo reset Kafka..."
        sleep 3
    elif [[ "$script" == "203_neural_judge_without_rag_scores.py" ]]; then
        # Attendo che tutti i judge carichino i modelli prima di avviare il producer
        echo "  ⏳ Attendo caricamento modelli neurali..."
        sleep 8
    else
        sleep 1
    fi
done

echo "Tutti i servizi sono stati lanciati."