#!/bin/zsh

scripts=(
    "199_reset_kafka.py:1"
    "200_stream_processor.py:0"
    "202_ai_judge_gemini.py:0"
    "203_neural_judge.py:0"
    "204_neural_judge_no_rag.py:0"
    "205_neural_judge_no_comment.py:0"
    "206_neural_judge_only_new.py:0"
    "207_neural_judge_minimal.py:0"
    #"300_mock_producer.py:1"
)

if command -v konsole &> /dev/null; then
    TERMINAL_APP="konsole"
    echo "- Rilevato terminale: Konsole"
elif command -v ptyxis &> /dev/null; then
    TERMINAL_APP="ptyxis"
    echo "- Rilevato terminale: Ptyxis"
else
    echo "! Errore: Nessun terminale supportato trovato (Konsole o Ptyxis)."
    exit 1
fi

for entry in "${scripts[@]}"; do
    script="${entry%%:*}"
    autoclose="${entry##*:}"
    
    echo "- Lancio $script in $TERMINAL_APP..."
    
    if [[ "$autoclose" == "1" ]]; then
        CMD="print -P '%F{green}Avvio $script...%f'; trap 'print -P \"%F{red}Terminato.%f\"; exit 0' SIGINT SIGTERM; python $script; print -P '%F{yellow}Script terminato. Chiusura automatica in 2 secondi...%f'; sleep 2; exit 0"
    else
        CMD="print -P '%F{green}Avvio $script...%f'; trap 'print -P \"%F{red}Terminato.%f\"; exit 0' SIGINT SIGTERM; python $script; print -P '%F{yellow}Script terminato. Premi Invio per chiudere o usa il terminale.%f'; read; exit 0"
    fi

    if [[ "$TERMINAL_APP" == "konsole" ]]; then
        konsole --separate --workdir "$PWD" -e zsh -c "$CMD" &
    
    elif [[ "$TERMINAL_APP" == "ptyxis" ]]; then
        ptyxis --new-window --working-directory "$PWD" -- zsh -c "$CMD" &
    fi
    
    if [[ "$script" == "199_reset_kafka.py" ]]; then
        echo "  - Attendo reset Kafka..."
        sleep 3
    elif [[ "$script" == "207_neural_judge_minimal.py" ]]; then
        echo "  - Attendo caricamento modelli neurali..."
        sleep 10
    else
        sleep 1
    fi
done

echo "- Tutti i servizi lanciati."