#!/bin/zsh



scripts=(
    "200_stream_processor.py"
    "201_ai_judge_gemini.py"
    "202_neural_judge.py"
    "203_neural_judge_without_rag_scores.py"
    "204_mock_producer.py"
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

for script in "${scripts[@]}"; do
    echo "Lancio $script in $TERMINAL_APP..."
    
    CMD="print -P '%F{green}Avvio $script...%f'; trap 'print -P \"%F{red}Terminato.%f\"; exit 0' SIGINT SIGTERM; python $script; print -P '%F{yellow}Script terminato. Premi Enter per chiudere o usa il terminale.%f'; read; exit 0"

    if [[ "$TERMINAL_APP" == "konsole" ]]; then
        konsole --workdir "$PWD" -e zsh -c "$CMD" &
    
    elif [[ "$TERMINAL_APP" == "ptyxis" ]]; then
        ptyxis --new-window --working-directory "$PWD" -- zsh -c "$CMD" &
    fi
    
    sleep 1
done

echo "Tutti i servizi sono stati lanciati."