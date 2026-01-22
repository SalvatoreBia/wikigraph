# PYTHON 3.12.7


## Report


https://www.overleaf.com/project/6970d6100a286eb9d599f640

---

non posso mettere commenti nel config.json che gli pesava il culo definire un carattere di escaper per i commenti

```json
"llm": {
    "provider": "local", // gemini o local
    "generation_model": "gemma-3-27b-it", // per gemini
    "judge_model": "gemma-3-27b-it", // per gemini
    "local": {
        "base_url": "http://localhost:1234/v1", // per local
        "api_key": "lm-studio", // per local
        "model": "openai/gpt-oss-20b" // per local
    }
},
```
