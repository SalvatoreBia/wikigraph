#!/usr/bin/env python3
"""
Script per scaricare ed estrarre i file di Wikipedia solo se ci sono stati aggiornamenti.
Verifica l'header Last-Modified per determinare se i file remoti sono più recenti.
"""

import os
import sys
import gzip
import shutil
import requests
import time
from datetime import datetime
from pathlib import Path


# URL dei file da scaricare
BASE_URL = "https://dumps.wikimedia.org/itwiki/latest/"
FILES = [
    "itwiki-latest-page.sql.gz",
    "itwiki-latest-pagelinks.sql.gz"
]

# Directory di progetto (parent della directory src)
PROJECT_DIR = Path(__file__).parent.parent
DOWNLOAD_DIR = PROJECT_DIR / "data"


def get_remote_last_modified(url):
    """
    Ottiene la data di ultima modifica del file remoto.
    
    Args:
        url: URL del file remoto
        
    Returns:
        datetime object o None se non disponibile
    """
    try:
        response = requests.head(url, allow_redirects=True, timeout=10)
        response.raise_for_status()
        
        last_modified = response.headers.get('Last-Modified')
        if last_modified:
            # Converte il formato HTTP date in datetime
            return datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S %Z')
        return None
    except Exception as e:
        print(f"Errore nel recupero dell'header per {url}: {e}")
        return None


def get_local_file_time(filepath):
    """
    Ottiene la data di modifica del file locale.
    
    Args:
        filepath: Path del file locale
        
    Returns:
        datetime object o None se il file non esiste
    """
    if os.path.exists(filepath):
        timestamp = os.path.getmtime(filepath)
        return datetime.fromtimestamp(timestamp)
    return None


def download_file(url, destination):
    """
    Scarica un file con barra di progresso.
    
    Args:
        url: URL del file da scaricare
        destination: Path di destinazione
    """
    print(f"\nScaricamento di {os.path.basename(url)}...")
    
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        block_size = 8192
        downloaded = 0
        last_print_time = 0
        
        with open(destination, 'wb') as f:
            for chunk in response.iter_content(chunk_size=block_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    # Aggiorna la barra solo ogni 0.5 secondi
                    current_time = time.time()
                    if current_time - last_print_time >= 0.5 or downloaded == total_size:
                        if total_size > 0:
                            percent = (downloaded / total_size) * 100
                            bar_length = 50
                            filled = int(bar_length * downloaded / total_size)
                            bar = '=' * filled + '-' * (bar_length - filled)
                            
                            # Converti bytes in MB
                            downloaded_mb = downloaded / (1024 * 1024)
                            total_mb = total_size / (1024 * 1024)
                            
                            print(f'\r[{bar}] {percent:.1f}% ({downloaded_mb:.1f}/{total_mb:.1f} MB)', end='', flush=True)
                            last_print_time = current_time
        
        print()  # Nuova riga dopo il completamento
        return True
        
    except Exception as e:
        print(f"\nErrore durante il download: {e}")
        if os.path.exists(destination):
            os.remove(destination)
        return False


def extract_gz_file(gz_filepath, output_filepath):
    """
    Estrae un file .gz
    
    Args:
        gz_filepath: Path del file .gz
        output_filepath: Path del file di output
    """
    print(f"Estrazione di {os.path.basename(gz_filepath)}...")
    
    try:
        with gzip.open(gz_filepath, 'rb') as f_in:
            with open(output_filepath, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"File estratto: {output_filepath}")
        return True
    except Exception as e:
        print(f"Errore durante l'estrazione: {e}")
        if os.path.exists(output_filepath):
            os.remove(output_filepath)
        return False


def needs_update(remote_url, local_sql_path):
    """
    Determina se un file ha bisogno di essere aggiornato.
    
    Args:
        remote_url: URL del file remoto
        local_sql_path: Path del file SQL locale (non compresso)
        
    Returns:
        bool: True se serve aggiornamento, False altrimenti
    """
    # Se il file locale non esiste, serve scaricare
    if not os.path.exists(local_sql_path):
        print(f"File locale {os.path.basename(local_sql_path)} non trovato.")
        return True
    
    # Ottieni le date di modifica
    remote_time = get_remote_last_modified(remote_url)
    local_time = get_local_file_time(local_sql_path)
    
    if remote_time is None:
        print(f"Impossibile determinare la data di modifica remota. Verrà scaricato per sicurezza.")
        return True
    
    print(f"\nFile: {os.path.basename(local_sql_path)}")
    print(f"  Data locale:  {local_time.strftime('%Y-%m-%d %H:%M:%S') if local_time else 'N/A'}")
    print(f"  Data remota:  {remote_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Confronta le date (con un margine di 1 secondo per differenze di arrotondamento)
    if local_time and remote_time <= local_time:
        print(f"  ✓ File già aggiornato")
        return False
    
    print(f"  ↓ Aggiornamento disponibile")
    return True


def process_file(filename):
    """
    Processa un singolo file: controlla, scarica ed estrae se necessario.
    
    Args:
        filename: Nome del file .gz da processare
        
    Returns:
        bool: True se successo, False altrimenti
    """
    url = BASE_URL + filename
    gz_path = DOWNLOAD_DIR / filename
    sql_path = DOWNLOAD_DIR / filename.replace('.gz', '')
    
    # Controlla se serve aggiornamento
    if not needs_update(url, sql_path):
        return True
    
    # Scarica il file
    if not download_file(url, gz_path):
        return False
    
    # Estrai il file
    if not extract_gz_file(gz_path, sql_path):
        return False
    
    # Rimuovi il file compresso dopo l'estrazione
    try:
        os.remove(gz_path)
        print(f"File compresso rimosso: {gz_path}")
    except Exception as e:
        print(f"Attenzione: impossibile rimuovere {gz_path}: {e}")
    
    print(f"✓ {filename} aggiornato con successo!\n")
    return True


def main():
    """Funzione principale"""
    print("=" * 70)
    print("Script di aggiornamento file Wikipedia")
    print("=" * 70)
    print(f"Directory di lavoro: {DOWNLOAD_DIR}\n")
    
    # Crea la directory data se non esiste
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    
    success_count = 0
    total_files = len(FILES)
    
    for filename in FILES:
        if process_file(filename):
            success_count += 1
        else:
            print(f"✗ Errore nel processamento di {filename}\n")
    
    # Riepilogo finale
    print("=" * 70)
    print(f"Completato: {success_count}/{total_files} file processati con successo")
    print("=" * 70)
    
    return 0 if success_count == total_files else 1


if __name__ == "__main__":
    sys.exit(main())
