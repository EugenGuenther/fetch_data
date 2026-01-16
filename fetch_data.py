import os
import json
import requests
import sys
from datetime import datetime, timedelta

# --- 1. KONFIGURATION ---
# Holt den Key sicher aus den Umgebungsvariablen (durch GitHub Actions injiziert)
API_KEY = os.environ.get('FRED_API_KEY')

BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
# Historie: Wir laden 400 Tage, um sicher genug Puffer für 200-Tage-Durchschnitte zu haben
START_DATE = (datetime.now() - timedelta(days=3653)).strftime('%Y-%m-%d')

# Mapping: Dein App-Name -> FRED Series ID
SERIES_MAP = {
    "WALCL": "WALCL",           # Fed Assets
    "WTREGEN": "WTREGEN",       # Treasury Account
    "RRP": "RRPONTSYD",         # Reverse Repo
    "DGS10": "DGS10",           # 10Y Treasury Yield
    "DBAA": "DBAA"              # Corp Bonds
}

def fetch_series(series_id):
    """
    Lädt eine Serie herunter.
    Return: Liste von Datenpunkten ODER None bei Fehler.
    """
    # Sicherheits-Check vor jedem Request
    if not API_KEY:
        print("CRITICAL: API Key fehlt im Skript-Aufruf!")
        return None

    params = {
        "series_id": series_id,
        "api_key": API_KEY,
        "file_type": "json",
        "observation_start": START_DATE
    }
    
    # Header setzen, damit wir seriös wirken
    headers = {
        "User-Agent": "SentinelApp/1.0 (GitHub Actions Mirror)"
    }
    
    try:
        # Timeout verhindert Hängenbleiben (30 Sek)
        response = requests.get(BASE_URL, params=params, headers=headers, timeout=300)
        
        # HTTP Fehler abfangen (z.B. 403 Forbidden bei falschem Key)
        if response.status_code != 200:
            print(f"ERROR {series_id}: HTTP {response.status_code}")
            # Nur die ersten 100 Zeichen der Fehlermeldung zeigen (Sicherheit)
            print(f"Server Msg: {response.text[:100]}") 
            return None
            
        data = response.json()
        
        # Validierung: Ist das JSON leer?
        if "observations" not in data:
            print(f"ERROR {series_id}: Keine 'observations' im JSON.")
            return None

        clean_observations = []
        for obs in data["observations"]:
            # FRED nutzt "." für fehlende Daten -> Überspringen
            if obs["value"] != ".": 
                clean_observations.append({
                    "d": obs["date"],
                    "v": float(obs["value"])
                })
        
        if not clean_observations:
            print(f"WARNING {series_id}: Datenliste ist leer.")
            return None
            
        return clean_observations

    except Exception as e:
        print(f"EXCEPTION {series_id}: {str(e)}")
        return None

def main():
    print(f"--- Starte Sentinel Update: {datetime.now()} ---")
    
    if not API_KEY:
        print("ABBRUCH: 'FRED_API_KEY' Umgebungsvariable nicht gefunden.")
        sys.exit(1) # Rotes Kreuz in GitHub

    temp_storage = {}
    success = True

    # 1. LOOP: Alle Daten laden
    for name, series_id in SERIES_MAP.items():
        print(f"Lade {name}...")
        result = fetch_series(series_id)
        
        if result is None:
            print(f"ABBRUCH: Fehler bei {name}. Update gestoppt.")
            success = False
            break
        
        temp_storage[name] = result

    # 2. SAVE: Nur speichern, wenn ALLES erfolgreich war
    if success:
        output = {
            "meta": {
                "updated": datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'),
                "source": "Federal Reserve Bank of St. Louis"
            },
            "data": temp_storage
        }

        try:
            with open('sentinel_data.json', 'w') as f:
                json.dump(output, f) # Minified JSON (ohne Leerzeichen, spart Traffic)
            print("SUCCESS: Datei erfolgreich geschrieben.")
            sys.exit(0) # Grüner Haken
        except Exception as e:
            print(f"DISK ERROR: Konnte Datei nicht schreiben: {e}")
            sys.exit(1)
    else:
        print("FAIL-SAFE: Alte Datei wurde NICHT überschrieben.")
        sys.exit(1)

if __name__ == "__main__":
    main()



