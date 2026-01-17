import os
import json
import requests
import sys
from datetime import datetime, timedelta

# --- 1. KONFIGURATION ---
# Holt den Key sicher aus den Umgebungsvariablen
API_KEY = os.environ.get('FRED_API_KEY')

BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# Wir laden 400 Tage Historie (Puffer für 200-Tage-Durchschnitt) 3653
START_DATE = (datetime.now() - timedelta(days=400)).strftime('%Y-%m-%d')

# Mapping: Dein App-Name -> FRED Series ID
SERIES_MAP = {
    "WALCL": "WALCL",           # Fed Assets (Liquidität)
    "WTREGEN": "WTREGEN",       # Treasury Account (Liquidität)
    "RRP": "RRPONTSYD",         # Reverse Repo (Liquidität)
    "DGS10": "DGS10",           # 10Y Treasury (Volatilität & Kredit)
    "DBAA": "DBAA"              # Corp Bonds (Kredit)
}

def fetch_series(series_id):
    """
    Lädt eine Serie herunter und extrahiert nur Date/Value.
    """
    if not API_KEY:
        print("CRITICAL: API Key fehlt!")
        return None

    params = {
        "series_id": series_id,
        "api_key": API_KEY,
        "file_type": "json",
        "observation_start": START_DATE
    }
    
    # User-Agent setzen (Good Practice)
    headers = {
        "User-Agent": "SentinelApp/1.0 (GitHub Actions Mirror)"
    }
    
    try:
        response = requests.get(BASE_URL, params=params, headers=headers, timeout=300)
        
        # 1. HTTP Status Check
        if response.status_code != 200:
            print(f"ERROR {series_id}: HTTP {response.status_code}")
            return None
            
        data = response.json()
        
        # 2. JSON Struktur Check (Passt zu deinem JSON-Auszug)
        if "observations" not in data:
            print(f"ERROR {series_id}: Schlüssel 'observations' fehlt im JSON.")
            return None

        clean_observations = []
        for obs in data["observations"]:
            # FRED nutzt "." für fehlende Werte -> filtern
            if obs.get("value") != ".": 
                try:
                    clean_observations.append({
                        "d": obs["date"],           # Datum (String "YYYY-MM-DD")
                        "v": float(obs["value"])    # Wert (Als Zahl)
                    })
                except ValueError:
                    continue # Überspringe defekte Zahlen
        
        # 3. Leere Daten Check
        if not clean_observations:
            print(f"WARNING {series_id}: Keine gültigen Datenpunkte gefunden.")
            return None
            
        return clean_observations

    except Exception as e:
        print(f"EXCEPTION {series_id}: {str(e)}")
        return None

def main():
    print(f"--- Starte Sentinel Update: {datetime.now()} ---")
    
    if not API_KEY:
        print("ABBRUCH: FRED_API_KEY nicht gefunden.")
        sys.exit(1)

    temp_storage = {}
    success = True

    # SCHRITT 1: Alle Serien laden
    for name, series_id in SERIES_MAP.items():
        print(f"Lade {name}...")
        result = fetch_series(series_id)
        
        if result is None:
            print(f"ABBRUCH: Fehler bei {name}. Stoppe Update.")
            success = False
            break
        
        temp_storage[name] = result

    # SCHRITT 2: Speichern (Nur wenn alles erfolgreich war)
    if success:
        output = {
            "meta": {
                "updated": datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'),
                "source": "Federal Reserve Bank of St. Louis",
                "history_days_setting": str(START_DATE)
            },
            "data": temp_storage
        }

        try:
            with open('sentinel_data.json', 'w') as f:
                json.dump(output, f)
            print("SUCCESS: sentinel_data.json erfolgreich erstellt.")
            sys.exit(0)
        except Exception as e:
            print(f"DISK ERROR: Konnte Datei nicht schreiben: {e}")
            sys.exit(1)
    else:
        print("FAIL-SAFE: Update abgebrochen. Alte Datei bleibt erhalten.")
        sys.exit(1)

if __name__ == "__main__":
    main()







