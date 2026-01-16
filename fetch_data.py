import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta

# KONFIGURATION
API_KEY = os.environ.get('FRED_API_KEY')
BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
# Wir holen genug Historie für 200-Tage-Durchschnitte etc.
START_DATE = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

# DEINE SERIEN (Die neuen "kostenlosen" Quellen)
SERIES_MAP = {
    # Für MBP (Liquidität)
    "WALCL": "WALCL",           # Fed Assets
    "WTREGEN": "WTREGEN",       # TGA (Treasury Account)
    "RRP": "RRPONTSYD",         # Reverse Repo
    
    # Für ATM (Volatilität) & CFI (Credit)
    "DGS10": "DGS10",           # 10Y Treasury Yield
    "DBAA": "DBAA"              # Corp Bonds (Baa)
}

def fetch_series(series_id):
    """Holt Daten von FRED und gibt sie als saubere Liste zurück."""
    params = {
        "series_id": series_id,
        "api_key": API_KEY,
        "file_type": "json",
        "observation_start": START_DATE
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Nur Datum und Wert extrahieren
        observations = []
        for obs in data.get("observations", []):
            if obs["value"] != ".": # "." bedeutet "kein Datenpunkt" bei FRED
                observations.append({
                    "d": obs["date"],
                    "v": float(obs["value"])
                })
        return observations
    except Exception as e:
        print(f"Fehler bei {series_id}: {e}")
        return []

def main():
    if not API_KEY:
        raise ValueError("Kein API Key gefunden! Setze FRED_API_KEY in den Secrets.")

    final_data = {
        "meta": {
            "updated": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "source": "Federal Reserve Bank of St. Louis (Public Domain)"
        },
        "data": {}
    }

    print("Starte Download...")
    for name, series_id in SERIES_MAP.items():
        print(f"Lade {name} ({series_id})...")
        final_data["data"][name] = fetch_series(series_id)

    # Speichern als JSON für die App
    with open('sentinel_data.json', 'w') as f:
        json.dump(final_data, f)
    
    print("Fertig! sentinel_data.json wurde erstellt.")

if __name__ == "__main__":
    main()