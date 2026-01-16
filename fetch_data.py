import os
import json
import requests
from datetime import datetime, timedelta

# KONFIGURATION
# Der Key kommt sicher aus den GitHub Secrets
API_KEY = os.environ.get('FRED_API_KEY')
BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
# Wir holen 365 Tage Historie für die Durchschnitte
START_DATE = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

# DAS SIND DIE "KOSTENLOSEN" DATENQUELLEN (FRED ONLY)
SERIES_MAP = {
    # System Liquidity (MBP)
    "WALCL": "WALCL",           # Fed Assets
    "WTREGEN": "WTREGEN",       # Treasury Account
    "RRP": "RRPONTSYD",         # Reverse Repo
    
    # Volatilität & Kredit (ATM & CFI)
    "DGS10": "DGS10",           # 10Y Treasury Yield
    "DBAA": "DBAA"              # Corp Bonds (Baa)
}

def fetch_series(series_id):
    if not API_KEY:
        return []
        
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
        
        observations = []
        for obs in data.get("observations", []):
            if obs["value"] != ".": 
                observations.append({
                    "d": obs["date"],
                    "v": float(obs["value"])
                })
        return observations
    except Exception as e:
        print(f"Error fetching {series_id}: {e}")
        return []

def main():
    if not API_KEY:
        print("ACHTUNG: Kein API Key gefunden. Secret prüfen!")
        return

    final_data = {
        "meta": {
            "updated": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "source": "Federal Reserve Bank of St. Louis"
        },
        "data": {}
    }

    print("Starte Download von FRED...")
    for name, series_id in SERIES_MAP.items():
        print(f"Hole {name}...")
        final_data["data"][name] = fetch_series(series_id)

    # Speichern als JSON
    with open('sentinel_data.json', 'w') as f:
        json.dump(final_data, f)
    
    print("Fertig! sentinel_data.json wurde erstellt.")

if __name__ == "__main__":
    main()
