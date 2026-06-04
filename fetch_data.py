import os
import json
import requests
import sys
from datetime import datetime, timedelta

# Extraktion des kritischen API-Schlüssels aus dem Environment.
# Das Fehlen des Keys muss zwingend zum Abbruch führen.
API_KEY = os.environ.get('FRED_API_KEY')
BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# Definition des gleitenden Beobachtungsfensters: Exakt 10 Jahre in die Vergangenheit
START_DATE = (datetime.now() - timedelta(days=3653)).strftime('%Y-%m-%d')

# Mapping der systemkritischen Wirtschaftsindikatoren auf FRED-IDs
SERIES_MAP = {
    "WALCL": "WALCL",           # Total Assets of the Federal Reserve
    "WTREGEN": "WTREGEN",       # Treasury General Account
    "RRP": "RRPONTSYD",         # Overnight Reverse Repurchase Agreements
    "DGS10": "DGS10",           # 10-Year Treasury Constant Maturity Rate
    "DBAA": "DBAA"              # Moody's Seasoned Baa Corporate Bond Yield
}

def fetch_series(series_id):
    """
    Lädt eine Zeitreihe über das FRED API herunter und extrahiert die Date/Value-Paare.
    Implementiert strikte Validierung, Typensicherheit und CI/CD-freundliches Logging.
    """
    if not API_KEY:
        print("CRITICAL: API Key 'FRED_API_KEY' fehlt in der Laufzeitumgebung des Runners!")
        return None

    params = {
        "series_id": series_id,
        "api_key": API_KEY,
        "file_type": "json",
        "observation_start": START_DATE
    }
    
    # Custom User-Agent zur Vermeidung von Blockaden durch WAFs (Web Application Firewalls)
    headers = { "User-Agent": "SentinelApp/2.0 (GitHub Actions Automated Pipeline)" }
    
    try:
        # Ein Timeout von 30 Sekunden ist für die FRED API architektonisch sinnvoller als 300s,
        # um hängende Runner-Jobs (Zombie-Prozesse) zu verhindern.
        response = requests.get(BASE_URL, params=params, headers=headers, timeout=30)
        
        # Proaktives Abfangen von HTTP-Fehlercodes (z. B. 429 Rate Limits oder 400 Bad Request)
        if response.status_code!= 200:
            print(f"ERROR {series_id}: HTTP Status {response.status_code}")
            print(f"Server-Antwort (Diagnostic): {response.text}")
            return None
            
        data = response.json()
        
        # Validierung der Strukturintegrität der JSON-Antwort
        if "observations" not in data:
            print(f"ERROR {series_id}: Der kritische Schlüssel 'observations' fehlt in der Payload.")
            return None

        clean_observations =
        for obs in data["observations"]:
            # Filtern von ungültigen oder fehlenden Werten (repräsentiert durch ".")
            if obs.get("value")!= ".": 
                try:
                    clean_observations.append({
                        "d": obs["date"],        
                        "v": float(obs["value"])
                    })
                except ValueError:
                    # Stilles Ignorieren von Werten, die sich nicht in Float konvertieren lassen
                    continue 
        
        if not clean_observations:
            print(f"WARNING {series_id}: Der Zeitraum enthält keine gültigen, numerischen Datenpunkte.")
            return None
            
        return clean_observations

    # Differenzierte Fehlerbehandlung für präziseres Logging im GitHub Actions Dashboard
    except requests.exceptions.RequestException as e:
        print(f"NETWORK EXCEPTION {series_id}: Verbindung zur FRED API fehlgeschlagen: {str(e)}")
        return None
    except Exception as e:
        print(f"GENERAL EXCEPTION {series_id}: Unerwarteter Laufzeitfehler: {str(e)}")
        return None

def main():
    # ISO-Format für präzises chronologisches Logging
    print(f"--- Starte Sentinel Update Pipeline: {datetime.now().isoformat()} ---")
    
    if not API_KEY:
        print("ABBRUCH: Das FRED_API_KEY Secret wurde nicht in den Runner injiziert.")
        sys.exit(1)

    temp_storage = {}
    success = True

    # Iteration über das definierte Indikatoren-Mapping
    for name, series_id in SERIES_MAP.items():
        print(f"Lade Datensatz: {name} (ID: {series_id})...")
        result = fetch_series(series_id)
        
        if result is None:
            print(f"ABBRUCH: Kritischer Fehler bei {name}. Der Update-Prozess wird präventiv gestoppt, um Datenkorruption zu vermeiden.")
            success = False
            break # Beendet die Schleife sofort
        
        temp_storage[name] = result
        
    if success:
        output = {
            "meta": {
                "updated": datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'),
                "source": "Federal Reserve Bank of St. Louis"
            },
            "data": temp_storage
        }

        try:
            # Das Skript überschreibt die existierende Datei im Workspace des Runners.
            # Die Verwendung von separators=(',', ':') entfernt sämtliche unnötigen Leerzeichen
            # und optimiert die Datei drastisch für die Speicherung in Git.
            with open('sentinel_data.json', 'w') as f:
                json.dump(output, f, separators=(',', ':'))
            print("SUCCESS: Datensatz 'sentinel_data.json' wurde im Runner-Dateisystem erfolgreich aggregiert.")
            sys.exit(0) # Positiver Exit-Code für die Pipeline
        except IOError as e:
            print(f"DISK ERROR: Datei konnte nicht auf das Host-System geschrieben werden: {e}")
            sys.exit(1)
    else:
        print("FAIL-SAFE: Prozess frühzeitig abgebrochen. Die alte Datendatei bleibt im Repository unberührt.")
        sys.exit(1)

if __name__ == "__main__":
    main()
