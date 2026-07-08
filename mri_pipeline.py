import os
import requests
import pandas as pd
import duckdb
import numpy as np
from sklearn.decomposition import IncrementalPCA
from sklearn.preprocessing import StandardScaler
from datetime import datetime, timedelta

# Market Reality Index (MRI): An indicator scanning beneath the surface of the stock market to see the "bones" of the economy.
# Konfiguration
FRED_API_KEY = os.environ.get("FRED_API_KEY")
FRED_SERIES = {
    'T10Y2Y': 'd',      # Daily: 10-Year Minus 2-Year Treasury
    'RRPONTSYD': 'd',   # Daily: Overnight Reverse Repurchase Agreements
    'WALCL': 'w',       # Weekly: Federal Reserve Total Assets
    'ICSA': 'w'         # Weekly: Initial Claims
}
LOOKBACK_DAYS = 365
ROLLING_WINDOW = 252 # Handelstage für Z-Score und PCA

def fetch_fred_data(series_id, frequency):
    """Holt historische Zeitreihendaten von der FRED API."""
    if not FRED_API_KEY:
        raise ValueError("FRED_API_KEY Umgebungsvariable nicht gesetzt.")
        
    start_date = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime('%Y-%m-%d')
    url = f"https://api.stlouisfed.org/fred/series/observations"
    params = {
        'series_id': series_id,
        'api_key': FRED_API_KEY,
        'file_type': 'json',
        'observation_start': start_date,
        'frequency': frequency
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()['observations']
    
    df = pd.DataFrame(data)[['date', 'value']]
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df['series'] = series_id
    return df.dropna()

def fetch_gdelt_tone_mock():
    """
    Platzhalter für die GDELT GKG Ingestion.
    In Produktion: Download der täglichen CSV, Filterung nach 'ECON_' 
    und Berechnung des volumengewichteten durchschnittlichen Tons.
    """
    dates = pd.date_range(end=datetime.now(), periods=LOOKBACK_DAYS)
    df = pd.DataFrame({
        'date': dates.strftime('%Y-%m-%d'),
        'value': np.random.uniform(-5, 5, size=LOOKBACK_DAYS), # Simulierter Sentiment-Score
        'series': 'GDELT_ECON_TONE'
    })
    return df

def process_data_with_duckdb(fred_dfs, gdelt_df):
    """Nutzt DuckDB für schnelles Resampling und Forward-Filling der Zeitreihen."""
    # Alle Daten in einen DataFrame packen
    all_data = pd.concat(fred_dfs + [gdelt_df], ignore_index=True)
    all_data['date'] = pd.to_datetime(all_data['date'])
    
    # In-Memory DuckDB Verbindung
    con = duckdb.connect(':memory:')
    con.register('raw_data', all_data)
    
    # SQL-Query zum Pivotieren und Forward-Füllen für Handelstage
    query = """
    WITH dates AS (
        SELECT DISTINCT date FROM raw_data WHERE date >= current_date - INTERVAL 1 YEAR
    ),
    pivoted AS (
        SELECT 
            d.date,
            MAX(CASE WHEN series = 'T10Y2Y' THEN value END) as T10Y2Y,
            MAX(CASE WHEN series = 'RRPONTSYD' THEN value END) as RRPONTSYD,
            MAX(CASE WHEN series = 'WALCL' THEN value END) as WALCL,
            MAX(CASE WHEN series = 'ICSA' THEN value END) as ICSA,
            MAX(CASE WHEN series = 'GDELT_ECON_TONE' THEN value END) as GDELT_TONE
        FROM dates d
        LEFT JOIN raw_data r ON d.date = r.date
        GROUP BY d.date
    )
    SELECT 
        date,
        LAST_VALUE(T10Y2Y IGNORE NULLS) OVER (ORDER BY date) as T10Y2Y,
        LAST_VALUE(RRPONTSYD IGNORE NULLS) OVER (ORDER BY date) as RRPONTSYD,
        LAST_VALUE(WALCL IGNORE NULLS) OVER (ORDER BY date) as WALCL,
        LAST_VALUE(ICSA IGNORE NULLS) OVER (ORDER BY date) as ICSA,
        LAST_VALUE(GDELT_TONE IGNORE NULLS) OVER (ORDER BY date) as GDELT_TONE
    FROM pivoted
    ORDER BY date;
    """
    
    clean_df = con.execute(query).fetchdf()
    return clean_df.dropna()

def calculate_mri(df):
    """Berechnet den Index mithilfe der IncrementalPCA."""
    features = ['T10Y2Y', 'RRPONTSYD', 'WALCL', 'ICSA', 'GDELT_TONE']
    
    # Z-Score Standardisierung über das rollierende Fenster
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(df[features])
    
    # Inkrementelle PCA initialisieren (Batch-Size anpassen je nach Speicherbedarf)
    ipca = IncrementalPCA(n_components=2, batch_size=30)
    principal_components = ipca.fit_transform(scaled_data)
    
    # PC1 und PC2 extrahieren
    df['PC1_MacroTrend'] = principal_components[:, 0]
    df['PC2_Divergence'] = principal_components[:, 1]
    
    # Gewichtete Kombination zur Erstellung des rohen Index
    # (Gewichte basieren idealerweise auf der erklärten Varianz ipca.explained_variance_ratio_)
    w1, w2 = ipca.explained_variance_ratio_
    df['Raw_Index'] = (df['PC1_MacroTrend'] * w1) + (df['PC2_Divergence'] * w2)
    
    # Min-Max-Skalierung auf einen anwenderfreundlichen Bereich (0 - 100)
    min_val = df['Raw_Index'].min()
    max_val = df['Raw_Index'].max()
    df['MRI_Score'] = ((df['Raw_Index'] - min_val) / (max_val - min_val)) * 100
    
    return df

def main():
    print("Starte MRI Pipeline...")
    
    # 1. Datenabruf
    fred_data = []
    for series, freq in FRED_SERIES.items():
        print(f"Lade FRED Serie: {series}...")
        fred_data.append(fetch_fred_data(series, freq))
        
    print("Lade GDELT Sentiment...")
    gdelt_data = fetch_gdelt_tone_mock()
    
    # 2. Datenaufbereitung mit DuckDB
    print("Richte Zeitreihen mit DuckDB aus...")
    clean_df = process_data_with_duckdb(fred_data, gdelt_data)
    
    # 3. Algorithmische Berechnung (PCA)
    print("Berechne Hauptkomponenten und MRI-Score...")
    final_df = calculate_mri(clean_df)
    
    # 4. Ausgabe für API / Datenbank
    latest_score = final_df.iloc[-1]
    print("\n--- Aktueller Marktindikator ---")
    print(f"Datum: {latest_score['date'].strftime('%Y-%m-%d')}")
    print(f"MRI Score (0-100): {latest_score['MRI_Score']:.2f}")
    print(f"PC1 (Makro-Trend): {latest_score['PC1_MacroTrend']:.2f}")
    print(f"PC2 (Divergenz-Warnung): {latest_score['PC2_Divergence']:.2f}")
    
    # Speichern für die App-Backend-Datenbank
    final_df[['date', 'MRI_Score', 'PC1_MacroTrend', 'PC2_Divergence']].to_json("mri_output.json", orient="records")
    print("Daten erfolgreich in 'mri_output.json' gespeichert.")

if __name__ == "__main__":
    main()
