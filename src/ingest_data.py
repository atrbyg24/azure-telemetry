import sqlite3
import pandas as pd
import glob
import os

DB_PATH = os.path.join('..', 'data', 'processed', 'telemetry.db')
RAW_DATA_PATH = os.path.join('..', 'data', 'raw', 'azurefunctions-dataset2019', '*.csv')

def ingest_telemetry():
    """
    Ingests the raw CSV telemetry data into a SQLite database.
    """

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    
    files = glob.glob(RAW_DATA_PATH)
    
    if not files:
        print(f"No CSVs found in {RAW_DATA_PATH}!")
        return

    for file in files:
        table_name = os.path.basename(file).replace('.csv', '').lower()
        print(f"Ingesting {file} into table: {table_name}")
        
        # Using chunks for telemetry data to save RAM
        for chunk in pd.read_csv(file, chunksize=100000):
            chunk.columns = [c.lower().replace(' ', '_') for c in chunk.columns]
            chunk.to_sql(table_name, conn, if_exists='append', index=False)
            
    conn.close()
    print("Ingestion complete.")

if __name__ == "__main__":
    ingest_telemetry()