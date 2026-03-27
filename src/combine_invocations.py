import sqlite3
import pandas as pd
import os

DB_PATH = os.path.join('..', 'data', 'processed', 'telemetry.db')
OUT_PATH = os.path.join('..', 'data', 'processed', 'invocations_combined.parquet')

def combine_invocations():    
    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}")
        return
                
    print(f"Connecting to database at {DB_PATH}")
    conn = sqlite3.connect(str(DB_PATH))
    
    sparse_chunks = []
    
    for day in range(1, 15):
        day_str = f"{day:02d}"
        table_name = f"invocations_per_function_md.anon.d{day_str}"
        print(f"Processing day {day_str}...")
        
        try:
            df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
        except Exception as e:
            print(f"Could not read {table_name}. Error: {e}")
            continue
            
        df = df.drop(columns=[c for c in ['hashowner', 'hashapp', 'trigger'] if c in df.columns])
        
        minute_columns = [col for col in df.columns if col.isdigit()]
        
        melted_df = df.melt(
            id_vars=['hashfunction'], 
            value_vars=minute_columns, 
            var_name='minute_of_day', 
            value_name='invocations'
        )
        
        # Filter out zero invocations to save massive amounts of memory memory
        melted_df = melted_df[melted_df['invocations'] > 0].copy()
        
        # Convert to global_minutes
        melted_df['global_minute'] = melted_df['minute_of_day'].astype(int) + (day * 1440)
        melted_df = melted_df.drop(columns=['minute_of_day'])
        
        sparse_chunks.append(melted_df)
    
    print("Combining all days and saving to Parquet...")
    if sparse_chunks:
        final_df = pd.concat(sparse_chunks, ignore_index=True)
        final_df.to_parquet(out_path, index=False)
        print(f"Done! Saved sparse dataset with {len(final_df)} rows to {out_path}")
    else:
        print("No data found.")
        
    conn.close()

if __name__ == "__main__":
    combine_invocations()
