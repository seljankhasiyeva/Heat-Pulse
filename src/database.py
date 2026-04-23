import duckdb
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "weather.duckdb")
DATA_RAW_PATH = os.path.join(BASE_DIR, "data", "raw", "*_historical.csv")

def get_connection(db_path=DB_PATH):
    """Connects to the DuckDB database."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    return duckdb.connect(db_path)

def create_schemas(conn):
    """Creates the necessary schemas."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    conn.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
    print("Schemas created: raw, staging, analytics")

def create_raw_tables(conn):
    """Initializes the raw table structure from CSV files."""
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS raw.weather_daily AS 
        SELECT * FROM read_csv_auto('{DATA_RAW_PATH}') LIMIT 0;
    """)
    print("Raw table structure initialized.")

def load_raw_data(conn):
    """Loads all historical CSV files into the raw table."""
    conn.execute("TRUNCATE TABLE raw.weather_daily;")
    conn.execute(f"""
        INSERT INTO raw.weather_daily 
        SELECT * FROM read_csv_auto('{DATA_RAW_PATH}');
    """)
    count = conn.execute("SELECT count(*) FROM raw.weather_daily").fetchone()[0]
    print(f"Data loaded successfully. Total rows: {count}")