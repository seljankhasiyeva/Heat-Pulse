import os
import duckdb
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 1. Yollar
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "weather_data.duckdb")
RAW_FOLDER = os.path.join(BASE_DIR, "data", "raw")

def sync_all_cities():
    if not os.path.exists(RAW_FOLDER):
        print(f"--- XƏTA: {RAW_FOLDER} qovluğu tapılmadı!")
        return

    con = duckdb.connect(DB_PATH)
    
    # Cədvəli yaradarkən ehtiyatlı oluruq
    con.execute("""
        CREATE TABLE IF NOT EXISTS cities (
            city VARCHAR,
            lat DOUBLE,
            lon DOUBLE,
            temp DOUBLE,
            risk VARCHAR
        )
    """)
    
    try:
        # 2. Bütün CSV-ləri bir dəfəyə oxuyub bazaya doldurmaq
        # RAW_FOLDER/*.csv bütün faylları tapır və DuckDB onları avtomatik alt-alta birləşdirir
        all_csv_path = os.path.join(RAW_FOLDER, "*.csv")
        
        con.execute("DELETE FROM cities") # Köhnə datanı silirik
        
        # 'union_by_name=True' əgər bəzi fayllarda sütun sırası fərqlidirsə kömək edir
        con.execute(f"""
            INSERT INTO cities 
            SELECT * FROM read_csv_auto('{all_csv_path}', union_by_name=True)
        """)
        
        row_count = con.execute("SELECT count(*) FROM cities").fetchone()[0]
        file_count = len([f for f in os.listdir(RAW_FOLDER) if f.endswith('.csv')])
        
        print(f"--- UĞURLU: {file_count} ədəd fayldan cəmi {row_count} sətir yükləndi.")
        
    except Exception as e:
        print(f"--- CSV oxunma xətası: {e}")
        print("--- İpucu: Bütün CSV fayllarının sütun adları eyni olmalıdır.")
    
    con.close()

sync_all_cities()

# --- Endpointlər ---

@app.get("/api/weather")
def get_weather():
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        df = con.execute("SELECT * FROM cities").df()
        con.close()
        return {"status": "success", "data": df.to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

class City(BaseModel):
    city: str
    lat: float
    lon: float
    temp: float
    risk: str

@app.post("/api/admin/add")
def add_city(city: City):
    try:
        con = duckdb.connect(DB_PATH)
        con.execute("INSERT INTO cities VALUES (?, ?, ?, ?, ?)", 
                    (city.city, city.lat, city.lon, city.temp, city.risk))
        con.close()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}