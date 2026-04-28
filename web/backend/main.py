
import sys
import os
import duckdb
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Layihənin ana qovluğunu tapırıq
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ❗ DÜZƏLİŞ: Database.py-dakı adla eyni olmalıdır: "weather.duckdb"
DB_PATH = os.path.join(BASE_DIR, "data", "weather.duckdb")

@app.get("/api/weather")
def get_weather():
    if not os.path.exists(DB_PATH):
        return {"status": "error", "message": f"Baza faylı tapılmadı: {DB_PATH}"}

    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        
        # Artıq raw.raw_forecast işləyəcək, çünki database.py-da sxemi belə yaratdıq
        query = """
            SELECT 
                city, 
                temperature_2m_max as temp, 
                latitude as lat, 
                longitude as lon
            FROM raw.raw_forecast
            QUALIFY ROW_NUMBER() OVER(PARTITION BY city ORDER BY time DESC) = 1
        """
        
        df = conn.execute(query).df()
        conn.close()

        # Koordinatları string yox, float kimi göndərmək xəritə üçün daha yaxşıdır
        return {
            "status": "success", 
            "count": len(df),
            "data": df.to_dict(orient="records")
        }
    except Exception as e:
        return {"status": "error", "message": f"Xəta baş verdi: {str(e)}"}
