from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app=FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/weather")
def get_weather():
    return {
        "status": "success",
        "data": [
            {"city": "Baku", "temp": 32.5, "lat": 40.4093, "lon": 49.8671, "risk": "Medium"},
            {"city": "Ganja", "temp": 38.0, "lat": 40.6828, "lon": 46.3606, "risk": "High"},
            {"city": "Lankaran", "temp": 28.5, "lat": 38.7529, "lon": 48.8475, "risk": "Low"}
        ]
    }

