from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel # Məlumatın formatını yoxlamaq üçün

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


fake_db = [
    {"city": "Bakı", "temp": 32, "lat": 40.4093, "lon": 49.8671, "risk": "High"},
    {"city": "Gəncə", "temp": 35, "lat": 40.6828, "lon": 46.3606, "risk": "High"},
    {"city": "Lənkəran", "temp": 28, "lat": 38.7529, "lon": 48.8475, "risk": "Low"}
]

class City(BaseModel):
    city: str
    temp: float
    lat: float
    lon: float
    risk: str

@app.get("/api/weather")
def get_weather():
    return {"status": "success", "data": fake_db}

@app.post("/api/admin/add")
def add_city(city: City):
    fake_db.append(city.model_dump())
    return {"message": f"{city.city} uğurla əlavə edildi!", "current_data": fake_db}

@app.post("/api/admin/add")
def add_city(city: City):
    fake_db.append(city.dict()) 
    return {"status": "success"}