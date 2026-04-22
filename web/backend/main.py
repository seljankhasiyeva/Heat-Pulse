from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware



app = FastAPI()

# Frontend (Port 3000 və ya birbaşa fayl) Backend-ə (Port 8000) çata bilsin deyə:
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],

    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/weather")
def get_weather():


    # Bax, app.js-dəki result.data-nı buradakı "data" siyahısı təmin edir
    return {
        "status": "success",
        "data": [
            {
                "city": "Bakı", 
                "temp": 32, 
                "lat": 40.4093, 
                "lon": 49.8671, 
                "risk": "High" # Bu "High" olduğu üçün app.js onu qırmızı (red-500) edəcək
            },
            {
                "city": "Gəncə", 
                "temp": 35, 
                "lat": 40.6828, 
                "lon": 46.3606, 
                "risk": "High"
            },
            {
                "city": "Lənkəran", 
                "temp": 28, 
                "lat": 38.7529, 
                "lon": 48.8475, 
                "risk": "Low" # Bu isə yaşıl (green-500) olacaq
            }
        ]
    }

