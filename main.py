from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import requests
import datetime
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError

# Database config
DB_USER = "Your_databse_name
DB_PASS = "Database_password"
DB_NAME = "Database_Name"
DB_HOST = "DB_host or IP address"
DB_PORT = "5432"

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# FastAPI app
app = FastAPI(title="Weather Data API", version="1.0.0")

# Database models
class Venue(Base):
    __tablename__ = "venues"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)

class WeatherData(Base):
    __tablename__ = "weather_data"
    id = Column(Integer, primary_key=True, index=True)
    venue_id = Column(Integer, ForeignKey("venues.id"))
    date = Column(DateTime)
    temperature = Column(Float)
    relative_humidity = Column(Float)
    dewpoint = Column(Float)
    apparent_temperature = Column(Float)
    precipitation_probability = Column(Float)
    precipitation = Column(Float)
    rain = Column(Float)
    showers = Column(Float)
    snowfall = Column(Float)
    snow_depth = Column(Float)

# Request model
class WeatherRequest(BaseModel):
    venue_id: int
    start_date: str  # format: YYYY-MM-DD
    end_date: str    # format: YYYY-MM-DD

# API endpoint
@app.post("/load_weather_data", summary="Load historical weather data", response_description="Weather data saved")
def load_weather_data(req: WeatherRequest):
    session = SessionLocal()

    try:
        # Validate venue
        venue = session.query(Venue).filter(Venue.id == req.venue_id).first()
        if not venue:
            raise HTTPException(status_code=404, detail="Venue not found")

        # Prepare API request
        base_url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": venue.latitude,
            "longitude": venue.longitude,
            "start_date": req.start_date,
            "end_date": req.end_date,
            "hourly": ",".join([
                "temperature_2m",
                "relative_humidity_2m",
                "dew_point_2m",
                "apparent_temperature",
                "precipitation_probability",
                "precipitation",
                "rain",
                "showers",
                "snowfall",
                "snow_depth"
            ]),
            "timezone": "auto"
        }

        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        # Insert into DB
        for i, timestamp in enumerate(data["hourly"]["time"]):
            weather = WeatherData(
                venue_id=req.venue_id,
                date=datetime.datetime.fromisoformat(timestamp),
                temperature=data["hourly"].get("temperature_2m", [None])[i],
                relative_humidity=data["hourly"].get("relative_humidity_2m", [None])[i],
                dewpoint=data["hourly"].get("dew_point_2m", [None])[i],
                apparent_temperature=data["hourly"].get("apparent_temperature", [None])[i],
                precipitation_probability=data["hourly"].get("precipitation_probability", [None])[i],
                precipitation=data["hourly"].get("precipitation", [None])[i],
                rain=data["hourly"].get("rain", [None])[i],
                showers=data["hourly"].get("showers", [None])[i],
                snowfall=data["hourly"].get("snowfall", [None])[i],
                snow_depth=data["hourly"].get("snow_depth", [None])[i],
            )
            session.add(weather)

        session.commit()
        return {"message": "Weather data saved successfully!"}

    except Exception as e:
        session.rollback()
        print(f"🔥 ERROR: {e}")  # print full error to terminal
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")

    finally:
        session.close()


    return {"message": "Weather data saved successfully!"}
