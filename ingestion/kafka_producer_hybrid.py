"""
ingestion/kafka_producer_hybrid.py
-----------------------------------
Hybrid producer:
  - Generates orders with Faker
  - Fetches real country/city data from RestCountries API
  - Enriches each order with live weather data from OpenWeatherMap API
  - Publishes enriched events to Kafka orders-raw topic

Usage:
  OPENWEATHER_API_KEY=xxx KAFKA_BOOTSTRAP=localhost:9092 python ingestion/kafka_producer_hybrid.py
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta

import requests
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC              = "orders-raw"
INTERVAL_SEC       = float(os.getenv("PRODUCE_INTERVAL", "1.0"))
OPENWEATHER_KEY    = os.getenv("OPENWEATHER_API_KEY", "")

STATUSES   = ["delivered", "shipped", "processing", "cancelled", "returned"]
CATEGORIES = ["electronics", "clothing", "home_appliances", "books", "sports",
              "beauty", "toys", "furniture", "food", "automotive"]

# Gercek sehirler - RestCountries + OpenWeather icin iyi sonuc veren sehirler
CITIES = [
    {"city": "Istanbul",   "country": "TR", "lat": 41.01, "lon": 28.95},
    {"city": "Ankara",     "country": "TR", "lat": 39.93, "lon": 32.86},
    {"city": "Izmir",      "country": "TR", "lat": 38.42, "lon": 27.14},
    {"city": "Berlin",     "country": "DE", "lat": 52.52, "lon": 13.40},
    {"city": "Amsterdam",  "country": "NL", "lat": 52.37, "lon": 4.90},
    {"city": "Paris",      "country": "FR", "lat": 48.85, "lon": 2.35},
    {"city": "London",     "country": "GB", "lat": 51.51, "lon": -0.13},
    {"city": "Warsaw",     "country": "PL", "lat": 52.23, "lon": 21.01},
    {"city": "Vienna",     "country": "AT", "lat": 48.21, "lon": 16.37},
    {"city": "Madrid",     "country": "ES", "lat": 40.42, "lon": -3.70},
    {"city": "Rome",       "country": "IT", "lat": 41.90, "lon": 12.49},
    {"city": "Prague",     "country": "CZ", "lat": 50.08, "lon": 14.44},
]

# RestCountries cache - her run'da bir kez cek
_country_cache = {}

def fetch_country_info(country_code: str) -> dict:
    if country_code in _country_cache:
        return _country_cache[country_code]
    try:
        resp = requests.get(
            f"https://restcountries.com/v3.1/alpha/{country_code}",
            timeout=5
        )
        data = resp.json()[0]
        info = {
            "country_name":       data.get("name", {}).get("common", country_code),
            "currency":           list(data.get("currencies", {}).keys())[0] if data.get("currencies") else "USD",
            "population":         data.get("population", 0),
            "region":             data.get("region", ""),
        }
        _country_cache[country_code] = info
        return info
    except Exception as e:
        print(f"[producer] RestCountries error for {country_code}: {e}")
        return {"country_name": country_code, "currency": "USD", "population": 0, "region": ""}


def fetch_weather(lat: float, lon: float, city: str) -> dict:
    try:
        resp = requests.get(
            "https://api.openweathermap.org/data/2.5/weather",
            params={
                "lat":   lat,
                "lon":   lon,
                "appid": OPENWEATHER_KEY,
                "units": "metric",
            },
            timeout=5,
        )
        data = resp.json()
        return {
            "weather_condition": data["weather"][0]["main"],
            "weather_desc":      data["weather"][0]["description"],
            "temperature_c":     round(data["main"]["temp"], 1),
            "feels_like_c":      round(data["main"]["feels_like"], 1),
            "humidity":          data["main"]["humidity"],
            "wind_speed":        data.get("wind", {}).get("speed", 0),
        }
    except Exception as e:
        print(f"[producer] OpenWeather error for {city}: {e}")
        return {
            "weather_condition": "Unknown",
            "weather_desc":      "unavailable",
            "temperature_c":     0,
            "feels_like_c":      0,
            "humidity":          0,
            "wind_speed":        0,
        }


def make_event(city_info: dict, country_info: dict, weather: dict) -> dict:
    price    = round(random.uniform(9.9, 999.9), 2)
    quantity = random.randint(1, 5)
    status   = random.choice(STATUSES)

    return {
        "event_type":   "order_created",
        "order_id":     str(uuid.uuid4()),
        "customer_id":  str(uuid.uuid4()),
        "order_status": status,
        "product": {
            "product_id":    str(uuid.uuid4()),
            "category_name": random.choice(CATEGORIES),
            "price":         price,
        },
        "quantity":      quantity,
        "freight_value": round(price * 0.1, 2),
        "customer": {
            "city":         city_info["city"],
            "country_code": city_info["country"],
            "country_name": country_info["country_name"],
            "currency":     country_info["currency"],
            "population":   country_info["population"],
            "region":       country_info["region"],
            "lat":          city_info["lat"],
            "lon":          city_info["lon"],
        },
        "weather":       weather,
        "purchased_at":  datetime.now().isoformat(),
        "ts":            datetime.now().isoformat(),
    }


def main():
    print(f"[producer] Connecting to Kafka at {KAFKA_BOOTSTRAP}...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )

    # Tum ulkelerin country info'sunu onceden cek
    print("[producer] Fetching country data from RestCountries API...")
    country_codes = list(set(c["country"] for c in CITIES))
    for code in country_codes:
        info = fetch_country_info(code)
        print(f"  {code}: {info['country_name']} ({info['currency']})")

    print(f"[producer] Streaming to '{TOPIC}' every {INTERVAL_SEC}s — Ctrl+C to stop\n")

    sent         = 0
    weather_cache = {}
    weather_ttl   = 300  # hava durumunu 5 dakikada bir yenile

    try:
        while True:
            city_info = random.choice(CITIES)
            city_key  = city_info["city"]

            # Hava durumu cache'i
            now = time.time()
            if city_key not in weather_cache or now - weather_cache[city_key]["fetched_at"] > weather_ttl:
                weather = fetch_weather(city_info["lat"], city_info["lon"], city_key)
                weather_cache[city_key] = {**weather, "fetched_at": now}
            else:
                weather = {k: v for k, v in weather_cache[city_key].items() if k != "fetched_at"}

            country_info = _country_cache.get(city_info["country"], {})
            event        = make_event(city_info, country_info, weather)

            producer.send(TOPIC, value=event, key=event["order_id"].encode())
            sent += 1
            print(
                f"[producer] #{sent:04d} | {city_key:<12} | "
                f"{weather['weather_condition']:<8} {weather['temperature_c']}°C | "
                f"status={event['order_status']}"
            )
            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        print(f"\n[producer] Stopped. Sent {sent} events.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
