import requests
import json
from datetime import datetime
from kafka import KafkaProducer

# CONFIG
API_KEY = "89f0def4a1229febdfa8b5ecf7c83e0b"
DATA_SOURCE = "openweather"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "weather_hourly"

# PARSE DATA FROM URL
cities = ['Jakarta', 'Singapore', 'Tokyo', 'Paris', 'Berlin']
api_data = {}

for city in cities:
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    api_data[city] = data

# KAFKA CONFIG
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
# EXTRACT DATA AND SEND TO KAFKA
for key, value in api_data.items():
    weather_data={
        'location': {
            'latitude': api_data[key]['coord']['lat'],
            'longitude': api_data[key]['coord']['lon'],
            'country': api_data[key]['sys']['country'],
            'city': key,
        },
        'weather': {
            'timestamp': datetime.utcfromtimestamp(api_data[key]['dt']).isoformat(),
            'temperature_c': api_data[key]['main']['temp'],
            'humidity': api_data[key]['main']['humidity'],
            'wind_speed': api_data[key]['wind']['speed'],
            'precipitation': api_data[key].get("rain", {}).get("1h", 0),
        },
        'metadata': {
            'data_source': DATA_SOURCE
        }
    }

    producer.send(KAFKA_TOPIC, weather_data)
    producer.flush()
    print("Data sent to kafka")
    print(json.dumps(weather_data, indent=2))



