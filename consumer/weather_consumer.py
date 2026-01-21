import json
import psycopg2
from kafka import KafkaConsumer

# CONFIG
KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
KAFKA_TOPIC = "weather_hourly"

DB_CONFIG = {
	"host": "localhost",
	"port": 5432,
	"dbname": "weather_pipeline",
	"user": "airflow",
	"password": "airflow"
}


# DATABASE CONNECTION
conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = True
cursor = conn.cursor()


# KAFKA CONSUMER
consumer = KafkaConsumer(
	KAFKA_TOPIC,
	bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
	group_id="weather_consumer_group",
	value_deserializer=lambda v: json.loads(v.decode("utf-8")),
	auto_offset_reset='earliest',
	enable_auto_commit=True
)

print("Kafka consumer started")

# CONSUMER LOOP
for message in consumer:
    print("Raw message from kafka:", message.value)
    data = message.value
    latitude=data['location']['latitude']
    longitude=data['location']['longitude']
    country=data['location']['country']
    city=data['location']['city']

    # LOOKUP / INSERT LOCATION
    cursor.execute(
		"""
			SELECT location_id from dim_location where latitude=%s AND longitude=%s
		""", (latitude, longitude)
	)

    result = cursor.fetchone()

    if result:
        location_id=result[0]
    else:
        cursor.execute(

			"""
				INSERT INTO dim_location (latitude, longitude, city, country) VALUES (%s, %s, %s, %s) RETURNING location_id
			""", (latitude, longitude, city, country)

		)
        location_id = cursor.fetchone()[0]


    # INSERT FACT TABLE
    cursor.execute(
		"""
			INSERT INTO fact_weather_hourly (location_id, timestamp, temperature_c, humidity, precipitation, wind_speed, data_source) values (%s, %s, %s, %s, %s, %s, %s) on conflict (location_id, timestamp) do nothing
		""", (location_id, data['weather']['timestamp'], data['weather']['temperature_c'], data['weather']['humidity'], data['weather']['precipitation'], data['weather']['wind_speed'], data['metadata']['data_source'] )
	)

    print(f"Inserted weather data for location_id={location_id}")
