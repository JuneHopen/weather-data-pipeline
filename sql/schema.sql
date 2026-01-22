CREATE TABLE dim_location (
    location_id SERIAL PRIMARY KEY,
    city TEXT,
    country TEXT,
    region TEXT,
    latitude NUMERIC(6,3) NOT NULL,
    longitude NUMERIC(6,3) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (latitude, longitude)
);


CREATE TABLE fact_weather_hourly (
    id SERIAL PRIMARY KEY,
    location_id INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    temperature_c NUMERIC(5,2),
    humidity NUMERIC(5,2),
    precipitation NUMERIC(5,2),
    wind_speed NUMERIC(5,2),
    data_source TEXT NOT NULL,
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_location
        FOREIGN KEY (location_id)
        REFERENCES dim_location(location_id),

    CONSTRAINT uq_weather_hourly
        UNIQUE (location_id, timestamp)
);
