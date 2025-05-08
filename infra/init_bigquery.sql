-- Dataset: airline_analytics

-- Tabla final para análisis de pasajeros (Gold Layer)
CREATE OR REPLACE TABLE `your-gcp-project.airline_analytics.passenger_summary`
(
  passenger_id STRING,
  name STRING,
  flight_id STRING,
  origin STRING,
  destination STRING,
  flight_date DATE,
  ticket_price FLOAT64,
  weather_conditions STRING,
  created_at TIMESTAMP
);

-- Otra tabla de ejemplo si lo deseas (agrega más según necesidades)
CREATE OR REPLACE TABLE `your-gcp-project.airline_analytics.flight_stats`
(
  flight_id STRING,
  origin STRING,
  destination STRING,
  average_delay_minutes FLOAT64,
  num_passengers INT64,
  processed_at TIMESTAMP
);
