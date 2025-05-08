# Airline Batch Data Pipeline (GCP)

Este proyecto implementa un pipeline de procesamiento batch de datos para una aerolínea utilizando tecnologías modernas de Google Cloud Platform, con un enfoque modular y profesional.

## Tecnologías Usadas

- **Google Cloud Storage (GCS)**: Almacenamiento de datos en esquema Medallion (Bronze, Silver, Gold)
- **Dataproc + Spark**: Procesamiento batch distribuido
- **BigQuery**: Almacenamiento analítico
- **Cloud Composer (Airflow)**: Orquestación de tareas
- **SQL Server + API + CSV**: Fuentes de datos heterogéneas
- **CI/CD**: Versionado y despliegue con GitHub (ramas `dev` y `main`)

## Arquitectura General

1. Ingesta de datos desde CSV, API REST y SQL Server
2. Procesamiento en tres capas:
   - **Bronze**: Datos crudos
   - **Silver**: Datos limpios y transformados
   - **Gold**: Datos listos para análisis
3. Carga en BigQuery para visualización y análisis
4. Orquestación con Composer