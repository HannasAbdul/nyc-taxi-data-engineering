# NYC Taxi Data Engineering Pipeline

An end-to-end data engineering project that ingests NYC Yellow Taxi trip records, loads them into Snowflake, transforms them with dbt, and orchestrates the pipeline using Apache Airflow.

The project demonstrates practical data engineering concepts including reliable ingestion, data validation, idempotency, data quality testing, dimensional modelling, orchestration, and operational logging.

## 1. Project Architecture

```text
NYC TLC Official Data
        |
        v
Python Ingestion Pipeline
        |
        +--> Download Parquet and CSV files
        +--> Validate source files
        +--> Calculate SHA-256 checksums
        +--> Maintain ingestion manifest
        |
        v
Snowflake RAW Layer
        |
        v
dbt Transformations
        |
        +--> Staging
        +--> Intermediate
        +--> Marts
        |
        v
Analytics-ready Data Models

Apache Airflow orchestrates:
Python Ingestion --> dbt Build
```

## 2. Technology Stack

| Technology | Purpose |
|---|---|
| Python | Data ingestion and validation |
| Snowflake | Cloud data warehouse |
| SQL | Data transformation and analysis |
| dbt | Transformation modelling and testing |
| Apache Airflow | Workflow orchestration |
| Docker Compose | Containerized Airflow environment |
| PostgreSQL | Airflow metadata database |
| Git | Source control |
| Ubuntu Linux | Development environment |

## 3. Data Source

The project uses official NYC Taxi and Limousine Commission (TLC) Yellow Taxi trip records.

Source datasets:

- Yellow Taxi trip records: January 2025, Parquet format.
- Taxi zone lookup: CSV format.

The January 2025 Yellow Taxi dataset contains 3,475,226 source records.

## 4. Data Ingestion

The Python ingestion pipeline is responsible for downloading, validating, and loading source files into Snowflake.

Key implementation features:

- Source file validation.
- SHA-256 checksum calculation.
- Ingestion manifest tracking.
- Snowflake internal stage uploads.
- Controlled Snowflake COPY operations.
- Load-history reconciliation.
- Duplicate-load prevention.
- Safe handling of uncertain load outcomes.
- Structured application logging.

The ingestion process is designed to be idempotent: processing an already successfully loaded source file should not load it again.

## 5. Snowflake Data Warehouse

Database: `NYC_TAXI_DW`

Warehouse: `NYC_TAX_WH`

The RAW layer contains:

- `YELLOW_TRIPS`
- `TAXI_ZONES`

Initial source record counts:

| Table | Rows |
|---|---:|
| YELLOW_TRIPS | 3,475,226 |
| TAXI_ZONES | 265 |

## 6. dbt Transformation Layers

The dbt project follows a layered modelling approach.

### Staging

Standardizes and prepares raw source data.

- `stg_yellow_trips`
- `stg_taxi_zones`

### Intermediate

Combines trip records with taxi zone information and applies business logic.

- `int_trips_with_zones`

### Marts

Creates analytics-ready fact and dimension models.

- `fct_trips`
- `dim_date`
- `dim_taxi_zone`
- `dim_vendor`
- `dim_payment_type`
- `mart_daily_borough_revenue`

The fact and dimension models provide a dimensional structure suitable for analytical queries.

## 7. Data Quality and Reliability

The project includes dbt schema tests and custom SQL data tests.

Examples include:

- Not-null and uniqueness checks.
- Valid trip-duration checks.
- Distance-quality classification.
- Daily revenue grain validation.
- Fact and intermediate record reconciliation.
- Mart trip-count reconciliation.

Additional ingestion safeguards include checksum-based file identification, manifest tracking, controlled first-load approval, and Snowflake load-history checks.

## 8. Airflow Orchestration

Apache Airflow runs in Docker using LocalExecutor and PostgreSQL for metadata storage.

The main DAG is:

`nyc_taxi_ingestion`

Execution sequence:

1. Run the Python ingestion pipeline.
2. Confirm successful ingestion.
3. Execute `dbt build`.
4. Stop downstream execution if ingestion fails.

The DAG includes retry configuration and task execution timeouts.

A separate isolated failure-handling test was used to verify that downstream tasks are blocked when an upstream task fails.

## 9. Data Quality Findings

The project identified several characteristics of the source data.

- 3,473,155 trips passed the project's duration-quality rules and were included in the trip fact table.
- 2,071 trips were excluded from the fact table because of invalid duration classifications.
- Approximately 97.38% of intermediate records had valid recorded distances under the project's classification rules.
- Zero-distance records and extreme-distance records were retained for investigation rather than automatically treated as fraudulent or invalid.
- All 265 taxi zones passed the project's zone validation.

The distance threshold used to flag extreme trips is a project-defined quality rule, not an official TLC restriction.

## 10. Analytical Investigation

The project also investigated revenue anomalies and fare reconciliation.

One January 20, 2025 trip contained a reported total amount of $863,380.37, making it a significant daily revenue outlier.

This record was flagged for further investigation. The available evidence does not establish fraud or explain the cause.

The project also investigated differences between reported total amounts and component fare fields. Rather than imposing an unsupported universal fare equation, the pipeline preserves reported source totals and flags discrepancies for analysis.

## 11. Running the Project

### Prerequisites

- Python 3.12
- Docker and Docker Compose
- Snowflake account
- dbt-compatible Snowflake credentials

### Environment setup

Clone the repository and enter the project directory.

Create a virtual environment and install the project requirements:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create your local environment file:

```bash
cp .env.example .env
```

Populate `.env` with your own Snowflake credentials and required secrets.

Never commit `.env` to Git.

### Run ingestion

```bash
python scripts/run_ingestion.py
```

### Run dbt

```bash
cd nyc_taxi_dbt
dbt debug
dbt build
```

### Start Airflow

From the project root:

```bash
docker compose -f airflow/docker-compose.yaml up -d --build
```

The Airflow web interface is available locally at:

`http://localhost:8080`

Trigger the `nyc_taxi_ingestion` DAG from the Airflow interface.

Use the local Compose configuration for your development environment. The `docker-compose.example.yaml` file is provided as a sanitized configuration template.

## 12. Repository Structure

```text
nyc_taxi/
├── airflow/
│   ├── dags/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── docker-compose.yaml
│   └── docker-compose.example.yaml
├── data/
│   ├── raw/
│   └── metadata/
├── ingestion/
│   ├── src/
│   │   └── nyc_taxi/
│   └── tests/
├── nyc_taxi_dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── macros/
│   └── tests/
├── scripts/
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

## 13. Engineering Considerations and Future Improvements

Potential next improvements include:

- Automated unit and integration testing in CI/CD.
- Centralized monitoring and alerting.
- Configurable log rotation and retention.
- Incremental processing for additional monthly source files.
- Automated deployment and environment separation.
- More comprehensive recovery and failure-injection testing.

This project is a production-oriented portfolio implementation. Further security hardening, automated deployment, monitoring, and operational testing would be required before treating it as a production service.

## 14. Project Objective

The objective is to demonstrate the complete data engineering lifecycle: acquiring source data, validating it, loading it into a cloud warehouse, transforming it into analytical models, testing data quality, and orchestrating the workflow reliably.
