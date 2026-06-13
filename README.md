# Real-Time Dynamic Pricing Pipeline on Azure

A production-style **real-time streaming data platform** for dynamic pricing using **Apache Kafka, Azure Databricks (Spark Structured Streaming), MLflow, Snowflake (with Streams/CDC), dbt, and Apache Airflow**.

The system simulates live demand/supply events, performs streaming ML inference to predict prices, persists data across raw and analytical layers in Snowflake, transforms it via incremental dbt models, and orchestrates the batch transformation layer with Airflow.

---

## Architecture Overview
```text
Local Python Event Producer
        ↓
       Kafka (Azure VM)
        ↓
Azure Databricks (Spark Structured Streaming + ML Inference)
        ↓
───────────────────────────────
| Persist & Transform          |
|                              |
| Snowflake RAW                |
| └── pricing_stream (CDC via Snowflake Streams)
|                  │
|                  ↓
|            dbt models
|            ├── stg_pricing   (cleaned, enriched)
|            └── fact_pricing  (incremental, demand-classified)
|                  │
|                  ↓
| Snowflake MARTS               |
| └── fact_pricing               |
───────────────────────────────
        ↓
   Apache Airflow
   (orchestrates dbt run/test every 5 min)
```

---

## System Characteristics

- Event-driven, streaming-first architecture
- Genuine Spark Structured Streaming (`readStream`/`writeStream`/`foreachBatch`) with checkpointing
- CDC-enabled ingestion via Snowflake Streams
- Clear separation of raw ingestion, transformation, and orchestration layers
- ML-driven price inference embedded directly in the streaming pipeline
- Incremental dbt models for efficient batch transformation

---

## Technology Stack

| Layer           | Technology                                |
|-----------------|---------------------------------------------|
| Ingestion       | Apache Kafka (on Azure VM)                |
| Processing      | Azure Databricks, PySpark Structured Streaming |
| ML              | MLflow, scikit-learn (RandomForestRegressor) |
| Storage / DW    | Snowflake (Streams for CDC)               |
| Transformation  | dbt Core (incremental models)             |
| Orchestration   | Apache Airflow (Dockerized)               |
| Infrastructure  | Azure VM, Azure Databricks, Docker        |
| Language        | Python, SQL                               |

---

## Project Structure

```text
dynamic-pricing/
│
├── producer/
│   └── producer.py              # Generates synthetic pricing events, sends to Kafka
│
├── kafka/
│   └── docker-compose.yml       # Kafka + Zookeeper, deployed on Azure VM
│
├── spark/
│   └── streaming_consumer.py    # Reference copy of Databricks streaming logic
│
├── pricing_project/              # dbt project
│   ├── models/
│   │   ├── sources.yml
│   │   ├── stg_pricing.sql
│   │   └── fact_pricing.sql
│   └── dbt_project.yml
│
├── airflow/
│   ├── docker-compose.yml
│   └── dags/
│       └── pricing_pipeline.py  # DAG: dbt run -> dbt test, every 5 min
│
├── architecture/
│   └── dynamic_pricing_pipeline.png   # Architecture diagram
│
├── screenshots/
│   ├── kafka_running.png
│   ├── producer_output.png
│   ├── databricks_training.png
│   ├── databricks_streaming.png
│   ├── mlflow_experiment.png
│   ├── snowflake_raw.png
│   ├── snowflake_marts.png
│   ├── dbt_run.png
│   └── airflow_dag.png
│
└── README.md
```

---

## Data Flow

### 1. Event Ingestion
A Python-based generator produces synthetic pricing events (`city`, `demand`, `supply`, `event_time`) and publishes them to a Kafka topic (`pricing`) running on an Azure VM.

### 2. Streaming Processing + ML Inference
Azure Databricks consumes Kafka events using Spark Structured Streaming (`readStream`). JSON payloads are parsed into structured DataFrames. Each micro-batch is passed to a trained MLflow model (`RandomForestRegressor`) to generate a `predicted_price`, then written to Snowflake via `foreachBatch`.

### 3. CDC-Enabled Raw Storage
Predictions and raw event data are inserted into `RAW.PRICING_STREAM` in Snowflake. A Snowflake Stream (`pricing_stream_changes`) tracks all inserts for downstream CDC-based processing.

### 4. Transformation Layers
- **stg_pricing**: cleans and enriches raw data — renames columns, casts types, derives `demand_supply_ratio`, `event_hour`, `event_day_of_week`
- **fact_pricing**: incremental model — classifies each event into `high_demand` / `normal` / `low_demand` based on the demand/supply ratio, processing only new rows on each run

### 5. Orchestration
Apache Airflow (Dockerized, SequentialExecutor) runs a DAG every 5 minutes that executes `dbt run` followed by `dbt test` against the dbt project.

---

## ML Inference Logic (Example)

```python
model = mlflow.sklearn.load_model('runs:/<RUN_ID>/model')
pdf['predicted_price'] = model.predict(pdf[['demand','supply','hour','day_of_week']])
```

The design allows easy replacement with a more sophisticated model or periodic retraining via a separate Airflow-orchestrated job.

## Demand Classification Logic (dbt)

```sql
CASE
    WHEN demand_supply_ratio > 1.5 THEN 'high_demand'
    WHEN demand_supply_ratio < 0.7 THEN 'low_demand'
    ELSE 'normal'
END AS demand_flag
```

---

## Power BI / Reporting (Future Extension)

`MARTS.FACT_PRICING` is structured as a query-ready analytical table (`city`, `predicted_price`, `demand_flag`, `event_hour`, etc.) and can be connected directly to Power BI or any BI tool for:

- Real-time price trend monitoring by city
- Demand classification distribution
- Hourly/daily demand pattern analysis

---

## Technical Highlights

- Implemented real-time ingestion using Apache Kafka on an Azure VM
- Built genuine Spark Structured Streaming pipelines (`readStream`/`writeStream`/`foreachBatch`) with checkpointing on Databricks
- Trained and registered an ML model (RandomForestRegressor) using MLflow for real-time inference
- Enabled CDC on Snowflake using Streams for incremental ingestion tracking
- Designed incremental dbt models for efficient, business-ready transformations
- Implemented demand classification logic using SQL CASE expressions in dbt
- Orchestrated batch transformations using Apache Airflow (Dockerized)
- Deployed and configured Azure infrastructure including VM networking, NSGs, and Databricks workspace

---

## Execution (High-Level)

1. Start Kafka broker on Azure VM
2. Run Python event producer locally
3. Start Databricks Structured Streaming notebook
4. Verify rows landing in `RAW.PRICING_STREAM` in Snowflake
5. Start Airflow — confirm `dbt run` / `dbt test` succeed every 5 minutes
6. Verify `MARTS.FACT_PRICING` populated with classified, business-ready data

---

## Design Considerations

- Kafka decouples the producer from downstream processing for scalability
- Spark Structured Streaming with checkpointing provides fault-tolerant, continuous processing
- Snowflake Streams enable CDC-based incremental ingestion tracking
- dbt incremental models avoid full-table reprocessing on every run
- Airflow provides scheduled, observable orchestration of the batch transformation layer
- Clear separation of streaming (real-time) and batch (5-min) responsibilities

---

## Author

**N S Alston Peter**
Data Engineering | Data Engineer | Azure
