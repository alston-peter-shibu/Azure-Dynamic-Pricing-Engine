# Screenshots

| Screenshot | Description |
|------------|-------------|
| `kafka_running.png` | Azure VM showing both Kafka and Zookeeper containers running via `sudo docker ps` |
| `producer_output.png` | Local terminal showing producer sending events every 2 seconds to Kafka |
| `databricks_training.png` | Databricks training notebook output — model trained and logged to MLflow |
| `mlflow_experiment.png` | MLflow Experiments page showing `/pricing_model_experiment` run |
| `databricks_streaming.png` | Databricks streaming notebook showing Structured Streaming query running |
| `snowflake_raw.png` | Snowflake query result — `SELECT * FROM raw.pricing_stream` with predicted prices |
| `snowflake_marts.png` | Snowflake query result — `SELECT * FROM marts.fact_pricing` with demand classification |
| `dbt_run.png` | Terminal showing `dbt run` output with both models SUCCESS |
| `airflow_dag.png` | Airflow UI showing `pricing_pipeline` DAG with both tasks green |
