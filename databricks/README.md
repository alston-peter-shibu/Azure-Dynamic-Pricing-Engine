# Databricks Notebooks

This folder contains the Databricks notebooks used in the pipeline.

| Notebook | Purpose |
|----------|---------|
| `notebooks/01_train_model.py` | Trains a RandomForestRegressor using synthetic pricing data and logs the model to MLflow |
| `notebooks/02_streaming_inference.py` | Spark Structured Streaming pipeline — consumes Kafka events, applies ML inference, writes results to Snowflake |
