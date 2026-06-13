# Databricks notebook source
# Cell 1 — Install dependencies
# MAGIC %pip install mlflow==2.13.0 databricks-sdk==0.28.0 scikit-learn kafka-python snowflake-connector-python

# COMMAND ----------
# Cell 2 — Restart kernel
dbutils.library.restartPython()

# COMMAND ----------
# Cell 3 — Structured Streaming inference (readStream + foreachBatch)
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import mlflow
import mlflow.sklearn
import pandas as pd

# Load MLflow model — replace with your training run ID
model = mlflow.sklearn.load_model('runs:/<YOUR_RUN_ID>/model')
print("Model loaded successfully")

# Schema for incoming Kafka JSON messages
schema = StructType([
    StructField('city', StringType()),
    StructField('demand', IntegerType()),
    StructField('supply', IntegerType()),
    StructField('event_time', StringType())
])

# Read stream from Kafka on Azure VM
df = spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers', '<KAFKA_VM_PUBLIC_IP>:9092') \
    .option('subscribe', 'pricing') \
    .option('startingOffsets', 'latest') \
    .load()

parsed = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col('json'), schema).alias('data')) \
    .select('data.*')

# Write each micro-batch to Snowflake with ML inference
def write_to_snowflake(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    import os
    os.environ['SNOWFLAKE_HOME'] = '/tmp/snowflake'
    os.makedirs('/tmp/snowflake', exist_ok=True)
    import snowflake.connector

    pdf = batch_df.toPandas()
    pdf['hour'] = 12
    pdf['day_of_week'] = 2
    pdf['predicted_price'] = model.predict(pdf[['demand','supply','hour','day_of_week']])

    conn = snowflake.connector.connect(
        account='<SNOWFLAKE_ACCOUNT>',
        user='<SNOWFLAKE_USER>',
        password='<SNOWFLAKE_PASSWORD>',
        database='PRICING_DB',
        schema='RAW',
        warehouse='PRICING_WH'
    )
    cursor = conn.cursor()
    for _, row in pdf.iterrows():
        cursor.execute("""
            INSERT INTO raw.pricing_stream (city, demand, supply, event_time, predicted_price)
            VALUES (%s, %s, %s, %s, %s)
        """, (row['city'], int(row['demand']), int(row['supply']), row['event_time'], float(row['predicted_price'])))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Batch {batch_id}: wrote {len(pdf)} rows")

# Start the streaming query
query = parsed.writeStream \
    .foreachBatch(write_to_snowflake) \
    .option('checkpointLocation', '/tmp/checkpoints/pricing_v2') \
    .start()

print("Structured Streaming query started")
