# Databricks notebook source
# Cell 1 — Install dependencies
# MAGIC %pip install mlflow==2.13.0 databricks-sdk==0.28.0 scikit-learn

# COMMAND ----------
# Cell 2 — Restart kernel
dbutils.library.restartPython()

# COMMAND ----------
# Cell 3 — Train and log model
import mlflow
import mlflow.sklearn
import random
import pandas as pd
from sklearn.ensemble import RandomForestRegressor

# Generate synthetic training data
data = [{"city": random.choice(['London','Manchester']),
          "demand": random.randint(50,200), "supply": random.randint(20,150),
          "hour": 12, "day_of_week": 2} for _ in range(1000)]

df = pd.DataFrame(data)
df['price'] = (df['demand'] / df['supply']) * 10

X = df[['demand', 'supply', 'hour', 'day_of_week']]
y = df['price']

mlflow.set_experiment('/pricing_model_experiment')

with mlflow.start_run():
    model = RandomForestRegressor(n_estimators=20)
    model.fit(X, y)
    mlflow.log_param('num_trees', 20)
    mlflow.sklearn.log_model(model, 'model')
    print('Model trained and logged to MLflow')
