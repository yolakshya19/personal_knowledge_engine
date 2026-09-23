# Databricks notebook source
# CELL 1 - imports
import requests
import json
from datetime import datetime, timezone
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)
from dotenv import load_dotenv

# COMMAND ----------

# CELL 2 - load env
load_dotenv("configs/.env")

key = os.getenv("JOOBLE_API_KEY")

url = f"https://jooble.org/api/{key}"

# COMMAND ----------

payload = {"location": "India"}

response = requests.post(url, json=payload)

print("Status:", response.status_code)
data = response.json()
print(json.dumps(data, indent= 2, ensure_ascii= False))

# print("Total jobs:", data["totalCount"])

# for job in data["jobs"]:
#     print(f"\nTitle: {job['title']}")
#     print(f"Company: {job.get('company')}")
#     print(f"Location: {job.get('location')}")
#     print(f"Updated: {job.get('updated')}")
#     print(f"URL: {job.get('link')}")

# COMMAND ----------
