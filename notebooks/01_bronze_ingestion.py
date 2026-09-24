# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
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

# CELL 2 - load secrets
# load_dotenv("configs/.env")

# key = os.getenv("JOOBLE_API_KEY")

key = dbutils.secrets.get(
    catalog='workspace',
    schema='default',
    key='jooble_api_key'
)

url = f"https://jooble.org/api/{key}"

adzuna_id = dbutils.secrets.get(
    catalog='workspace',
    schema='default',
    key='adzuna_app_id'
)

adzuna_key = dbutils.secrets.get(
    catalog='workspace',
    schema='default',
    key='adzuna_app_key'
)

params = {
    "app_id": {adzuna_id},
    "app_key": {adzuna_key},
    "what_and": "data engineer",
    "what_phrase": "data engineer",
    # "where": "Noida",
    "full_time": 1,
    "sort_by": "date",
    "results_per_page": 200
}

country = 'in'
# page = 1
# adzuna_url = f"https://api.adzuna.com/v1/api/jobs/in/search/{page}"

# response = requests.get(adzuna_url, params=params)

# COMMAND ----------

total_jobs = []
page = 1
while True:
    adzuna_url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
    response = requests.get(adzuna_url, params=params)
    data = response.json()

    jobs = data['results']
    print(len(jobs))
    if len(jobs) == 0:
        break
    total_jobs.extend(jobs)
    print(len(total_jobs))
    if len(total_jobs) > 10000:
        break
    
    # if page > 4:
    #     break
    page += 1

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),

    StructField("company", StructType([
        StructField("display_name", StringType(), True)
    ]), True),

    StructField("location", StructType([
        StructField("display_name", StringType(), True),
        StructField("area", ArrayType(StringType()), True)
    ]), True),

    StructField("created", StringType(), True),
    StructField("contract_type", StringType(), True),
    StructField("contract_time", StringType(), True),
    StructField("salary_min", DoubleType(), True),
    StructField("salary_max", DoubleType(), True),
    StructField("salary_is_predicted", StringType(), True),
    StructField("redirect_url", StringType(), True)
])

raw_df = spark.createDataFrame(total_jobs, schema=schema)
display(raw_df)

# COMMAND ----------

# all_jobs = []
# page = 1
# fetched = 0

# while True:

#     payload = {
#         # "keywords": "Data Engineer",
#         "location": "India",
#         "page": page,
#         "ResultOnPage": 30
#     }

#     response = requests.post(url, json=payload)
#     data = response.json()

#     total = data["totalCount"]
#     jobs = data["jobs"]

#     all_jobs.extend(jobs)
#     fetched += len(jobs)

#     print(f"Page {page}: fetched {len(jobs)} | Total fetched: {fetched}/{total}")

#     if fetched >= total or not jobs:
#         break

#     page += 1

# print(f"Final jobs fetched: {len(all_jobs)}")

# raw_df = spark.createDataFrame(all_jobs)
# display(raw_df)
raw_df = raw_df.dropDuplicates()

# COMMAND ----------

raw_df.printSchema();
raw_df.count();
raw_df.filter(F.col("id").isNull()).count()
raw_df = (
    raw_df
    # .withColumn('job_source', F.lit('jooble'))
    .withColumn('ingested_at', F.current_timestamp())
    .withColumn('job_id',
                F.sha2(
                    F.concat_ws(
                        '|',
                        F.lower(F.trim(F.col('id'))),
                        F.lower(F.trim(F.col("title")))
                    ),
                    256
                ))
)

# COMMAND ----------

display(raw_df)

# COMMAND ----------

# Data quality checks

# 1. critical columns null values
critical = ['id', 'title', 'description', 'company.display_name', 'location.display_name', 'created', 'redirect_url', 'job_id']

raw_df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in critical
]).show()


# 2. no duplicate job_id
raw_df.groupBy("job_id") \
    .count() \
    .filter(F.col('count') > 1) \
    .show()

raw_df.select(F.countDistinct("job_id")).show()

# 3 null job_id
raw_df.select(
    F.count(F.when(F.col('job_id').isNull(), 1)).alias('null_ids')
    ).show()

# data vloume
rows = raw_df.count()
print(f"Rows ingest: {rows}")

# COMMAND ----------

# writing data into delta tables
raw_df.write \
    .format('delta') \
    .mode('append') \
    .saveAsTable('workspace.bronze.jobs_bronze')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- reading data from the delta table
# MAGIC select * from workspace.bronze.jobs_bronze