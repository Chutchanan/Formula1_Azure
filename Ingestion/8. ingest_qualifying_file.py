# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1 - Ingest qualifying.json file (multiple file of multiLine json file)

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
from pyspark.sql.functions import col, current_timestamp, to_timestamp, lit, concat

# COMMAND ----------

raw_folder_path = "/mnt/formular1dl2023/raw"
processed_folder_path = "/mnt/formular1dl2023/processed"

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                     StructField("raceId", IntegerType(), True),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("constructorId", IntegerType(), True),
                                     StructField("number", IntegerType(), True),
                                     StructField("position", IntegerType(), True),
                                     StructField("q1", StringType(), True),
                                     StructField("q2", StringType(), True),
                                     StructField("q3", StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename columns and add ingestion date

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("raceId", "race_id") \
                                          .withColumnRenamed("driverId", "driver_id") \
                                          .withColumnRenamed("qualifyId", "qualify_id") \
                                          .withColumnRenamed("constructorId", "constructor_id") \
                                          .withColumn("ingestion_date", current_timestamp()) \
                                          .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write data to datalake as delta

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.qualifying;

# COMMAND ----------

dbutils.notebook.exit("Success")