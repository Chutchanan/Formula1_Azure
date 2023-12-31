# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1 - Ingest pit_stops.json file (MultiLine json file)

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

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("stop", IntegerType(), True),
                                     StructField("lap", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("duration", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True),
])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename columns and add ingestion date

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed("raceId", "race_id") \
                                          .withColumnRenamed("driverId", "driver_id") \
                                          .withColumn("ingestion_date", current_timestamp()) \
                                          .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write data to datalake as delta

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(pit_stops_final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops;

# COMMAND ----------

dbutils.notebook.exit("Success")