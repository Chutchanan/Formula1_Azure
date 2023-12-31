# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1 - Ingest races.csv file

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, current_timestamp, to_timestamp, lit, concat

# COMMAND ----------

raw_folder_path = "/mnt/formular1dl2023/raw"
processed_folder_path = "/mnt/formular1dl2023/processed"

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("year", IntegerType(), True),
                                     StructField("round", IntegerType(), True),
                                     StructField("circuitId", IntegerType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("date", StringType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read.option("header",True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Concat datetime column

# COMMAND ----------

races_concat_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Select only required columns

# COMMAND ----------

races_selected_df = races_concat_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("race_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Rename columns

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
                                          .withColumnRenamed("year", "race_year") \
                                          .withColumnRenamed("circuitId", "circuit_id") \
                                          .withColumn("file_date", lit(v_file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Add ingestion date to dataframe

# COMMAND ----------

races_final_df = races_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Write data to datalake as delta

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")