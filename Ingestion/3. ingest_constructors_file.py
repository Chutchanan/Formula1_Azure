# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1 - Ingest constructors.json file

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

constructors_schema = StructType(fields=[StructField("constructorId", IntegerType(), False),
                                     StructField("constructorRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("nationality", StringType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

constructors_df = spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Drop url column

# COMMAND ----------

constructors_dropped_df = constructors_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

constructors_final_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                          .withColumnRenamed("constructorRef", "constructor_ref") \
                                          .withColumn("file_date", lit(v_file_date)) \
                                          .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write data to datalake as delta

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Success")