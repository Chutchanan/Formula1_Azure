# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1 - Ingest drivers.json file (Nested json format)

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, DateType
from pyspark.sql.functions import col, current_timestamp, to_timestamp, lit, concat

# COMMAND ----------

raw_folder_path = "/mnt/formular1dl2023/raw"
processed_folder_path = "/mnt/formular1dl2023/processed"

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
])

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                     StructField("driverRef", StringType(), True),
                                     StructField("number", IntegerType(), True),
                                     StructField("code", StringType(), True),
                                     StructField("name", name_schema),
                                     StructField("dob", DateType(), True),
                                     StructField("nationality", StringType(), True),
                                     StructField("url", StringType(), True),
])

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC 1. driverId to driver_id
# MAGIC 2. driverRef to driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concat of forename and surname

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("file_date", lit(v_file_date)) \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Drop unwanted columns

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop("url")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write data to datalake as Parquet

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")