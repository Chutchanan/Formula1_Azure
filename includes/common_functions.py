# Databricks notebook source
# MAGIC %md
# MAGIC ##### Merge data to delta table

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
'''
Merge (Update/Insert) data
Input Parameter
1. input_df = The new data that we want to insert to the existing table
2. db_name = database name
3. table_name = table_name
4. folder_path = folder_path
5. merge_condition = condition(s) that we use to merge the table
6. partition_column = column that we want to partition.
'''
  spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
  from delta.tables import DeltaTable
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
    deltaTable.alias("tgt").merge(
        input_df.alias("src"),
        merge_condition) \
      .whenMatchedUpdateAll()\
      .whenNotMatchedInsertAll()\
      .execute()
  else:
    input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")
