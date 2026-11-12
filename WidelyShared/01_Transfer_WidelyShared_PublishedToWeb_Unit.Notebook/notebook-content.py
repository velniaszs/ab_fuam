# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7d41449a-e630-46c5-bbfe-e869e7344639",
# META       "default_lakehouse_name": "FUAM_Lakehouse",
# META       "default_lakehouse_workspace_id": "7ac9875f-7544-4cd9-ae79-cef02837d23f",
# META       "known_lakehouses": []
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# #### Capacities
# 
# ##### Data ingestion strategy:
# <mark style="background: #88D5FF;">**REPLACE**</mark>
# 
# ##### Related pipeline:
# 
# **Load_Capacities_E2E**
# 
# ##### Source:
# 
# **Files** from FUAM_Lakehouse folder **bronze_file_location** variable
# 
# ##### Target:
# 
# **1 Delta table** in FUAM_Lakehouse 
# - **gold_table_name** variable value


# CELL ********************

from datetime import datetime, timedelta
from pyspark.sql.functions import col, explode, upper
from delta.tables import *
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true") # needed for automatic schema evolution in merge 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

## Parameters
display_data = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Variables
bronze_file_location = f"Files/raw/widely_shared_artifacts/publishedToWeb"
silver_table_name = "FUAM_Staging_Lakehouse.widelyshared_publishToWeb_silver"
gold_table_name = "widelyshared_publishToWeb"
gold_table_name_with_prefix = f"Tables/{gold_table_name}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean Silver table, if exists
if spark.catalog.tableExists(silver_table_name):
    del_query = "DELETE FROM " + silver_table_name
    spark.sql(del_query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get Bronze data
bronze_df = spark.read.option("multiline", "true").json(bronze_file_location)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if display_data:
    display(bronze_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Explode json subset structure
exploded_df = bronze_df.select(explode("ArtifactAccessEntities").alias("d"))

# This prevents the notebook running into an error when no widely share organization links are existant in the tenant
if exploded_df.count() == 0 :
    notebookutils.notebook.exit("No widely share organization links available")

# Extract json objects to tabular form
extracted_df = exploded_df.select(col("d.*"))
extracted_df = extracted_df.withColumnRenamed("displayName", "item_displayName")\

extracted_df = extracted_df.select(col("*"),col("sharer.*"))

try:
  extracted_df = extracted_df.withColumnRenamed("displayName", "sharer_displayName")\
  .withColumnRenamed("emailAddress", "sharer_emailAddress")\
  .withColumnRenamed("graphId", "sharer_graphId")\
  .withColumnRenamed("identifier", "sharer_identifier")\
  .withColumnRenamed("principalType", "sharer_principalType")

except:
    print("Error at rename")

extracted_df = extracted_df.withColumn("ItemId", upper("artifactId")).drop("artifactId").drop("sharer")


if display_data:
    display(extracted_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df = extracted_df
if display_data:
    display(silver_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write prepared bronze_df to silver delta table
silver_df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable(silver_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get Silver table data
query = """
SELECT 
     to_date(current_timestamp()) AS TransferDate
     ,current_timestamp() AS TransferDateTime
     ,*
FROM """ + silver_table_name


silver_df = spark.sql(query)

if display_data:
     display(silver_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable(gold_table_name)

 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write history of bronze files
mssparkutils.fs.cp(bronze_file_location, bronze_file_location.replace("Files/raw/", "Files/history/") + datetime.now().strftime('%Y/%m/%d') + "/", True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
