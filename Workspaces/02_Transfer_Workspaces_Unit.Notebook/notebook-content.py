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
# META       "default_lakehouse_workspace_id": "7ac9875f-7544-4cd9-ae79-cef02837d23f"
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# #### Workspaces
# 
# ##### Data ingestion strategy:
# <mark style="background: #88D5FF;">**REPLACE**</mark>
# 
# ##### Related pipeline:
# 
# **Load_PBI_Workspaces_E2E**
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

import requests
from pyspark.sql.functions import col, lit, udf, explode, to_date, json_tuple, from_json, schema_of_json, get_json_object, upper
from pyspark.sql.types import StringType, json , ArrayType, StructType, StructField
from pyspark.sql import SparkSession
import json
from delta.tables import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
import datetime
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

# MARKDOWN ********************

# #### Workspaces via PBI API

# CELL ********************

## Variables
pbi_bronze_file_location = f"Files/raw/workspaces/"
pbi_silver_table_name = "FUAM_Staging_Lakehouse.workspaces_silver"
pbi_gold_table_name = "workspaces"
pbi_gold_table_name_with_prefix = f"Tables/{pbi_gold_table_name}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean Silver table, if exists
if spark.catalog.tableExists(pbi_silver_table_name):
    del_query = "DELETE FROM " + pbi_silver_table_name
    spark.sql(del_query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get Bronze data
bronze_df = spark.read.option("multiline", "true").json(pbi_bronze_file_location)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Explode json subset structure
exploded_df = bronze_df.select(explode("value").alias("d"))

# Extract json objects to tabular form
extracted_df = exploded_df.select(col("d.*"))

# Convert key(s) to upper case
extracted_df = extracted_df.withColumn("id", f.upper(f.col("id")))
extracted_df = extracted_df.withColumn("capacityId", f.upper(f.col("capacityId")))

# Generate empty description column in case it is not available
if  not ("description" in extracted_df.columns):
    print("Create empty description column")
    extracted_df = extracted_df.withColumn("description", lit(""))

# Select all columns
silver_df = extracted_df.select(
    col("capacityId").alias("CapacityId"),
    col("id").alias("WorkspaceId"),
    col("capacityMigrationStatus").alias("CapacityMigrationStatus"),
    col("defaultDatasetStorageFormat").alias("DefaultDatasetStorageFormat"),
    col("description").alias("Description"),
    col("hasWorkspaceLevelSettings ").alias("HasWorkspaceLevelSettings"),
    col("isOnDedicatedCapacity").alias("IsOnDedicatedCapacity"),
    col("isReadOnly").alias("IsReadOnly"),
    col("name").alias("WorkspaceName"),
    col("state").alias("State"),
    col("type").alias("Type")
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if display_data:
    display(silver_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write prepared bronze_df to silver delta table
silver_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable(pbi_silver_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# This function maps and merges the silver data to gold dynamically
def write_silver_to_gold(pbi_silver_table_name, gold_table_name, ids):
    # TODO
    query = "SELECT *, current_timestamp() AS fuam_modified_at, False as fuam_deleted  FROM " + pbi_silver_table_name 
    silver_df = spark.sql(query)
    
    if spark._jsparkSession.catalog().tableExists('FUAM_Lakehouse', pbi_gold_table_name):
        # if exists -> MERGE to gold
        print("Gold table exists and will be merged.")
        gold_df = DeltaTable.forName(spark, pbi_gold_table_name)


        gold_columns = gold_df.toDF().columns
        silver_columns = silver_df.columns
        combined_columns = list(set(gold_columns) | set(silver_columns))
        id_cols = {}
        merge_id_stmt = ''
        for col in combined_columns:
            if col in ids:
                merge_id_stmt =  merge_id_stmt +  " t." + col + " = s." + col + " and"
                id_cols[col] = "s." + col

                
        # delete last and in merge id statement
        merge_id_stmt = merge_id_stmt[:-4]


        # Merge silver (s = source) to gold (t = target)
        try:
            merge = (gold_df.alias('t') \
            .merge(silver_df.alias('s'), merge_id_stmt )) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .whenNotMatchedBySourceUpdate( condition = "t.fuam_deleted == False or t.fuam_deleted IS NULL", set = {"fuam_deleted" : "True", "fuam_modified_at": "current_timestamp()"} )
            
            merge.execute()
        except:
        # In case the tables already exist, but the fuam column are not existent because of an old version do merge whenNotMatchedBySourceUpdate
            merge = (gold_df.alias('t') \
            .merge(silver_df.alias('s'), merge_id_stmt )) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
                        
            merge.execute()

    else:
        # else -> INSERT to gold
        print("Gold table will be created.")

        silver_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable(pbi_gold_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge semantic model refreshes to gold table
write_silver_to_gold(pbi_silver_table_name, pbi_gold_table_name, ['WorkspaceId'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# write history of bronze files
mssparkutils.fs.cp(pbi_bronze_file_location, pbi_bronze_file_location.replace("Files/raw/", "Files/history/") + datetime.datetime.now().strftime('%Y/%m/%d') + "/", True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Workspaces via Fabric API

# CELL ********************

## Variables
fb_bronze_file_location = f"Files/raw/fabric_workspaces/"
fb_silver_table_name = "FUAM_Staging_Lakehouse.fabric_workspaces_silver"
fb_gold_table_name = "fabric_workspaces"
fb_gold_table_name_with_prefix = f"Tables/{fb_gold_table_name}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean Silver table, if exists
if spark.catalog.tableExists(fb_silver_table_name):
    del_query = "DELETE FROM " + fb_silver_table_name
    spark.sql(del_query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get Bronze data
fb_bronze_df = spark.read.option("multiline", "true").json(fb_bronze_file_location)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if display_data:
    display(fb_bronze_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Explode json subset structure
fb_exploded_df = fb_bronze_df.select(explode("workspaces").alias("d"))
# Extract json objects to tabular form
fb_extracted_df = fb_exploded_df.select(col("d.*"))


if "domainId" not in fb_extracted_df.columns:
    fb_extracted_df = fb_extracted_df.withColumn("domainId",lit(None) )

fb_extracted_df = fb_extracted_df.withColumnRenamed("capacityId", "CapacityId")\
.withColumnRenamed("id", "WorkspaceId")\
.withColumnRenamed("domainId", "DomainId")

fb_extracted_df = fb_extracted_df.withColumn("DomainId", upper("DomainId"))\
.withColumn("CapacityId", upper("CapacityId"))\
.withColumn("WorkspaceId", upper("WorkspaceId"))\

# Version 2026.5.1 Add empty tags column in case not tags are present in the tenant
tags_schema = ArrayType(StructType([
    StructField("displayName", StringType(), True),
    StructField("id",          StringType(), True),
]))

if "tags" not in fb_extracted_df.columns:
    fb_extracted_df = fb_extracted_df.withColumn("tags", lit(None).cast(tags_schema) )


if display_data:
    display(fb_extracted_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Rename columns if needed
fb_silver_df = fb_extracted_df

if display_data:
    display(fb_silver_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write prepared bronze_df to silver delta table
fb_silver_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(fb_silver_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get Silver table data
query = """
SELECT *
FROM """ + fb_silver_table_name


fb_silver_df = spark.sql(query)

if display_data:
     display(fb_silver_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#fb_silver_df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable(fb_gold_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Merge Workspace Tables

# CELL ********************

df_workspaces = spark.sql("SELECT * FROM FUAM_Lakehouse.workspaces")
display(df_workspaces)

if "DomainId" not in df_workspaces.columns:
    print("Add DomainId to table")
    spark.sql("ALTER TABLE workspaces ADD COLUMNS (DomainId STRING) ")

if "tags" not in df_workspaces.columns:
    print("Add tags to table")
    spark.sql("ALTER TABLE workspaces ADD COLUMNS (tags ARRAY<STRUCT<displayName: STRING, id: STRING>>) ")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO workspaces trg
# MAGIC USING FUAM_Staging_Lakehouse.fabric_workspaces_silver src 
# MAGIC ON trg.WorkspaceId = src.WorkspaceId
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET DomainId = src.DomainId,
# MAGIC tags = src.tags

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_workspace_tags = spark.sql("""
SELECT WorkspaceId, UPPER(tags.id) tagId, tags.displayName FROM (
SELECT WorkspaceId, EXPLODE(tags)  tags FROM workspaces
WHERE tags IS NOT NULL
) a
""")

df_workspace_tags.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("workspace_tags")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write history of bronze files
from datetime import datetime
mssparkutils.fs.cp(fb_bronze_file_location, fb_bronze_file_location.replace("Files/raw/", "Files/history/") + datetime.now().strftime('%Y/%m/%d') + "/", True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
