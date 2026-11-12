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
from pyspark.sql.functions import col, explode, upper, lit
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
bronze_file_location = f"Files/raw/domains/"
silver_table_name = "FUAM_Staging_Lakehouse.domains_silver"
gold_table_name = "domains_flatten"
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
exploded_df = bronze_df.select(explode("domains").alias("d"))

# Extract json objects to tabular form
extracted_df = exploded_df.select(col("d.*"))

# Rename existing columns
extracted_df = (
    extracted_df
        .withColumnRenamed("id", "DomainId")
        .withColumnRenamed("parentDomainId", "ParentDomainId")
        .withColumnRenamed("displayName", "DomainName")
        .withColumnRenamed("contributorsScope", "DomainContributorsScope")
        .withColumnRenamed("description", "DomainDescription")
)

# Handle ParentDomainId safely
if "ParentDomainId" in extracted_df.columns:
    silver_df = (
        extracted_df
            .withColumn("DomainId", upper(col("DomainId")))
            .withColumn("ParentDomainId", upper(col("ParentDomainId")))
    )
else:
    silver_df = (
        extracted_df
            .withColumn("DomainId", upper(col("DomainId")))
            .withColumn("ParentDomainId", lit(None).cast("string"))
    )

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

# Get Silver table data for Domains - Flatten structure
domains_generic_query = """
SELECT 
     gen.DomainId,
     gen.DomainContributorsScope,
     gen.DomainName AS OriginalDomainName,
     gen.ParentDomainId AS OriginalParentDomainId,
     CASE 
          WHEN gen.ParentDomainId IS NULL THEN gen.DomainName 
          ELSE md.DomainName
     END AS MainDomainName,
     CASE 
          WHEN gen.ParentDomainId IS NOT NULL THEN gen.DomainName 
          ELSE 'Without Subdomain'
     END AS SubDomainName,
     CASE 
          WHEN gen.ParentDomainId IS NOT NULL THEN 1 
          ELSE 0 
          END AS IsSubDomain     
FROM """ + silver_table_name + """ AS gen LEFT OUTER JOIN """ + silver_table_name + """ AS md on gen.ParentDomainId = md.DomainId """

domains_generic_silver_df = spark.sql(domains_generic_query)

if display_data:
     display(domains_generic_silver_df)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

domains_generic_silver_df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable(f"{gold_table_name}")

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
