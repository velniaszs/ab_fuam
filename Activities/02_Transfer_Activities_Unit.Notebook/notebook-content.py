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

# #### Activities
# 
# ##### Data ingestion strategy:
# <mark style="background: lightgreen;">**APPEND**</mark>
# 
# ##### Related pipeline:
# 
# **Load_Activities_E2E**
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

from pyspark.sql.functions import col, explode, to_date, date_format, lit, upper, md5
import pyspark.sql.functions as f
from delta.tables import *
import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

## Parameters
display_data = True
activity_days_in_scope = 2 # It will be used for anonymization only
anonymize_tables = False # Controls whether sensitive information from activity logs is hashed.
anonymize_files = False # Controls whether raw JSON files from Lakehouse Files' activity logs are deleted. 
anonymize_after_days = 90 # Controls after how many days table anonymization has to happen.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Variables
bronze_file_location = f"Files/raw/activities/*/"
silver_table_name = "FUAM_Staging_Lakehouse.activities_silver"
gold_table_name = "activities"
gold_table_name_with_prefix = f"Tables/{gold_table_name}"

last_activity_date = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# This function converts all complex data types to StringType
def convert_columns_to_string(schema, parent = "", lvl = 0):
    """
    Input:
    - schema: Dataframe schema as StructType
    
    Output: List
    Returns a list of columns in the schema casting them to String to use in a selectExpr Spark function.
    """
    
    lst=[]
    
    for x in schema:
        # check if complex datatype has to be converted to string
        if str(x.dataType) in {"DateType()", "StringType()", "BooleanType()", "LongType()", "IntegerType()", "DoubleType()", "FloatType()"}:
            # no need to convert
            lst.append("{col}".format(col=x.name))
        else:
            # it has to be converted
            # print(str(x.dataType))
            lst.append("cast({col} as string) as {col}".format(col=x.name))

    return lst

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
exploded_df = bronze_df.select(explode("activityEventEntities").alias("d"))

del bronze_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Select all columns (columns are dynamic)
silver_df = exploded_df.select(
    to_date(col("d.CreationTime").substr(1,10), "yyyy-MM-dd").alias("CreationDate"),
    date_format("d.CreationTime","yyyyMMdd").alias("CreationDateKey"),
    date_format("d.CreationTime","H").alias("CreationHour"),
    date_format("d.CreationTime","mm").alias("CreationMinute"),
    col("d.*")
    )
# Put selected ID columns to Upper Case
for co in silver_df.columns:
    if co in ['ActivityId','ArtifactId','CapacityId','DashboardId','DataflowId','DatasetId','DatasourceId','FolderObjectId','GatewayId','Id','ItemId','ReportId','UserId','WorkspaceId','','','','','','','','','',]:
        silver_df = silver_df.withColumn(co, f.upper(silver_df[co]))
del exploded_df

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

# Anonymize new activity logs
if anonymize_tables == True and anonymize_after_days == 0:
    silver_df = silver_df.withColumn('UserId', md5(col("UserId")))
    silver_df = silver_df.withColumn('UserKey', md5(col("UserKey")))
    print("INFO: New Activity logs have been anonymized.")

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

# Check if gold table exists 
table_exists = None
if spark._jsparkSession.catalog().tableExists('FUAM_Lakehouse', gold_table_name):
    table_exists = True
    print("Gold table exists.")
else:
    table_exists = False

# Get latest activity date from silver_df
silver_min_df = silver_df.select(col('CreationDate')).orderBy(col('CreationDate'), ascending=True).first()
silver_last_activity_date = silver_min_df['CreationDate']

# Calculate latest activity date
if table_exists:

    # Get latest activity date from gold table
    get_latest_date_sql = "SELECT CreationDate FROM FUAM_Lakehouse.activities ORDER BY CreationDate DESC LIMIT 1"
    gold_min_df = spark.sql(get_latest_date_sql)
    if gold_min_df.count() == 0:
        # in case there are no records in gold take silver_last_activity_date
        print("No existing records")
        gold_last_activity_date = silver_last_activity_date
    else:
        gold_last_activity_date = gold_min_df.first()['CreationDate']

    if silver_last_activity_date < gold_last_activity_date:
        print("From silver_df")
        last_activity_date = silver_last_activity_date
    else:
        print("From gold")
        last_activity_date = gold_last_activity_date

else:
    print("From silver_df")
    last_activity_date = silver_last_activity_date

print(last_activity_date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean delta content from Gold table
if table_exists:
    del_query = f"DELETE FROM FUAM_Lakehouse.{gold_table_name} WHERE CreationDate >= TO_DATE('{last_activity_date}')"
    spark.sql(del_query)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filter silver_df data based on last activity date
silver_df = silver_df.filter(f.col("CreationDate") >= f.lit(last_activity_date))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if display_data:
    display(silver_df)
    # show converted table schema
    print(convert_columns_to_string(silver_df.schema))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert silver_df's complex data type columns to StringType columns
silver_df_converted = silver_df.selectExpr(convert_columns_to_string(silver_df.schema))
del silver_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write prepared silver_df_converted to gold delta table
silver_df_converted.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable(gold_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Historical data manipulation of 'activities' Lakehouse table
# Deletion, anonymization if configured

# CELL ********************

# FILES - Write history of bronze files
# and delete historical activity logs files if anyonimization is enabled 
if not(anonymize_files):
    path = bronze_file_location.replace("*/", '', )
    mssparkutils.fs.cp(path, path.replace("Files/raw/", "Files/history/") + datetime.datetime.now().strftime('%Y/%m/%d') + "/", True)
    print("Activity logs have been stored on FUAM_Lakehouse/Files/history folder")
else:
    print("Activity logs will not be stored due to anonymization")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# TABLE - ANONYMIZATION AFTER X DAYS

from pyspark.sql.functions import col, md5, current_date, datediff, when
from pyspark.sql.functions import expr

if anonymize_tables == True and anonymize_after_days > 0:

    a_df = spark.read.format("delta").table("activities")

    if anonymize_tables:
        retention_threshold = current_date() - expr(f"INTERVAL {anonymize_after_days} DAYS")
        print(retention_threshold)
        a_df = a_df.withColumn(
            'UserId',
            when(col('CreationDate') < retention_threshold, md5(col('UserId'))).otherwise(col('UserId'))
        ).withColumn(
            'UserKey',
            when(col('CreationDate') < retention_threshold, md5(col('UserKey'))).otherwise(col('UserKey'))
        )
        print("INFO: Activities have been anonymized according to data retention policy.")

        # If you want to overwrite the table:
        a_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("activities")

    if display_data:
        display(a_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# AUDIT TABLE
# Log anonymization to audit table in FUAM Lakehouse

from pyspark.sql import Row 
from datetime import datetime

if anonymize_files:
    df_anonymized_activity_log_audit_entry_2 = spark.createDataFrame([ 
        Row(timestamp=datetime.now(), type="Lakehouse file", affected_object="Files/History/activities/", affected_objects="All historical activities files", reason="Fabric Administrator/FUAM Owner has activated anonymizitation logic.")
        ])  
        
    df_anonymized_activity_log_audit_entry_2.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable("audit_activity_log_anonymization")


if anonymize_tables and anonymize_after_days == 0:
    df_anonymized_activity_log_audit_entry = spark.createDataFrame([ 
        Row(timestamp=datetime.now(), activity_days_in_scope=activity_days_in_scope, type="Lakehouse table", affected_object="Tables/activities", affected_objects="UserKey, UserId columns", reason="Fabric Administrator/FUAM Owner has activated anonymizitation logic for current data.", algorithm="MD5")
        ])  
        
    df_anonymized_activity_log_audit_entry.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable("audit_activity_log_anonymization")

if anonymize_tables and anonymize_after_days > 0:
    df_anonymized_activity_log_audit_entry = spark.createDataFrame([ 
        Row(timestamp=datetime.now(), anonymize_after_days=anonymize_after_days, type="Lakehouse table", affected_object="Tables/activities", affected_objects="UserKey, UserId columns", reason="Fabric Administrator/FUAM Owner has activated anonymizitation logic for historical data.", algorithm="MD5")
        ])  
        
    df_anonymized_activity_log_audit_entry.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable("audit_activity_log_anonymization")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
