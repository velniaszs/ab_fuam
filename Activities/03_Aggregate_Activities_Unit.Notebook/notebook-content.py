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

# #### Activities (aggregated last 30 days)
# 
# ##### Data ingestion strategy:
# <mark style="background: #88D5FF;">**REPLACE**</mark>
# 
# ##### Related pipeline:
# 
# **Load_Activities_E2E**
# 
# ##### Source:
# 
# **1 Delta table**  from FUAM_Lakehouse
# - **activities**
# 
# ##### Target:
# 
# **1 Delta table** in FUAM_Lakehouse 
# - **aggr_table_name** variable value

# PARAMETERS CELL ********************

## Parameters
display_data = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Variables
aggr_table_name = "aggregated_activities_last_30days"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get aggregated Gold data
query = """
SELECT 
    CreationDate,
    CreationDateKey,
    CreationHour,
    Activity,
    Experience,
    Workload,
    UPPER(CapacityId) as CapacityId,
    UPPER(WorkspaceId) as WorkspaceId,
    GatewayId,
    COALESCE(UPPER(ArtifactId), UPPER(ObjectId)) AS fuam_item_id,
    COALESCE(ObjectType, ArtifactKind) AS fuam_item_kind,
    CASE 
        WHEN ResultStatus = 'Failed' THEN 'Failed' 
        WHEN ResultStatus = 'InProgress' THEN 'InProgress'
        WHEN ResultStatus = 'Succeeded' THEN 'Succeeded'
        WHEN IsSuccess = 1 THEN 'Succeeded'
        ELSE 'N/A'
    END AS fuam_activity_status,
    COUNT(*) AS CountOfActivities,
    COUNT(DISTINCT UserId) AS DistinctCountOfUsers
FROM FUAM_Lakehouse.activities
WHERE CreationDate >= date_sub(CAST(current_timestamp() as DATE), 30)
GROUP BY
    CreationDate,
    CreationDateKey,
    CreationHour,
    Activity,
    Experience,
    Workload,
    CapacityId,
    WorkspaceId,
    GatewayId,
    ArtifactId,
    ItemId,
    ObjectId,
    ObjectType,
    ArtifactKind,
    ResultStatus,
    IsSuccess
ORDER BY CreationDate DESC
"""

# Query data
agg_df = spark.sql(query) 



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if display_data:
    display(agg_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Overwrite aggregated table
agg_df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable(aggr_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
