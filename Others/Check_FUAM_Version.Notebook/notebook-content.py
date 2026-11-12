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
# META       "known_lakehouses": [
# META         {
# META           "id": "7d41449a-e630-46c5-bbfe-e869e7344639"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Check FUAM Version
# 
# This notebook checks from Fabric-Toolbox repository the latest version of FUAM.
# 
# The FUAM_Core_Report will show you, if your current installed FUAM version should be updated.

# CELL ********************

# Parameters
display_data = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import lit
import requests
import json

# Step 1: Fetch the JSON from the public URL
url = "https://raw.githubusercontent.com/microsoft/fabric-toolbox/refs/heads/main/monitoring/fabric-unified-admin-monitoring/data/current_fuam_version.json"
response = requests.get(url)
data = response.json()

# Step 2: Convert the JSON to a Spark DataFrame
df = spark.createDataFrame([data])

# Optional: Add a timestamp column for tracking
from pyspark.sql.functions import current_timestamp
df = df.withColumn("last_check_timestamp", current_timestamp())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if display_data:
    display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 3: Write the DataFrame to a Delta table in the Lakehouse
df.write.format("delta").mode("overwrite").saveAsTable("audit_latest_available_fuam_version")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
