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
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# #### Get Capacity Metrics (Timepoints)
# by Capacity by Day
# 
# ##### Data ingestion strategy:
# <mark style="background: #D69AFE;">**MERGE**</mark>
# 
# ##### Related pipeline:
# 
# **Load_Capacity_Metrics_E2E**
# 
# ##### Source:
# 
# **Capacity Metrics** via SemPy DAX execute query function
# 
# ##### Target:
# 
# **1 Delta table** in FUAM_Lakehouse 
# - **gold_table_name** variable value

# CELL ********************

import sempy.fabric as fabric
from datetime import datetime, timedelta
import datetime as dt
import pyspark.sql.functions as f
from delta.tables import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

## Parameters
# These parameters will be overwritten while executing the notebook 
# from Load_FUAM_Data_E2E Pipeline
metric_days_in_scope = 3
metric_workspace = "f41928d0-580d-4178-90ee-9d7cde90e602"
metric_dataset = "34d3843c-ac57-4fd6-8c79-d278be9fcfa0"
display_data = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Variables
silver_table_name = "FUAM_Staging_Lakehouse.capacity_metrics_by_timepoint_silver"
gold_table_name = "capacity_metrics_by_timepoint"
gold_table_name_with_prefix = f"Tables/{gold_table_name}"
count_of_connectivity_errors = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check Table Status
version = ''

try: 
    check_table_structure_query = """EVALUATE ROW("Blocked_workspaces__Day_", 'All Measures'[Blocked workspaces (Day)])"""
    check_table_structure_df = fabric.evaluate_dax(workspace=metric_workspace, dataset=metric_dataset, dax_string=check_table_structure_query)
    print("INFO: Test for v53 successful")
    version = 'v53'
except:
    print("INFO: Test for v53 failed")

if version == '':
    try: 
        check_table_structure_query = """DEFINE    MPARAMETER 'DefaultCapacityID' = "0000000-0000-0000-0000-00000000"
                                        EVALUATE   SUMMARIZECOLUMNS("Background billable CU %", [Background billable CU %]    )"""
        check_table_structure_df = fabric.evaluate_dax(workspace=metric_workspace, dataset=metric_dataset, dax_string=check_table_structure_query)
        print("INFO: Test for v47 successful")
        version = 'v47'
    except:
        print("INFO: Test for v47 failed")


if version == '':
    try:
        check_table_structure_query = """EVALUATE ROW("Background billable CU %", 'All Measures'[Background billable CU %])"""
        check_table_structure_df = fabric.evaluate_dax(workspace=metric_workspace, dataset=metric_dataset, dax_string=check_table_structure_query)
        print("INFO: Test for v40 successful")
        version = 'v40'
    except:
        print("INFO: Test for v40 failed")

if version == '':
    try:
        check_table_structure_query_alternative = """EVALUATE ROW("xBackground__", 'All Measures'[xBackground %])"""
        check_table_structure_df_alternative = fabric.evaluate_dax(workspace=metric_workspace, dataset=metric_dataset, dax_string=check_table_structure_query_alternative)
        version = 'v37'
        print("INFO: Test for v37 successful")
    except:
        print("INFO: Test for v37 failed")


# Validate version compatibility
if version != '':
    print( f'INFO: Version {version} is valid')
else: 
    raise Exception("ERROR: Capacity Metrics data structure is not compatible or connection to capacity metrics is not possible.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fetch capacities from connected capacity metrics app
try:
  if version in ['v53']:
    capacity_query = """EVALUATE SELECTCOLUMNS (    Capacities, "Capacity Id", Capacities[Capacity Id] , "state" , Capacities[state] )"""
  elif version in ['v47', 'v44', 'v40']:
    capacity_query = """EVALUATE SELECTCOLUMNS (    Capacities, "capacity Id", Capacities[capacity Id] , "state" , Capacities[state] )"""
  else:
    capacity_query = """EVALUATE SELECTCOLUMNS (    Capacities, "capacity Id", Capacities[CapacityId] , "state" , Capacities[state] )"""
  capacities = fabric.evaluate_dax(workspace=metric_workspace, dataset=metric_dataset, dax_string=capacity_query)
  capacities.columns = ['CapacityId', 'State']
  capacities = spark.createDataFrame(capacities)
except Exception as e:
  notebookutils.notebook.exit("ERROR: Capacity Metrics data structure is not compatible or connection to capacity metrics is not possible.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if display_data:
    display(capacities)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Iterate days
def iterate_dates(start_date, end_date):
    # Init array
    dates = []
    # Convert string inputs to datetime objects
    start = dt.datetime.strptime(start_date, '%Y-%m-%d')
    end = dt.datetime.strptime(end_date, '%Y-%m-%d')
    
    # Initialize current date as start date
    current_date = start.date()
    
    while current_date <= end.date():

        dates.append(
            {
                "date": current_date,
                "year": current_date.year,
                "month": current_date.month,
                "day": current_date.day
            })
        # Move to the next day
        current_date += dt.timedelta(days=1)

    return dates


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean Silver table
try:
    query = "DELETE FROM " + silver_table_name
    spark.sql(query)

except Exception as ex:
    print("INFO: Silver table doesn't exist yet.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Iterate capacities and days
for cap in capacities.collect():
    capacity_id = cap[0]
    
    print(f"INFO: Scoped CapacityId: {capacity_id}")

    try:
        # Get today's date
        today = datetime.now()

        # Calculate the dates between today and days_in_scope
        days_ago = today - timedelta(days=metric_days_in_scope)

        # Format dates in 'yyyy-mm-dd'
        today_str = today.strftime('%Y-%m-%d')
        days_ago_str = days_ago.strftime('%Y-%m-%d')

        date_array = iterate_dates(days_ago_str, end_date=today_str)
        print(f"INFO: Get data for CapacityId: {capacity_id}")

        # Iterate days for current capacity
        for date in date_array:
            year = date['year']
            month = date['month']
            day = date['day']
            date_label = str(year) + '-' + str(month) + '-' + str(day)


            dax_query_v53 = f"""
            DEFINE
            
             MPARAMETER 'CapacitiesList' = {{ \"{capacity_id}\" }}

            VAR __DS0Core = 
                    SUMMARIZECOLUMNS(
                        Capacities[capacity Id],
                        'TimePoints'[TimePoint],
                        TREATAS({{"{capacity_id}"}}, 'Capacities'[Capacity Id]),
                        TREATAS({{DATE({year}, {month}, {day})}}, 'Dates'[Date]),
                        "B_P", 'All Measures'[Background billable CU %],
                        "I_P", 'All Measures'[Interactive billable CU %],
                        "B_NB_P", 'All Measures'[Background non billable CU %],
                        "I_NB_P", 'All Measures'[Interactive non billable CU %],
                        "AS_P", 'All Measures'[SKU CU by timepoint %],
                        "CU_L", 'All Measures'[CU limit],
                        "T_CU_U_P", 'All Measures'[Cumulative CU usage % preview],
                        "C_CU_U_S", 'All Measures'[Cumulative CU usage (s)],
                        "SKU_CU_TP", 'All Measures'[SKU CU by timepoint],
                        "I_Del_P", 'All Measures'[Dynamic interactive delay %],
                        "I_Rej_P", 'All Measures'[Dynamic interactive rejection %],
                        "I_Rej_TH", 'All Measures'[Interactive rejection threshold],
                        "B_Rej_P", 'All Measures'[Dynamic background rejection %],
                        "B_Rej_TH", 'All Measures'[Background rejection threshold],
                        "CO_A_P", 'All Measures'[Carry over add %],
                        "CO_BD_P", 'All Measures'[Carry over burndown %],
                        "CO_C_P", 'All Measures'[Cumulative carry over %],
                        "OV_RL", 'All Measures'[Overage reference line],
                        "Exp_BD_M", 'All Measures'[Expected burndown in minutes]
                    )

            EVALUATE
                __DS0Core
            """

            dax_query_v47 = f"""
            DEFINE
            
             MPARAMETER 'CapacitiesList' = {{ \"{capacity_id}\" }}

            VAR __DS0Core = 
                    SUMMARIZECOLUMNS(
                        Capacities[capacity Id],
                        'TimePoints'[TimePoint],
                        FILTER(Capacities, Capacities[capacity Id] = \"{capacity_id}\" ),
                        FILTER(TimePoints,  'TimePoints'[Date] = DATE({year}, {month}, {day})),
                        "B_P", 'All Measures'[Background billable CU %],
                        "I_P", 'All Measures'[Interactive billable CU %],
                        "B_NB_P", 'All Measures'[Background non billable CU %],
                        "I_NB_P", 'All Measures'[Interactive non billable CU %],
                        "AS_P", 'All Measures'[SKU CU by timepoint %],
                        "CU_L", 'All Measures'[CU limit],
                        "T_CU_U_P", 'All Measures'[Cumulative CU usage % preview],
                        "C_CU_U_S", 'All Measures'[Cumulative CU usage (s)],
                        "SKU_CU_TP", 'All Measures'[SKU CU by timepoint],
                        "I_Del_P", 'All Measures'[Dynamic interactive delay %],
                        "I_Rej_P", 'All Measures'[Dynamic interactive rejection %],
                        "I_Rej_TH", 'All Measures'[Interactive rejection threshold],
                        "B_Rej_P", 'All Measures'[Dynamic background rejection %],
                        "B_Rej_TH", 'All Measures'[Background rejection threshold],
                        "CO_A_P", 'All Measures'[Carry over add %],
                        "CO_BD_P", 'All Measures'[Carry over burndown %],
                        "CO_C_P", 'All Measures'[Cumulative carry over %],
                        "OV_RL", 'All Measures'[Overage reference line],
                        "Exp_BD_M", 'All Measures'[Expected burndown in minutes]
                    )

            EVALUATE
                __DS0Core
            """
            
            dax_query_v44 = f"""
            DEFINE
            
             MPARAMETER 'CapacitiesList' = {{ \"{capacity_id}\" }}

            VAR __DS0Core = 
                    SUMMARIZECOLUMNS(
                        Capacities[capacity Id],
                        'TimePoints'[TimePoint],
                        FILTER(Capacities, Capacities[capacity Id] = \"{capacity_id}\" ),
                        FILTER(TimePoints,  'TimePoints'[Date] = DATE({year}, {month}, {day})),
                        "B_P", 'All Measures'[Background billable CU %],
                        "I_P", 'All Measures'[Interactive billable CU %],
                        "B_NB_P", 'All Measures'[Background non billable CU %],
                        "I_NB_P", 'All Measures'[Interactive non billable CU %],
                        "AS_P", 'All Measures'[SKU CU by timepoint %],
                        "CU_L", 'All Measures'[CU limit],
                        "T_CU_U_P", 'All Measures'[Cumulative CU usage % preview],
                        "C_CU_U_S", 'All Measures'[Cumulative CU usage (s)],
                        "SKU_CU_TP", 'All Measures'[SKU CU by timepoint],
                        "I_Del_P", 'All Measures'[Dynamic interactive delay %],
                        "I_Rej_P", 'All Measures'[Dynamic interactive rejection %],
                        "I_Rej_TH", 'All Measures'[Interactive rejection threshold],
                        "B_Rej_P", 'All Measures'[Dynamic background rejection %],
                        "B_Rej_TH", 'All Measures'[Background rejection threshold],
                        "CO_A_P", 'All Measures'[Carry over add %],
                        "CO_BD_P", 'All Measures'[Carry over burndown %],
                        "CO_C_P", 'All Measures'[Cumulative carry over %],
                        "OV_RL", 'All Measures'[Overage reference line],
                        "Exp_BD_M", 'All Measures'[Expected burndown in minutes]
                    )

            EVALUATE
                __DS0Core
            """


            dax_query_v40 = f"""
            DEFINE
            
            MPARAMETER 'CapacityID' = \"{capacity_id}\"

            VAR __DS0Core = 
                    SUMMARIZECOLUMNS(
                        Capacities[capacity Id],
                        'TimePoints'[TimePoint],
                        FILTER(Capacities, Capacities[capacity Id] = \"{capacity_id}\" ),
                        FILTER(TimePoints,  'TimePoints'[Date] = DATE({year}, {month}, {day})),
                        "B_P", 'All Measures'[Background billable CU %],
                        "I_P", 'All Measures'[Interactive billable CU %],
                        "B_NB_P", 'All Measures'[Background non billable CU %],
                        "I_NB_P", 'All Measures'[Interactive non billable CU %],
                        "AS_P", 'All Measures'[SKU CU by timepoint %],
                        "CU_L", 'All Measures'[CU limit],
                        "T_CU_U_P", 'All Measures'[Cumulative CU usage % preview],
                        "C_CU_U_S", 'All Measures'[Cumulative CU usage (s)],
                        "SKU_CU_TP", 'All Measures'[SKU CU by timepoint],
                        "I_Del_P", 'All Measures'[Dynamic interactive delay %],
                        "I_Rej_P", 'All Measures'[Dynamic interactive rejection %],
                        "I_Rej_TH", 'All Measures'[Interactive rejection threshold],
                        "B_Rej_P", 'All Measures'[Dynamic background rejection %],
                        "B_Rej_TH", 'All Measures'[Background rejection threshold],
                        "CO_A_P", 'All Measures'[Carry over add %],
                        "CO_BD_P", 'All Measures'[Carry over burndown %],
                        "CO_C_P", 'All Measures'[Cumulative carry over %],
                        "OV_RL", 'All Measures'[Overage reference line],
                        "Exp_BD_M", 'All Measures'[Expected burndown in minutes]
                    )

            EVALUATE
                __DS0Core
            """

            dax_query_v37 = f"""
            DEFINE

            MPARAMETER 'CapacityID' = \"{capacity_id}\"

            VAR __DS0Core = 
                    SUMMARIZECOLUMNS(
                        Capacities[capacityId],
                        'TimePoints'[TimePoint],
                        FILTER(Capacities, Capacities[capacityId] = \"{capacity_id}\" ),
                        FILTER(TimePoints,  'TimePoints'[Date] = DATE({year}, {month}, {day})),
                        "B_P", 'All Measures'[xBackground %],
                        "I_P", 'All Measures'[xInteractive %],
                        "B_NB_P", 'All Measures'[xBackground % Preview],
                        "I_NB_P", 'All Measures'[xInteractive % Preview],
                        "AS_P", 'All Measures'[SKU CU by TimePoint %],
                        "CU_L", 'All Measures'[CU Limit],
                        "T_CU_U_P", 'All Measures'[Cumulative CU Usage % Preview],
                        "C_CU_U_S", 'All Measures'[Cumulative CU Usage (s)],
                        "SKU_CU_TP", 'All Measures'[SKU CU by TimePoint],
                        "I_Del_P", 'All Measures'[Dynamic InteractiveDelay %],
                        "I_Rej_P", 'All Measures'[Dynamic InteractiveRejection %],
                        "I_Rej_TH", 'All Measures'[Interactive rejection threshold],
                        "B_Rej_P", 'All Measures'[Dynamic BackgroundRejection %],
                        "B_Rej_TH", 'All Measures'[Background rejection threshold],
                        "CO_A_P", 'All Measures'[xCarryOver_added %],
                        "CO_BD_P", 'All Measures'[xCarryOver_burndown %],
                        "CO_C_P", 'All Measures'[xCarryOver_Cumulative %],
                        "OV_RL", 'All Measures'[Overage reference line],
                        "Exp_BD_M", 'All Measures'[Expected burndown in minutes]
                    )
            EVALUATE
                __DS0Core
            """

            dax_query = ""
            # Choose query
            if version == 'v53':
                print("INFO: v53 selected")
                dax_query = dax_query_v53
            elif version == 'v47':
                print("INFO: v47 selected")
                dax_query = dax_query_v47
            elif version == 'v44':
                print("INFO: v44 selected")
                dax_query = dax_query_v44
            elif version == 'v40':
                print("INFO: v40 selected")
                dax_query = dax_query_v40
            elif version == 'v37':
                print("INFO: v37 selected")
                dax_query = dax_query_v37
                
            # Execute DAX query
            capacity_df = fabric.evaluate_dax(workspace=metric_workspace, dataset=metric_dataset, dax_string=dax_query)
            capacity_df.columns = ['CapacityId', 'TimePoint', 'BackgroundPercentage', 'InteractivePercentage', 
                                    'BackgroundNonBillablePercentage', 'InteractiveNonBillablePercentage', 'AutoscalePercentage', 
                                    'CULimitPercentage', 'TotalCUUsagePercentage', 'TotalCUs', 'SKUCUByTimePoint', 
                                    'InteractiveDelayPercentage', 'InteractiveRejectionPercentage', 'InteractiveRejectionThreshold', 
                                    'BackgroundRejectionPercentage', 'BackgroundRejectionThreshold', 'CarryOverAddedPercentage', 
                                    'CarryOverBurndownPercentage', 'CarryOverCumulativePercentage', 'OverageReferenceLine', 
                                    'ExpectedBurndownInMin']
            
            if not(capacity_df.empty):
                # Transfer pandas df to spark df
                capacity_df = spark.createDataFrame(capacity_df)

                if display_data:
                    display(capacity_df)

                # Write prepared bronze_df to silver delta table
                print(f"INFO: Appending data for CapacityId: {capacity_id} on Date: {date_label}")
                capacity_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable(silver_table_name)  
            else:
                print(f"INFO: No data for CapacityId: {capacity_id} on Date: {date_label}")

    except Exception as ex:
        count_of_connectivity_errors += 1
        print(ex)
        continue

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Handle error if interface is not reachable
if count_of_connectivity_errors > 0: 
    print("WARNING: Connection to at least one of the capacities is not possible. Please review the configurations.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    # Get Silver table data
    query = "SELECT * FROM  " + silver_table_name
    silver_df = spark.sql(query)
    silver_df = silver_df.withColumn('CapacityId', f.upper(f.col("CapacityId")))

except Exception as ex:
    # Handle error if interface is not reachable
    if count_of_connectivity_errors > 0: 
        notebookutils.notebook.exit("ERROR: Connection to capacity metrics is not possible. Please review the configurations.")
    else:
        notebookutils.notebook.exit("ERROR: Silver table doesn't exist yet. This issue occurs also when the connection to capacity metrics is not configured correctly or endpoint is not reachable.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if gold table exists
if spark._jsparkSession.catalog().tableExists('FUAM_Lakehouse', gold_table_name):
    # if exists -> MERGE to gold
    print("INFO: Gold table exists and will be merged.")

    gold_df = DeltaTable.forPath(spark, gold_table_name_with_prefix)
    # Merge silver (s = source) to gold (t = target)
    gold_df.alias('t') \
    .merge(
        silver_df.alias('s'),
        "s.CapacityId = t.CapacityId AND s.TimePoint = t.TimePoint"
    ) \
    .whenMatchedUpdate(set =
        {
             "BackgroundPercentage": "s.BackgroundPercentage"
            ,"InteractivePercentage": "s.InteractivePercentage"
            ,"BackgroundNonBillablePercentage": "s.BackgroundNonBillablePercentage"
            ,"InteractiveNonBillablePercentage": "s.InteractiveNonBillablePercentage"
            ,"AutoscalePercentage": "s.AutoscalePercentage"
            ,"CULimitPercentage": "s.CULimitPercentage"
            ,"TotalCUUsagePercentage": "s.TotalCUUsagePercentage"
            ,"TotalCUs": "s.TotalCUs"
            ,"SKUCUByTimePoint": "s.SKUCUByTimePoint"
            ,"InteractiveDelayPercentage": "s.InteractiveDelayPercentage"
            ,"InteractiveRejectionPercentage": "s.InteractiveRejectionPercentage"
            ,"InteractiveRejectionThreshold": "s.InteractiveRejectionThreshold"
            ,"BackgroundRejectionPercentage": "s.BackgroundRejectionPercentage"
            ,"BackgroundRejectionThreshold": "s.BackgroundRejectionThreshold"
            ,"CarryOverAddedPercentage": "s.CarryOverAddedPercentage"
            ,"CarryOverBurndownPercentage": "s.CarryOverBurndownPercentage"
            ,"CarryOverCumulativePercentage": "s.CarryOverCumulativePercentage"
            ,"OverageReferenceLine": "s.OverageReferenceLine"
            ,"ExpectedBurndownInMin": "s.ExpectedBurndownInMin"
        }
    ) \
    .whenNotMatchedInsert(values =
        {
             "CapacityId": "s.CapacityId"
            ,"TimePoint": "s.TimePoint"
            ,"BackgroundPercentage": "s.BackgroundPercentage"
            ,"InteractivePercentage": "s.InteractivePercentage"
            ,"BackgroundNonBillablePercentage": "s.BackgroundNonBillablePercentage"
            ,"InteractiveNonBillablePercentage": "s.InteractiveNonBillablePercentage"
            ,"AutoscalePercentage": "s.AutoscalePercentage"
            ,"CULimitPercentage": "s.CULimitPercentage"
            ,"TotalCUUsagePercentage": "s.TotalCUUsagePercentage"
            ,"TotalCUs": "s.TotalCUs"
            ,"SKUCUByTimePoint": "s.SKUCUByTimePoint"
            ,"InteractiveDelayPercentage": "s.InteractiveDelayPercentage"
            ,"InteractiveRejectionPercentage": "s.InteractiveRejectionPercentage"
            ,"InteractiveRejectionThreshold": "s.InteractiveRejectionThreshold"
            ,"BackgroundRejectionPercentage": "s.BackgroundRejectionPercentage"
            ,"BackgroundRejectionThreshold": "s.BackgroundRejectionThreshold"
            ,"CarryOverAddedPercentage": "s.CarryOverAddedPercentage"
            ,"CarryOverBurndownPercentage": "s.CarryOverBurndownPercentage"
            ,"CarryOverCumulativePercentage": "s.CarryOverCumulativePercentage"
            ,"OverageReferenceLine": "s.OverageReferenceLine"
            ,"ExpectedBurndownInMin": "s.ExpectedBurndownInMin"
        }
    ) \
    .execute()

else:
    # else -> INSERT to gold
    print("INFO: Gold table will be created.")

    silver_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable(gold_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean Silver table
query = "DELETE FROM " + silver_table_name
spark.sql(query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Aggregate timepoints
df = spark.sql("SELECT DISTINCT TimePoint, to_date(TimePoint) Date, to_timestamp(date_format(TimePoint, 'yyyy-mm-dd HH:00:00')) Hour  FROM FUAM_Lakehouse.capacity_metrics_by_timepoint")

# Write prepared bronze_df to silver delta table
df.write.mode("overwrite").format("delta").saveAsTable("calendar_timepoints")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
