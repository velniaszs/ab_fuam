# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f8dd9826-2d71-41c8-bddc-4188573d168e",
# META       "default_lakehouse_name": "FUAM_Backup_Lakehouse",
# META       "default_lakehouse_workspace_id": "7ac9875f-7544-4cd9-ae79-cef02837d23f",
# META       "known_lakehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Lakehouse Backup
# This notebook makes a regular backup of the tables from FUAM_Lakehouse to the FUAM_Lakehouse_Backup 

# CELL ********************

import sempy.fabric as fabric
from datetime import datetime, timedelta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

backup_files = True
keep_historic_days = 7

fuam_workspace_id = '88c8d9fa-2c24-3fad-8f46-b36431c7ba1d'
fuam_lakehouse_id = '6cff634b-88f7-3505-bed2-c03a36776a8b'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Init the client
client = fabric.FabricRestClient()

# Set date helpers
current_time = datetime.now()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fuam_lakehouse_tables_path = f"abfss://{fuam_workspace_id}@onelake.dfs.fabric.microsoft.com/{fuam_lakehouse_id}/Tables"
fuam_lakehouse_files_path = f"abfss://{fuam_workspace_id}@onelake.dfs.fabric.microsoft.com/{fuam_lakehouse_id}/Files"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.fastcp(fuam_lakehouse_tables_path, 'Files/'+ current_time.strftime("%Y/%m/%d") + '/Tables', True)
if backup_files:
   notebookutils.fs.fastcp(fuam_lakehouse_files_path, 'Files/'+ current_time.strftime("%Y/%m/%d") + '/Files', True) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Remove historic backups olders than keep_historic_days


# CELL ********************

subfolders = []
def list_subfolders(path, max_level, date_path):
  for item in mssparkutils.fs.ls(path):
    if max_level > 0:
      list_subfolders(item.path, max_level - 1, date_path + '-' + item.name)
    else:
      fold = {}
      fold["date"] = datetime.strptime((date_path + '-' + item.name)[1:], "%Y-%m-%d")
      fold["path"] = item.path
      subfolders.append(fold)
    

list_subfolders('Files/', max_level= 2, date_path = '')


for dat in subfolders:
  
  time_between_insertion = datetime.now() - dat['date']
  if  time_between_insertion.days > keep_historic_days:
    print(dat['date'])
    print(dat['path'])
    mssparkutils.fs.rm(dat['path'], True) # Set the last parameter as True to remove all files and directories recursively

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Remove empty folders after old backups have been removed

# CELL ********************

def find_empty_dirs(path, max_level):
    directories = notebookutils.fs.ls(path)
    for directory in directories:
        if (directory.size == 0) & (max_level > 0):
            find_empty_dirs(directory.path, max_level - 1)
            contents = notebookutils.fs.ls(directory.path)
            if len(contents) == 0:
                # Logic
                notebookutils.fs.rm(directory.path, recurse=True)
                print(f"Removed empty directory: {directory.path}")

find_empty_dirs('Files/', 4)

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
