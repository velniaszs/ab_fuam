# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Welcome to FUAM Deployment
# 
# This notebook deployes the latest FUAM version in the specified workspace. It works for initial deployment and for the upgrade process of FUAM.
# 
# **End-to-end documenation on fabric-toolbox:**
# 
# [Visit - How to deploy and configure FUAM](https://github.com/microsoft/fabric-toolbox/blob/main/monitoring/fabric-unified-admin-monitoring/how-to/How_to_deploy_FUAM.md)
# 
# **What is happening in this notebook?**
#  - The notebook checks the two cloud connections for FUAM (if initial deployment, connections will be created, otherwise check only)
#  - It downloads the latest FUAM src files from Github
#  - It deploys/updates the Fabric items in the current workspace
#  - It creates all needed tables automatically, so reports work also with some data missing
# 
# **Next steps**
# - (Optional) Change connection names, only if needed
# - Run this notebook
# 
# If you **deploy** FUAM in this workspace at the **first time**:
# - Navigate to the cloud connections
# - Search under cloud connection for **fuam fabric-service-api admin** and for **fuam pbi-service-api admin** 
# - Add the credentials of your service principal to these connections
# 
# If you **update** your existing FUAM workspace:
# - After the notebooks has been executed, you are **done**


# CELL ********************

%pip install ms-fabric-cli

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

pbi_connection_name = 'fuam pbi-service-api admin'
fabric_connection_name = 'fuam fabric-service-api admin'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Import of needed libaries

# CELL ********************

import subprocess
import os
import json
from zipfile import ZipFile 
import shutil
import re
import requests
import zipfile
from io import BytesIO
import yaml
import sempy.fabric as fabric
import tempfile
from deltalake import write_deltalake
import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Download of source & config files
# This part downloads all source and config files of FUAM needed for the deployment into the ressources of the notebook

# PARAMETERS CELL ********************

def download_folder_as_zip(repo_owner, repo_name, output_zip, branch="main", folder_to_extract="src",  remove_folder_prefix = ""):
    # Construct the URL for the GitHub API to download the repository as a zip file
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/zipball/{branch}"
    
    # Make a request to the GitHub API
    response = requests.get(url)
    response.raise_for_status()
    
    # Ensure the directory for the output zip file exists
    os.makedirs(os.path.dirname(output_zip), exist_ok=True)
    
    # Create a zip file in memory
    with zipfile.ZipFile(BytesIO(response.content)) as zipf:
        with zipfile.ZipFile(output_zip, 'w') as output_zipf:
            for file_info in zipf.infolist():
                parts = file_info.filename.split('/')
                if  re.sub(r'^.*?/', '/', file_info.filename).startswith(folder_to_extract): 
                    # Extract only the specified folder
                    file_data = zipf.read(file_info.filename)
                    output_zipf.writestr(('/'.join(parts[1:]).replace(remove_folder_prefix, "")), file_data)

def uncompress_zip_to_folder(zip_path, extract_to):
    # Ensure the directory for extraction exists
    os.makedirs(extract_to, exist_ok=True)
    
    # Uncompress all files from the zip into the specified folder
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    
    # Delete the original zip file
    os.remove(zip_path)

repo_owner = "Microsoft"
repo_name = "fabric-toolbox"
branch = "main"
folder_prefix = "monitoring/fabric-unified-admin-monitoring"

download_folder_as_zip(repo_owner, repo_name, output_zip = "./builtin/src/src.zip", branch = branch, folder_to_extract= f"/{folder_prefix}/src", remove_folder_prefix = f"{folder_prefix}/")
download_folder_as_zip(repo_owner, repo_name, output_zip = "./builtin/config/config.zip", branch = branch, folder_to_extract= f"/{folder_prefix}/config" , remove_folder_prefix = folder_prefix)
download_folder_as_zip(repo_owner, repo_name, output_zip = "./builtin/data/data.zip", branch = branch, folder_to_extract= f"/{folder_prefix}/data" , remove_folder_prefix = folder_prefix)
uncompress_zip_to_folder(zip_path = "./builtin/config/config.zip", extract_to= "./builtin")
uncompress_zip_to_folder(zip_path = "./builtin/data/data.zip", extract_to= "./builtin")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

base_path = './builtin/'
config_path = os.path.join(base_path, 'config/deployment_config.yaml')

with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

deploy_order_path = os.path.join(base_path, 'config/deployment_order.json')
with open(deploy_order_path, 'r') as file:
        deployment_order = json.load(file)

src_workspace_name = config['workspace']
src_pbi_connection = config['connections']['pbi_connection']
src_fabric_connection = config['connections']['fabric_connection']

semantic_model_connect_to_lakehouse = config['fuam_lakehouse_semantic_models']

mapping_table=[]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Definition of deployment functions

# CELL ********************

# Set environment parameters for Fabric CLI
token = notebookutils.credentials.getToken('pbi')
os.environ['FAB_TOKEN'] = token
os.environ['FAB_TOKEN_ONELAKE'] = token

def run_fab_command(command, capture_output: bool = False, silently_continue: bool = False, timeout: int = 300):
    """Run fabric CLI command with timeout protection (default 5 minutes)."""
    try:
        result = subprocess.run(
            ["fab", "-c", command], 
            capture_output=capture_output, 
            text=True, 
            timeout=timeout
        )
        if (not(silently_continue) and (result.returncode > 0 or result.stderr)):
            raise Exception(f"Error running fab command. exit_code: '{result.returncode}'; stderr: '{result.stderr}'")    
        if (capture_output): 
            output = result.stdout.strip()
            return output
    except subprocess.TimeoutExpired:
        raise Exception(f"Command timed out after {timeout} seconds: {command}")

def fab_get_id(name):
    id = run_fab_command(f"get /{trg_workspace_name}.Workspace/{name} -q id" , capture_output = True, silently_continue= True)
    return(id)

def get_id_by_name(name):
    for it in deployment_order:
        if it.get("name") == name:
                return it.get("fuam_id")
    return None


def copy_to_tmp(name):
    """Extract files from zip to memory (handles nested folders at any depth)."""
    path2zip = "./builtin/src/src.zip"
    file_contents = {}  # Store file paths and their content in memory
    
    with ZipFile(path2zip) as archive:
        for file in archive.namelist():
            # Skip directory entries (ending with /) but include all files at any nesting level
            # This handles: src/name/file.txt, src/name/subfolder/file.txt, src/name/a/b/c/file.txt, etc.
            if file.startswith(f'src/{name}/') and not file.endswith('/'):
                # Read file content into memory instead of extracting to disk
                file_contents[file] = archive.read(file)
    
    return file_contents


def replace_ids_in_memory(file_contents, mapping_table):
    """Replace IDs in memory-stored files."""
    updated_contents = {}
    
    for file_path, content_bytes in file_contents.items():
        file_name = os.path.basename(file_path)
        
        # Decode bytes to string
        try:
            content = content_bytes.decode('utf-8')
        except:
            # If decoding fails, keep as binary
            updated_contents[file_path] = content_bytes
            continue
        
        if file_name.endswith('.ipynb'):
            notebook_json = json.loads(content)
            dependencies = notebook_json.get('metadata', {}).get('dependencies', {})
            depend = json.dumps(dependencies)
            for mapping in mapping_table:  
                depend = depend.replace(mapping["old_id"], mapping["new_id"])
            notebook_json['metadata']['dependencies'] = json.loads(depend)
            content = json.dumps(notebook_json)
            
        elif file_name.endswith(('.py', '.json', '.pbir', '.platform', '.tmdl')) and not file_name.endswith('report.json'):
            for mapping in mapping_table:  
                content = content.replace(mapping["old_id"], mapping["new_id"])
        
        updated_contents[file_path] = content.encode('utf-8')
    
    return updated_contents

def write_memory_to_temp(file_contents, temp_dir):
    """Write in-memory files to temporary directory (system temp, not builtin storage)."""
    for file_path, content_bytes in file_contents.items():
        full_path = os.path.join(temp_dir, file_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, 'wb') as f:
            f.write(content_bytes)
    return temp_dir

def get_semantic_model_id_from_memory(file_contents, name):
    """Get semantic model ID from in-memory report definition."""
    definition_path = f'src/{name}/definition.pbir'
    if definition_path in file_contents:
        content = json.loads(file_contents[definition_path].decode('utf-8'))
        semantic_model_id = content.get('datasetReference', {}).get('byConnection', {}).get('pbiModelDatabaseName')
        if semantic_model_id:
            return semantic_model_id
    return None

def get_semantic_model_id(report_folder):
    definition_file = os.path.join(report_folder, 'definition.pbir')
    if os.path.exists(definition_file):
        with open(definition_file, 'r', encoding='utf-8') as file:
            content = json.load(file)
            semantic_model_id = content.get('datasetReference', {}).get('byConnection', {}).get('pbiModelDatabaseName')
            if semantic_model_id:
                return semantic_model_id
    return None

def update_sm_connection_to_fuam_lakehouse_in_memory(file_contents, name):
    """Update semantic model connection to FUAM lakehouse in memory."""
    new_sm_db = run_fab_command(f"get /{trg_workspace_name}.Workspace/FUAM_Lakehouse.Lakehouse -q properties.sqlEndpointProperties.connectionString", capture_output=True, silently_continue=True)
    new_lakehouse_sql_id = run_fab_command(f"get /{trg_workspace_name}.Workspace/FUAM_Lakehouse.Lakehouse -q properties.sqlEndpointProperties.id", capture_output=True, silently_continue=True)
    
    expressions_path = f'src/{name}/definition/expressions.tmdl'
    if expressions_path in file_contents:
        content = file_contents[expressions_path].decode('utf-8')
        match = re.search(r'Sql\.Database\("([^"]+)",\s*"([^"]+)"\)', content)
        if match:
            old_sm_db, old_lakehouse_sql_id = match.group(1), match.group(2)
            content = content.replace(old_sm_db, new_sm_db).replace(old_lakehouse_sql_id, new_lakehouse_sql_id)
            file_contents[expressions_path] = content.encode('utf-8')
    return file_contents

def update_sm_connection_to_fuam_lakehouse(semantic_model_folder):
    new_sm_db= run_fab_command(f"get /{trg_workspace_name}.Workspace/FUAM_Lakehouse.Lakehouse -q properties.sqlEndpointProperties.connectionString", capture_output = True, silently_continue=True)
    new_lakehouse_sql_id= run_fab_command(f"get /{trg_workspace_name}.Workspace/FUAM_Lakehouse.Lakehouse -q properties.sqlEndpointProperties.id", capture_output = True, silently_continue=True)
        
    expressions_file = os.path.join(semantic_model_folder, 'definition', 'expressions.tmdl')
    if os.path.exists(expressions_file):
        with open(expressions_file, 'r', encoding='utf-8') as file:
            content = file.read()
            match = re.search(r'Sql\.Database\("([^"]+)",\s*"([^"]+)"\)', content)
            if match:
                old_sm_db, old_lakehouse_sql_id = match.group(1), match.group(2)
                content = content.replace(old_sm_db, new_sm_db).replace(old_lakehouse_sql_id, new_lakehouse_sql_id)
                with open(expressions_file, 'w', encoding='utf-8') as file:
                    file.write(content)


def update_report_definition_in_memory(file_contents, name):
    """Update report definition in memory to reference semantic model by ID."""
    semantic_model_id = get_semantic_model_id_from_memory(file_contents, name)
    definition_path = f"src/{name}/definition.pbir"
    
    if definition_path in file_contents and semantic_model_id:
        report_definition = json.loads(file_contents[definition_path].decode('utf-8'))
        
        # Update connection string to reference the semantic model by ID
        # Format: Data Source=powerbi://api.powerbi.com/v1.0/myorg/{workspace};initial catalog={model};semanticmodelid={id}
        connection_string = f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/{trg_workspace_name};initial catalog={{MODEL_NAME}};integrated security=ClaimsToken;semanticmodelid={semantic_model_id}"
        
        # Ensure datasetReference structure exists
        if "datasetReference" not in report_definition:
            report_definition["datasetReference"] = {}
        
        # Clear byPath if it exists
        if "byPath" in report_definition["datasetReference"]:
            del report_definition["datasetReference"]["byPath"]
        
        # Set byConnection with only the connectionString property
        report_definition["datasetReference"]["byConnection"] = {
            "connectionString": connection_string
        }
        
        file_contents[definition_path] = json.dumps(report_definition, indent=4).encode('utf-8')
    
    return file_contents

def update_report_definition(path): 
    """Update report definition to reference semantic model by ID (legacy disk-based version)."""
    semantic_model_id = get_semantic_model_id(path)
    definition_path = os.path.join(path, "definition.pbir")
   
    if semantic_model_id:
        with open(definition_path, "r", encoding="utf8") as file:
            report_definition = json.load(file)

        # Update connection string to reference the semantic model by ID
        connection_string = f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/{trg_workspace_name};initial catalog={{MODEL_NAME}};integrated security=ClaimsToken;semanticmodelid={semantic_model_id}"
        
        # Ensure datasetReference structure exists
        if "datasetReference" not in report_definition:
            report_definition["datasetReference"] = {}
        
        # Clear byPath if it exists
        if "byPath" in report_definition["datasetReference"]:
            del report_definition["datasetReference"]["byPath"]
        
        # Set byConnection with only the connectionString property
        report_definition["datasetReference"]["byConnection"] = {
            "connectionString": connection_string
        }

        with open(definition_path, "w") as file:
            json.dump(report_definition, file, indent=4)

def print_color(text, state):
    red  = '\033[91m'
    yellow = '\033[93m'  
    green = '\033[92m'   
    white = '\033[0m'  
    if state == "error":
        print(red, text, white)
    elif state == "warning":
        print(yellow, text, white)
    elif state == "success":
        print(green, text, white)
    else:
        print("", text)

 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Creation of connections

# CELL ********************

def create_or_get_connection(name, baseUrl, audience):
    try:
        run_fab_command(f"""create .connections/{name}.connection 
            -P connectionDetails.type=WebForPipeline,connectionDetails.creationMethod=WebForPipeline.Contents,connectionDetails.parameters.baseUrl={baseUrl},connectionDetails.parameters.audience={audience},credentialDetails.type=Anonymous""")
        print_color("New connection created. Enter service principal credentials", "success")
    except Exception as ex:
        print_color("Connection already exists", "warning")

    conn_id = run_fab_command(f"get .connections/{name}.Connection -q id", silently_continue= True, capture_output= True)
    print("Connection ID:" + conn_id)
    
    
    return(conn_id)
    
conn_pbi_service_api_admin = create_or_get_connection(pbi_connection_name, "https://api.powerbi.com/v1.0/myorg/admin", "https://analysis.windows.net/powerbi/api" )
conn_fabric_service_api_admin = create_or_get_connection(fabric_connection_name, "https://api.fabric.microsoft.com/v1/admin", "	https://api.fabric.microsoft.com" )

mapping_table.append({ "old_id": get_id_by_name(src_pbi_connection), "new_id": conn_pbi_service_api_admin })
mapping_table.append({ "old_id": get_id_by_name(src_fabric_connection), "new_id": conn_fabric_service_api_admin })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Get current Workspace
# This cell gets the current workspace to deploy FUAM automatically inside it

# CELL ********************

trg_workspace_id = fabric.get_notebook_workspace_id()
res = run_fab_command(f"api -X get workspaces/{trg_workspace_id}" , capture_output = True, silently_continue=True)
trg_workspace_name = json.loads(res)["text"]["displayName"]

print(f"Current workspace: {trg_workspace_name}")
print(f"Current workspace ID: {trg_workspace_id}")


mapping_table.append({ "old_id": get_id_by_name(src_workspace_name + ".Workspace"), "new_id": trg_workspace_id })
mapping_table.append({ "old_id": "00000000-0000-0000-0000-000000000000", "new_id": trg_workspace_id })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

mapping_table

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Deployment Logic
# This part iterates through all the items, gets the respective source code, replaces all IDs dynamically and deploys the new item

# CELL ********************

exclude = [src_workspace_name + ".Workspace", src_pbi_connection, src_fabric_connection]

for it in deployment_order:

    new_id = None
    
    name = it["name"]
    
    if name in exclude:
            continue

    print("")
    print("#############################################")
    print(f"Deploying {name}")

    # Copy to memory and replace IDs in-memory
    file_contents = copy_to_tmp(name)
    file_contents = replace_ids_in_memory(file_contents, mapping_table)

    cli_parameter = ''
    if "Notebook" in name:
        cli_parameter = cli_parameter + " --format .ipynb"
    elif "Lakehouse" in name:
        run_fab_command(f"create /{trg_workspace_name}.Workspace/{name}  -P enableSchemas=False" , silently_continue=True )
        new_id = fab_get_id(name)
        mapping_table.append({ "old_id": get_id_by_name(name), "new_id": new_id })
        
        continue
    elif "Report" in name:
        file_contents = update_report_definition_in_memory(file_contents, name)
    elif name in semantic_model_connect_to_lakehouse:
        file_contents = update_sm_connection_to_fuam_lakehouse_in_memory(file_contents, name)
    
    # Use system temp directory (often RAM-based) instead of builtin storage
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            write_memory_to_temp(file_contents, temp_dir)
            item_path = os.path.join(temp_dir, f"src/{name}")
            
            print(f"Importing {name}...")
            run_fab_command(f"import  /{trg_workspace_name}.Workspace/{name} -i {item_path} -f {cli_parameter} ", silently_continue= True, timeout=600)
            new_id= fab_get_id(name)
            mapping_table.append({ "old_id": it["fuam_id"], "new_id": new_id })
            print_color(f"✓ Successfully deployed {name}", "success")
        except subprocess.TimeoutExpired as e:
            print_color(f"✗ Timeout importing {name}: {str(e)}", "error")
            raise
        except Exception as e:
            print_color(f"✗ Error deploying {name}: {str(e)}", "error")
            raise
        # temp_dir automatically cleaned up when context exits


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Move items into folders
# The items will be moved into the respective folders. Definition is done in the deployment_config.yml

# CELL ********************

token = notebookutils.credentials.getToken('pbi')
os.environ['FAB_TOKEN'] = token
os.environ['FAB_TOKEN_ONELAKE'] = token

items_in_ws =  json.loads(run_fab_command(f'api /workspaces/{trg_workspace_id}/items', capture_output= True))['text']['value']


def find_existing_item_id(item_name):
    for item in items_in_ws:
        if item_name == item['displayName'] + '.' + item['type']:
            return item['id']


for folder in config['folders']:
    print(folder['name'])
    folder_name = folder['name']

    folder_exists = run_fab_command(f'exists /{trg_workspace_name}.Workspace/{folder_name}.Folder', capture_output= True)
    print(folder_exists)
    if 'false' in folder_exists:
        print(f'Create folder {folder_name}')
        run_fab_command(f'create /{trg_workspace_name}.Workspace/{folder_name}.Folder')
    
    folder_id = run_fab_command(f'get {trg_workspace_name}.Workspace/{folder_name}.Folder -q id',  capture_output= True) 
    print(f'Move items into folder: {folder_name}')  
    item_ids = []
    for item in folder['items']:
        found_it = find_existing_item_id(item)
        if found_it is not None:
            item_ids.append(found_it)
    it = str(item_ids).replace("'", '"')
    res = run_fab_command(f' api -X post workspaces/{trg_workspace_id}/items/bulkmove  -i \'{{"targetFolderId": "{folder_id}", "items": {it} }}\' ', capture_output = True)

    


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Post-Deployment logic
# In this separate notebook, all needed tables for FUAM are automatically deployed. Addtionally new columns will be added to lakehouse tables in order to be available for the semantic model. This notebook has been deployed from the source code in the step before

# CELL ********************

# Set environment parameters for Fabric CLI
token = notebookutils.credentials.getToken('pbi')
os.environ['FAB_TOKEN'] = token
os.environ['FAB_TOKEN_ONELAKE'] = token


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

abfss_path = f"abfss://{trg_workspace_name}@onelake.dfs.fabric.microsoft.com/FUAM_Config_Lakehouse.Lakehouse"

src_file_path = "./builtin/data/table_definitions.snappy.parquet"
trg_lakehouse_folder_path = abfss_path + "/Files/table_definitions/" 
notebookutils.fs.mkdirs(trg_lakehouse_folder_path)
notebookutils.fs.cp("./builtin/data/table_definitions.snappy.parquet", trg_lakehouse_folder_path + "table_definitions.snappy.parquet", recurse=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

pdf = pd.read_parquet(f"{notebookutils.nbResPath}/builtin/data/table_definitions.snappy.parquet")
write_deltalake(abfss_path + "/Tables/FUAM_Table_Definitions" , pdf, mode="overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# In case the last step fails, please try to run it again or go to the Init_FUAM_Lakehouse_Tables Notebook and run it manually

# CELL ********************

# Refresh SQL Endpoint for Config_Lakehouse
items = run_fab_command(f'api -X get -A fabric /workspaces/{trg_workspace_id}/items' , capture_output = True)
for it in json.loads(items)['text']['value']:
    if (it['displayName'] == 'FUAM_Config_Lakehouse' ) & (it['type'] =='SQLEndpoint' ):
        config_sql_endpoint = it['id']
    if (it['displayName'] == 'FUAM_Lakehouse' ) & (it['type'] =='SQLEndpoint' ):
        lh_sql_endpoint = it['id']
print(f"FUAM_Lakehouse SQL Endpoint ID: {lh_sql_endpoint}")
print(f"FUAM_Config_Lakehouse SQL Endpoint ID: {config_sql_endpoint}")

try:
    run_fab_command(f'api -A fabric -X post workspaces/{trg_workspace_id}/sqlEndpoints/{config_sql_endpoint}/refreshMetadata?preview=True -i {{}} ', capture_output=True)
except:
    print("SQL Endpoint Refresh API failed, it is still in Preview, so there can be changes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Fill default tables
time.sleep(10)
run_fab_command('job run ' + trg_workspace_name + '.Workspace/Init_FUAM_Lakehouse_Tables.Notebook -i {"parameters": {"_inlineInstallationEnabled": {"type": "Bool", "value": "True"} } }', timeout = 3600)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Refresh of SQL Endpoint to make sure all tables are available
try:
    run_fab_command(f'api -A fabric -X post workspaces/{trg_workspace_id}/sqlEndpoints/{lh_sql_endpoint}/refreshMetadata?preview=True -i {{}} ', capture_output=True)
    print("Refresh FUAM_Lakehouse_SQL_Endpoint")
except:
    print("SQL Endpoint Refresh API failed, it is still in Preview, so there can be changes")
# Refresh Semantic Models on top of lakehouse
base_path = './builtin/'
config_path = os.path.join(base_path, 'config/deployment_config.yaml')

with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

semantic_model_connect_to_lakehouse = config['fuam_lakehouse_semantic_models']

for sm in semantic_model_connect_to_lakehouse:
    sm_id = run_fab_command(f"get /{trg_workspace_name}.Workspace/{sm} -q id" , capture_output = True, silently_continue= True)
    run_fab_command(f'api -A powerbi -X post datasets/{sm_id}/refreshes -i  {{ "retryCount":"3" }} ')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
