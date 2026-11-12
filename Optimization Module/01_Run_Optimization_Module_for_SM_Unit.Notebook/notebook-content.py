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

# #### Optimization Module for Semantic Models (Beta)
# 
# ##### Description:
# This notebook runs the **Best Practice Analyzer (BPA)** and **VertiPaq Analyzer** for the **top *n* semantic models per capacity** (ranked by previous-day CU consumption) and persists the results to the FUAM Lakehouse. It is intended to provide deep, item-level optimization insights for the semantic models that contribute most to capacity utilization.
# 
# Authentication is performed through a **Service Principal (SPN)** whose credentials are stored in **Azure Key Vault**. If the SPN does not yet have access to a target workspace, the notebook automatically adds it as a workspace **Member** (using the executing user's tenant-admin permissions) and logs the assignment to an audit table.
# 
# ##### Data ingestion strategy:
# <mark style="background: #88D5FF;">**APPEND**</mark>
# 
# ##### Related pipeline:
# 
# *(standalone notebook – typically scheduled directly or via a dedicated pipeline)*
# 
# ##### Source:
# 
# - **FUAM_Lakehouse.capacity_metrics_by_item_by_operation_by_day** – used to rank semantic models per capacity by CU consumption.
# - **FUAM_Lakehouse.semantic_models** – used to filter to import / composite-mode semantic models.
# - **Power BI REST / XMLA endpoints** of the selected semantic models (queried as the SPN) for BPA and VertiPaq metadata.
# 
# ##### Target:
# 
# **Delta tables** in FUAM_Lakehouse
# - **semantic_model_analyzer_runs** – one row per analyzer run (header / model-level VertiPaq summary)
# - **semantic_model_bpa_results** – BPA rule violations per run
# - **semantic_model_vertipaq_tables** – table-level VertiPaq metrics per run *(not written when `analyzer_mode = 'minimal'`)*
# - **semantic_model_vertipaq_relationships** – relationship-level VertiPaq metrics per run *(not written when `analyzer_mode = 'minimal'`)*
# - **audit_granted_ws_access_for_analyzer** – audit log of workspaces to which the SPN was added by this notebook
# 
# ##### Parameters:
# 
# - **top_n_semantic_models_per_capacity** – number of top CU-consuming semantic models analyzed per capacity.
# - **analyzer_mode** – `'normal'` (full BPA + table/relationship VertiPaq) or `'minimal'` (reduced BPA rule set, model-level VertiPaq only).
# - **display_data** – toggle intermediate `display()` output for debugging.
# - **keyvault_name** and the four **keyvault_sp_..._secret_name** parameters – Key Vault and secret names used to retrieve the SPN tenant ID, client ID, client secret, and enterprise application object ID.
# </newCode>
# </invoke>


# MARKDOWN ********************

# #### Parameters & Setup
# 
# The following cells install required packages, declare notebook parameters and variables, and validate that all required Key Vault secret names have been provided. Execution stops early if any secret name is missing.
# </newCode>
# </invoke>

# CELL ********************

# Install the latest .whl package
%pip install semantic-link-labs --q

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

## Parameters
top_n_semantic_models_per_capacity = 5
analyzer_mode = 'normal' # Allowed enum values: [minimal] or [normal]
display_data = True

# Required
# Key Vault details, this should be used to query the APIs via Service Principal instead of the user
keyvault_name = 'fuamkv' # Name of the Azure Key Vault
# Names of the secrets saved in Azure Key Vault 
keyvault_sp_tenantId_secret_name = 'fuam-sp-tenant'   # Tenant ID secret name
keyvault_sp_clientId_secret_name = 'fuam-sp-client'   # Name for Client ID of Service Principal
keyvault_sp_secret_secret_name = 'fuam-sp-secret' # Name for Client Secret of Service Principal
keyvault_sp_ent_app_object_id_secret_name = 'fuam-sp-enterprise-app-object-id' # Name for Enterprise App Object Id of Service Principal

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Variables
## Don't change
analyzer_principal_type = 'app' # [app]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if key vault secrets are configured in the right way. If not the executing users identity is used
try:
    keyvault = f'https://{keyvault_name}.vault.azure.net/'
    tenant_id = notebookutils.credentials.getSecret(keyvault, keyvault_sp_tenantId_secret_name)
    client_id = notebookutils.credentials.getSecret(keyvault, keyvault_sp_clientId_secret_name)
    client_secret = notebookutils.credentials.getSecret(keyvault, keyvault_sp_secret_secret_name)
    enterprise_app_object_id = notebookutils.credentials.getSecret(keyvault, keyvault_sp_ent_app_object_id_secret_name)
    print("INFO: Service Principal identity is used for API authentification.")
except:
    raise Exception("ERROR: Configured Secrets not found in Key Vault. It is required for FUAM's Optimization Module")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Required pip packages & methods
# 
# Imports the libraries used throughout the notebook (`sempy`, `sempy_labs`, `msal`, `pandas`, etc.) and defines two helper variants of `execute_dax_to_pandas` for running DAX against a semantic model using the Service Principal context.
# </newCode>
# </invoke>

# CELL ********************

# =============================================================================
# Required packages
# All notebook-wide imports are centralized here. Lazy / function-scoped
# imports are kept only where they are required (e.g. the .NET TOM assembly
# inside `model_bpa_rules_v2`, which must be imported after
# `_init_analysis_services()` has loaded the assembly).
# =============================================================================

# --- Standard library ---
import io
import logging
import re
import warnings
from datetime import datetime
from typing import Optional

# --- Third-party ---
import pandas as pd
import pyarrow as pa
import requests
from msal import ConfidentialClientApplication

# --- PySpark ---
from pyspark.sql import Row

# --- sempy / sempy-labs ---
import sempy                                  # needed for sempy.fabric._client._utils._init_analysis_services()
import sempy.fabric as fabric
import sempy_labs as labs
from sempy_labs._authentication import token_provider
from sempy_labs._helper_functions import (
    create_relationship_name,
    format_dax_object_name,
)
from sempy_labs._list_functions import list_relationships
from sempy_labs._model_dependencies import get_model_calc_dependencies
from sempy_labs.tom import connect_semantic_model

# --- Logging configuration ---
logging.getLogger("trident_token_library_wrapper").setLevel(logging.WARNING)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# This function must be called from a SP context
def execute_dax_to_pandas(workspace_id: str, dataset_id: str, query: str) -> pd.DataFrame:

    tp = token_provider.get()           # ServicePrincipalTokenProvider
    access_token = tp("pbi")


    url = (
        f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}"
        f"/datasets/{dataset_id}/executeDaxQueries"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    body = {
        "query": query,
        "resultsetRowcountLimit": 500000
    }

    response = requests.post(url, headers=headers, json=body, timeout=180)
    response.raise_for_status()

    reader = pa.ipc.open_stream(io.BytesIO(response.content))

    table = reader.read_all()
    return table.to_pandas()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def execute_dax_to_pandas(workspace_id: str, dataset_id: str, query: str) -> pd.DataFrame:

    # --- MSAL SETUP ---
    AUTHORITY = f'https://login.microsoftonline.com/{tenant_id}'
    SCOPE = ['https://analysis.windows.net/powerbi/api/.default']

    app = ConfidentialClientApplication(
    client_id= client_id, authority=AUTHORITY, client_credential=client_secret
    )

    # --- Acquire Token ---
    result = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        raise Exception(f"Failed to get token: {result.get('error_description')}")

    access_token = result['access_token']
    


    url = (
        f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}"
        f"/datasets/{dataset_id}/executeDaxQueries"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    body = {
        "query": query,
        "resultsetRowcountLimit": 500000
    }

    response = requests.post(url, headers=headers, json=body, timeout=180)
    response.raise_for_status()

    reader = pa.ipc.open_stream(io.BytesIO(response.content))

    table = reader.read_all()
    return table.to_pandas()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Grant Workspace Access
# 
# Defines `add_member_to_workspace`, which assigns the analyzer Service Principal **Member** access to a workspace via the Power BI Admin REST API when access is missing. Each grant is logged to the `audit_granted_ws_access_for_analyzer` Delta table so administrators can review (and, if needed, revert) the access changes performed by this notebook.
# </newCode>
# </invoke>

# CELL ********************

# It adds the selected Service Principal to workspace (via Power BI Admin REST API), 
# whenever the app doesn't have access to a specific workspace
# The assignment will be logged in the FUAM Lakehouse 'audit_added_fuam_user_to_ws' table
# REST API Link: https://learn.microsoft.com/en-us/rest/api/power-bi/admin/groups-add-user-as-admin
def add_member_to_workspace(analyzer_entity: str, workspace_id: str, analyzer_principal_type: str):

    access_right = 'Member'

    access_token = notebookutils.credentials.getToken('pbi')

    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    # Allowed values: [user] or [app]
    if analyzer_principal_type == 'user':
        request_body = {
            "emailAddress": analyzer_entity,
            "groupUserAccessRight": access_right
            }
    elif analyzer_principal_type == 'app':
        request_body = {
            "identifier": analyzer_entity, # -> enterprise_app_object_id
            "principalType": "App",
            "groupUserAccessRight": access_right
            }
    else:
        request_body = None

    api_url = f'https://api.powerbi.com/v1.0/myorg/admin/groups/{workspace_id}/users'

    response = requests.post(api_url, json=request_body, headers=headers)

    if response.status_code != 200:
        raise FabricHTTPException(response)
    else:
        print(f"Status code: {response.status_code}. {analyzer_principal_type.upper()} has been added to workspace: '{workspace_id}' as {access_right}")


    # Add assignment to audit table in FUAM Lakehouse
    df_added_fuam_analyzer_member_to_ws = spark.createDataFrame([ 
        Row(timestamp= datetime.now(), principal_type=analyzer_principal_type, entity=analyzer_entity.upper(), workspace_id=workspace_id.upper(), access_right=access_right, reason="Semantic model caused high CU utilization.")
      ])  
    
    df_added_fuam_analyzer_member_to_ws.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable("audit_granted_ws_access_for_analyzer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Best Practice Analyzer (BPA)
# 
# Defines the FUAM BPA rule set (`model_bpa_rules_v2`) and the `scan_semantic_model_bpa` function, which connects to a semantic model via TOM, evaluates each rule against the appropriate scope (Model, Table, Column, Measure, Relationship, etc.), and returns the detected violations as a `pandas.DataFrame`. The `analyzer_mode` parameter controls which subset of rules is evaluated.
# </newCode>
# </invoke>

# CELL ********************

def model_bpa_rules_v2(
    dependencies: Optional[pd.DataFrame] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Shows the default rules for the semantic model BPA used by the run_model_bpa function.

    Parameters
    ----------
    dependencies : pd.DataFrame, default=None
        A pandas dataframe with the output of the 'get_model_calc_dependencies' function.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe containing the default rules for the run_model_bpa function.
    """

    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM


    # Catalog of semantic model BPA rules (configured for FUAM)
    rules = pd.DataFrame(
        [
                (
                    1,
                    lambda obj, tom: obj.DataType == TOM.DataType.Double,
                    "Performance",
                    False,
                    ["Column"],
                    "Warning",
                    "Query Operation",
                    "Do not use floating point data types",
                    'The "Double" floating point data type should be avoided, as it can result in unpredictable roundoff errors and decreased performance in certain scenarios. Use "Int64" or "Decimal" where appropriate (but note that "Decimal" is limited to 4 digits after the decimal sign).',
                ),
                (
                    2,
                    lambda obj, tom: obj.Type == TOM.ColumnType.Calculated,
                    "Performance",
                    False,
                    ["Column"],
                    "Warning",
                    "Background Operation",
                    "Avoid using calculated columns",
                    "Calculated columns do not compress as well as data columns so they take up more memory. They also slow down processing times for both the table as well as process recalc. Offload calculated column logic to your data warehouse and turn these calculated columns into data columns.",
                    "https://www.elegantbi.com/post/top10bestpractices",
                ),
                (
                    3,
                    lambda obj, tom: (
                            obj.FromCardinality == TOM.RelationshipEndCardinality.Many
                            and obj.ToCardinality == TOM.RelationshipEndCardinality.Many
                        )
                        or str(obj.CrossFilteringBehavior) == "BothDirections",
                    "Performance",
                    False,
                    ["Relationship"],
                    "Warning",
                    "Query Operation",
                    "Check if bi-directional and many-to-many relationships are valid",
                    "Bi-directional and many-to-many relationships may cause performance degradation or even have unintended consequences. Make sure to check these specific relationships to ensure they are working as designed and are actually necessary.",
                    "https://www.sqlbi.com/articles/bidirectional-relationships-and-ambiguity-in-dax",
                ),
                (
                    4,
                    lambda obj, tom: any(
                            re.search(pattern, obj.FilterExpression, flags=re.IGNORECASE)
                            for pattern in ["USERPRINCIPALNAME()", "USERNAME()"]
                        ),
                    "Performance",
                    False,
                    ["Row Level Security"],
                    "Info",
                    "Query Operation",
                    "Check if dynamic row level security (RLS) is necessary",
                    "Usage of dynamic row level security (RLS) can add memory and performance overhead. Please research the pros/cons of using it.",
                    "https://docs.microsoft.com/power-bi/admin/service-admin-rls",
                ),
                (
                    5,
                    lambda obj, tom: any(
                            r.FromCardinality == TOM.RelationshipEndCardinality.Many
                            and r.ToCardinality == TOM.RelationshipEndCardinality.Many
                            for r in tom.used_in_relationships(object=obj)
                        )
                        and any(t.Name == obj.Name for t in tom.all_rls()),
                    "Performance",
                    False,
                    "Table",
                    "Warning",
                    "Query Operation",
                    "Avoid using many-to-many relationships on tables used for dynamic row level security",
                    "Using many-to-many relationships on tables which use dynamic row level security can cause serious query performance degradation. This pattern's performance problems compound when snowflaking multiple many-to-many relationships against a table which contains row level security. Instead, use one of the patterns shown in the article below where a single dimension table relates many-to-one to a security table.",
                    "https://www.elegantbi.com/post/dynamicrlspatterns",
                ),
                (
                    6,
                    lambda obj, tom: (
                            obj.FromCardinality == TOM.RelationshipEndCardinality.Many
                            and obj.ToCardinality == TOM.RelationshipEndCardinality.Many
                        )
                        and obj.CrossFilteringBehavior
                        == TOM.CrossFilteringBehavior.BothDirections,
                    "Performance",
                    False,
                    "Relationship",
                    "Warning",
                    "Query Operation",
                    "Many-to-many relationships should be single-direction",
                    "",
                    "https://medium.com/@anu.ckp.1313/power-up-your-data-models-best-practices-for-relationship-design-to-boost-performance-in-power-bi-3a6a1e2a839a"
                ),
                (
                    7,
                    lambda obj, tom: tom.is_direct_lake() is False
                        and obj.IsAvailableInMDX
                        and (obj.IsHidden or obj.Parent.IsHidden)
                        and obj.SortByColumn is None
                        and not any(tom.used_in_sort_by(column=obj))
                        and not any(tom.used_in_hierarchies(column=obj)),
                    "Performance",
                    False,
                    "Column",
                    "Warning",
                    "Query Operation",
                    "Set IsAvailableInMdx to false on non-attribute columns",
                    "To speed up processing time and conserve memory after processing, attribute hierarchies should not be built for columns that are never used for slicing by MDX clients. In other words, all hidden columns that are not used as a Sort By Column or referenced in user hierarchies should have their IsAvailableInMdx property set to false. The IsAvailableInMdx property is not relevant for Direct Lake models.",
                    "https://blog.crossjoin.co.uk/2018/07/02/isavailableinmdx-ssas-tabular",
                ),
                (
                    8,
                    lambda obj, tom: tom.is_hybrid_table(table_name=obj.Parent.Name)
                        and obj.Mode == TOM.ModeType.DirectQuery
                        and obj.DataCoverageDefinition is None,
                    "Performance",
                    False,
                    "Partition",
                    "Warning",
                    "Query Operation",
                    "Set 'Data Coverage Definition' property on the DirectQuery partition of a hybrid table",
                    "Setting the 'Data Coverage Definition' property may lead to better performance because the engine knows when it can only query the import-portion of the table and when it needs to query the DirectQuery portion of the table.",
                    "https://learn.microsoft.com/analysis-services/tom/table-partitions?view=asallproducts-allversions",
                ),
                (
                    9,
                    lambda obj, tom: not any(
                            p.Mode == TOM.ModeType.DirectQuery for p in tom.all_partitions()
                        )
                        and any(p.Mode == TOM.ModeType.Dual for p in tom.all_partitions()),
                    "Performance",
                    False,
                    "Model",
                    "Warning",
                    "Query Operation",
                    "Dual mode is only relevant for dimension tables if DirectQuery is used for the corresponding fact table",
                    "Only use Dual mode for dimension tables/partitions where a corresponding fact table is in DirectQuery. Using Dual mode in other circumstances (i.e. rest of the model is in Import mode) may lead to performance issues especially if the number of measures in the model is high.",
                    ""
                ),
                (
                    10,
                    lambda obj, tom: sum(
                            1 for p in obj.Partitions if p.Mode == TOM.ModeType.Import
                        )
                        == 1
                        and obj.Partitions.Count == 1
                        and tom.has_hybrid_table()
                        and any(
                            r.ToCardinality == TOM.RelationshipEndCardinality.One
                            and r.ToTable.Name == obj.Name
                            for r in tom.used_in_relationships(object=obj)
                        ),
                    "Performance",
                    False,
                    "Table",
                    "Warning",
                    "Background Operation",
                    "Set dimensions tables to dual mode instead of import when using DirectQuery on fact tables",
                    "When using DirectQuery, dimension tables should be set to Dual mode in order to improve query performance.",
                    "https://learn.microsoft.com/power-bi/transform-model/desktop-storage-mode#propagation-of-the-dual-setting",
                ),
                (
                    11,
                    lambda obj, tom: obj.SourceType == TOM.PartitionSourceType.M
                        and any(
                            item in obj.Source.Expression
                            for item in [
                                'Table.Combine("',
                                'Table.Join("',
                                'Table.NestedJoin("',
                                'Table.AddColumn("',
                                'Table.Group("',
                                'Table.Sort("',
                                'Table.Pivot("',
                                'Table.Unpivot("',
                                'Table.UnpivotOtherColumns("',
                                'Table.Distinct("',
                                '[Query=(""SELECT',
                                "Value.NativeQuery",
                                "OleDb.Query",
                                "Odbc.Query",
                            ]
                        ),
                    "Performance",
                    False,
                    "Partition",
                    "Warning",
                    "Background Operation",
                    "Minimize Power Query transformations",
                    "Minimize Power Query transformations in order to improve model processing performance. It is a best practice to offload these transformations to the data warehouse if possible. Also, please check whether query folding is occurring within your model. Please reference the article below for more information on query folding.",
                    "https://docs.microsoft.com/power-query/power-query-folding",
                ),
                (
                    12,
                    lambda obj, tom: obj.CalculationGroup is None
                        and (
                            any(
                                r.FromTable.Name == obj.Name
                                for r in tom.used_in_relationships(object=obj)
                            )
                            and any(
                                r.ToTable.Name == obj.Name
                                for r in tom.used_in_relationships(object=obj)
                            )
                        ),
                    "Performance",
                    False,
                    "Table",
                    "Warning",
                    "Query Operation",
                    "Consider a star-schema instead of a snowflake architecture",
                    "Generally speaking, a star-schema is the optimal architecture for tabular models. That being the case, there are valid cases to use a snowflake approach. Please check your model and consider moving to a star-schema architecture.",
                    "https://docs.microsoft.com/power-bi/guidance/star-schema",
                ),
                (
                    13,
                    lambda obj, tom: tom.is_direct_lake_using_view(),
                    "Performance",
                    False,
                    "Model",
                    "Warning",
                    "Query Operation",
                    "Avoid using views when using Direct Lake mode",
                    "In Direct Lake mode, views will always fall back to DirectQuery. Thus, in order to obtain the best performance use lakehouse tables instead of views.",
                    "https://learn.microsoft.com/fabric/get-started/direct-lake-overview#fallback",
                ),
                (
                    14,
                    lambda obj, tom: obj.Expression.replace(" ", "").startswith("0+")
                        or obj.Expression.replace(" ", "").endswith("+0")
                        or re.search(
                            r"DIVIDE\s*\(\s*[^,]+,\s*[^,]+,\s*0\s*\)",
                            obj.Expression,
                            flags=re.IGNORECASE,
                        )
                        or re.search(
                            r"IFERROR\s*\(\s*[^,]+,\s*0\s*\)",
                            obj.Expression,
                            flags=re.IGNORECASE,
                        ),
                    "Performance",
                    False,
                    "Measure",
                    "Warning",
                    "Query Operation",
                    "Avoid adding 0 to a measure",
                    "Adding 0 to a measure in order for it not to show a blank value may negatively impact performance.",
                    ""
                ),
                (
                    15,
                    lambda obj, tom: tom.is_field_parameter(table_name=obj.Name) is False
                        and tom.is_calculated_table(table_name=obj.Name),
                    "Performance",
                    False,
                    "Table",
                    "Warning",
                    "Background Operation",
                    "Reduce usage of calculated tables",
                    "Migrate calculated table logic to your data warehouse. Reliance on calculated tables will lead to technical debt and potential misalignments if you have multiple models on your platform.",
                    ""
                ),
                (
                    16,
                    lambda obj, tom: obj.Type == TOM.ColumnType.Calculated
                        and re.search(r"related\s*\(", obj.Expression, flags=re.IGNORECASE),
                    "Performance",
                    False,
                    "Column",
                    "Warning",
                    "Query Operation",
                    "Reduce usage of calculated columns that use the RELATED function",
                    "Calculated columns do not compress as well as data columns and may cause longer processing times. As such, calculated columns should be avoided if possible. One scenario where they may be easier to avoid is if they use the RELATED function.",
                    "https://www.sqlbi.com/articles/storage-differences-between-calculated-columns-and-calculated-tables",
                ),
                (
                    17,
                    lambda obj, tom: (
                            (
                                sum(
                                    1
                                    for r in obj.Relationships
                                    if r.CrossFilteringBehavior
                                    == TOM.CrossFilteringBehavior.BothDirections
                                )
                                + sum(
                                    1
                                    for r in obj.Relationships
                                    if (
                                        r.FromCardinality == TOM.RelationshipEndCardinality.Many
                                    )
                                    and (r.ToCardinality == TOM.RelationshipEndCardinality.Many)
                                )
                            )
                            / max(int(obj.Relationships.Count), 1)
                        )
                        > 0.3,
                    "Performance",
                    False,
                    "Model",
                    "Warning",
                    "Query Operation",
                    "Avoid excessive bi-directional or many-to-many relationships",
                    "Limit use of b-di and many-to-many relationships. This rule flags the model if more than 30% of relationships are bi-di or many-to-many.",
                    "https://www.sqlbi.com/articles/bidirectional-relationships-and-ambiguity-in-dax",
                ),
                (
                    18,
                    lambda obj, tom: tom.is_calculated_table(table_name=obj.Name)
                        and (
                            obj.Name.startswith("DateTableTemplate_")
                            or obj.Name.startswith("LocalDateTable_")
                        ),
                    "Performance",
                    False,
                    "Table",
                    "Warning",
                    "Query Operation",
                    "Remove auto-date table",
                    "Avoid using auto-date tables. Make sure to turn off auto-date table in the settings in Power BI Desktop. This will save memory resources.",
                    "https://www.youtube.com/watch?v=xu3uDEHtCrg",
                ),
                (
                    19,
                    lambda obj, tom: (
                            re.search(r"date", obj.Name, flags=re.IGNORECASE)
                            or re.search(r"calendar", obj.Name, flags=re.IGNORECASE)
                        )
                        and str(obj.DataCategory) != "Time",
                    "Performance",
                    False,
                    "Table",
                    "Warning",
                    "Usability",
                    "Date/calendar tables should be marked as a date table",
                    "This rule looks for tables that contain the words 'date' or 'calendar' as they should likely be marked as a date table.",
                    "https://docs.microsoft.com/power-bi/transform-model/desktop-date-tables",
                ),
                (
                    20,
                    lambda obj, tom: tom.is_direct_lake() is False
                        and int(obj.Partitions.Count) == 1
                        and tom.row_count(object=obj) > 25000000,
                    "Performance",
                    False,
                    "Table",
                    "Warning",
                    "Background Operation",
                    "Large tables should be partitioned",
                    "Large tables should be partitioned in order to optimize processing. This is not relevant for semantic models in Direct Lake mode as they can only have one partition per table.",
                    ""
                ),
                (
                    21,
                    lambda obj, tom: any(
                            item in obj.FilterExpression.lower()
                            for item in [
                                "right(",
                                "left(",
                                "filter(",
                                "upper(",
                                "lower(",
                                "find(",
                            ]
                        ),
                    "Performance",
                    False,
                    "Row Level Security",
                    "Warning",
                    "Query Operation",
                    "Limit row level security (RLS) logic",
                    "Try to simplify the DAX used for row level security. Usage of the functions within this rule can likely be offloaded to the upstream systems (data warehouse).",
                    ""
                ),
                (
                    22,
                    lambda obj, tom: not any(
                            (c.IsKey and c.DataType == TOM.DataType.DateTime)
                            and str(t.DataCategory) == "Time"
                            for t in obj.Tables
                            for c in t.Columns
                        ),
                    "Performance",
                    False,
                    "Model",
                    "Warning",
                    "Usability",
                    "Model should have a date table",
                    "Generally speaking, models should generally have a date table. Models that do not have a date table generally are not taking advantage of features such as time intelligence or may not have a properly structured architecture.",
                    ""
                ),
                (
                    23,
                    lambda obj, tom: len(obj.Expression) == 0,
                    "Error Prevention",
                    False,
                    "Calculation Item",
                    "Error",
                    "Usability",
                    "Calculation items must have an expression",
                    "Calculation items must have an expression. Without an expression, they will not show any values.",
                    ""
                ),
                (
                    24,
                    lambda obj, tom: obj.FromColumn.DataType != obj.ToColumn.DataType,
                    "Error Prevention",
                    False,
                    "Relationship",
                    "Warning",
                    "Query Operation",
                    "Relationship columns should be of the same data type",
                    "Columns used in a relationship should be of the same data type. Ideally, they will be of integer data type (see the related rule '[Formatting] Relationship columns should be of integer data type'). Having columns within a relationship which are of different data types may lead to various issues.",
                    ""
                ),
                (
                    25,
                    lambda obj, tom: obj.Type == TOM.ColumnType.Data
                        and len(obj.SourceColumn) == 0,
                    "Error Prevention",
                    False,
                    "Column",
                    "Error",
                    "Usability",
                    "Data columns must have a source column",
                    "Data columns must have a source column. A data column without a source column will cause an error when processing the model.",
                    ""
                ),
                (
                    26,
                    lambda obj, tom: tom.is_direct_lake() is False
                        and obj.IsAvailableInMDX is False
                        and (
                            any(tom.used_in_sort_by(column=obj))
                            or any(tom.used_in_hierarchies(column=obj))
                            or obj.SortByColumn is not None
                        ),
                    "Error Prevention",
                    False,
                    "Column",
                    "Warning",
                    "Query Operation",
                    "Set IsAvailableInMdx to true on necessary columns",
                    "In order to avoid errors, ensure that attribute hierarchies are enabled if a column is used for sorting another column, used in a hierarchy, used in variations, or is sorted by another column. The IsAvailableInMdx property is not relevant for Direct Lake models.",
                    ""
                ),
                (
                    27,
                    lambda obj, tom: any(
                            re.search(
                                r"USERELATIONSHIP\s*\(\s*.+?(?=])\]\s*,\s*'*"
                                + obj.Name
                                + r"'*\[",
                                m.Expression,
                                flags=re.IGNORECASE,
                            )
                            for m in tom.all_measures()
                        )
                        and any(r.Table.Name == obj.Name for r in tom.all_rls()),
                    "Error Prevention",
                    False,
                    "Table",
                    "Error",
                    "Query Operation",
                    "Avoid the USERELATIONSHIP function and RLS against the same table",
                    "The USERELATIONSHIP function may not be used against a table which also leverages row-level security (RLS). This will generate an error when using the particular measure in a visual. This rule will highlight the table which is used in a measure's USERELATIONSHIP function as well as RLS.",
                    "https://blog.crossjoin.co.uk/2013/05/10/userelationship-and-tabular-row-security",
                ),
                (
                    28,
                    lambda obj, tom: re.search(
                            r"iferror\s*\(", obj.Expression, flags=re.IGNORECASE
                        ),
                    "DAX Expressions",
                    False,
                    "Measure",
                    "Warning",
                    "Query Operation",
                    "Avoid using the IFERROR function",
                    "Avoid using the IFERROR function as it may cause performance degradation. If you are concerned about a divide-by-zero error, use the DIVIDE function as it naturally resolves such errors as blank (or you can customize what should be shown in case of such an error).",
                    "https://www.elegantbi.com/post/top10bestpractices",
                ),
                (
                    29,
                    lambda obj, tom: re.search(
                            r"intersect\s*\(", obj.Expression, flags=re.IGNORECASE
                        ),
                    "DAX Expressions",
                    False,
                    "Measure",
                    "Warning",
                    "Query Operation",
                    "Use the TREATAS function instead of INTERSECT for virtual relationships",
                    "The TREATAS function is more efficient and provides better performance than the INTERSECT function when used in virutal relationships.",
                    "https://www.sqlbi.com/articles/propagate-filters-using-treatas-in-dax",
                ),
                (
                    30,
                    lambda obj, tom: re.search(
                            r"evaluateandlog\s*\(", obj.Expression, flags=re.IGNORECASE
                        ),
                    "DAX Expressions",
                    False,
                    "Measure",
                    "Warning",
                    "Usability",
                    "The EVALUATEANDLOG function should not be used in production models",
                    "The EVALUATEANDLOG function is meant to be used only in development/test environments and should not be used in production models.",
                    "https://pbidax.wordpress.com/2022/08/16/introduce-the-dax-evaluateandlog-function",
                ),
                (
                    31,
                    lambda obj, tom: any(
                            obj.Expression == f"[{m.Name}]" for m in tom.all_measures()
                        ),
                    "DAX Expressions",
                    False,
                    "Measure",
                    "Warning",
                    "Usability",
                    "Measures should not be direct references of other measures",
                    "This rule identifies measures which are simply a reference to another measure. As an example, consider a model with two measures: [MeasureA] and [MeasureB]. This rule would be triggered for MeasureB if MeasureB's DAX was MeasureB:=[MeasureA]. Such duplicative measures should be removed.",
                    ""
                ),
                (
                    32,
                    lambda obj, tom: any(
                            re.sub(r"\s+", "", obj.Expression)
                            == re.sub(r"\s+", "", m.Expression)
                            and obj.Name != m.Name
                            for m in tom.all_measures()
                        ),
                    "DAX Expressions",
                    False,
                    "Measure",
                    "Warning",
                    "Usability",
                    "No two measures should have the same definition",
                    "Two measures with different names and defined by the same DAX expression should be avoided to reduce redundancy.",
                    ""
                ),
                (
                    33,
                    lambda obj, tom: re.search(
                            r"DIVIDE\s*\((\s*.*?)\)\s*[+-]\s*1|\/\s*.*(?=[-+]\s*1)",
                            obj.Expression,
                            flags=re.IGNORECASE,
                        ),
                    "DAX Expressions",
                    False,
                    "Measure",
                    "Warning",
                    "Query Operation",
                    "Avoid addition or subtraction of constant values to results of divisions",
                    "Adding a constant value may lead to performance degradation.",
                    ""
                ),
                (
                    34,
                    lambda obj, tom: re.search(
                            r"[0-9]+\s*[-+]\s*[\(]*\s*SUM\s*\(\s*\'*[A-Za-z0-9 _]+\'*\s*\[[A-Za-z0-9 _]+\]\s*\)\s*/",
                            obj.Expression,
                            flags=re.IGNORECASE,
                        )
                        or re.search(
                            r"[0-9]+\s*[-+]\s*DIVIDE\s*\(",
                            obj.Expression,
                            flags=re.IGNORECASE,
                        ),
                    "DAX Expressions",
                    False,
                    "Measure",
                    "Warning",
                    "Query Operation",
                    "Avoid using '1-(x/y)' syntax",
                    "Instead of using the '1-(x/y)' or '1+(x/y)' syntax to achieve a percentage calculation, use the basic DAX functions (as shown below). Using the improved syntax will generally improve the performance. The '1+/-...' syntax always returns a value whereas the solution without the '1+/-...' does not (as the value may be 'blank'). Therefore the '1+/-...' syntax may return more rows/columns which may result in a slower query speed.    Let's clarify with an example:    Avoid this: 1 - SUM ( 'Sales'[CostAmount] ) / SUM( 'Sales'[SalesAmount] )  Better: DIVIDE ( SUM ( 'Sales'[SalesAmount] ) - SUM ( 'Sales'[CostAmount] ), SUM ( 'Sales'[SalesAmount] ) )  Best: VAR x = SUM ( 'Sales'[SalesAmount] ) RETURN DIVIDE ( x - SUM ( 'Sales'[CostAmount] ), x )",
                    ""
                ),
                (
                    35,
                    lambda obj, tom: re.search(
                            r"CALCULATE\s*\(\s*[^,]+,\s*FILTER\s*\(\s*\'*[A-Za-z0-9 _]+\'*\s*,\s*\[[^\]]+\]",
                            obj.Expression,
                            flags=re.IGNORECASE,
                        )
                        or re.search(
                            r"CALCULATETABLE\s*\(\s*[^,]*,\s*FILTER\s*\(\s*\'*[A-Za-z0-9 _]+\'*\s*,\s*\[",
                            obj.Expression,
                            flags=re.IGNORECASE,
                        ),
                    "DAX Expressions",
                    False,
                    "Measure",
                    "Warning",
                    "Query Operation",
                    "Filter measure values by columns, not tables",
                    "Instead of using this pattern FILTER('Table',[Measure]>Value) for the filter parameters of a CALCULATE or CALCULATETABLE function, use one of the options below (if possible). Filtering on a specific column will produce a smaller table for the engine to process, thereby enabling faster performance. Using the VALUES function or the ALL function depends on the desired measure result.\nOption 1: FILTER(VALUES('Table'[Column]),[Measure] > Value)\nOption 2: FILTER(ALL('Table'[Column]),[Measure] > Value)",
                    "https://docs.microsoft.com/power-bi/guidance/dax-avoid-avoid-filter-as-filter-argument",
                ),
                (
                    36,
                    lambda obj, tom: re.search(
                            r"CALCULATE\s*\(\s*[^,]+,\s*FILTER\s*\(\s*'*[A-Za-z0-9 _]+'*\s*,\s*'*[A-Za-z0-9 _]+'*\[[A-Za-z0-9 _]+\]",
                            obj.Expression,
                            flags=re.IGNORECASE,
                        )
                        or re.search(
                            r"CALCULATETABLE\s*\([^,]*,\s*FILTER\s*\(\s*'*[A-Za-z0-9 _]+'*\s*,\s*'*[A-Za-z0-9 _]+'*\[[A-Za-z0-9 _]+\]",
                            obj.Expression,
                            flags=re.IGNORECASE,
                        ),
                    "DAX Expressions",
                    False,
                    "Measure",
                    "Warning",
                    "Query Operation",
                    "Filter column values with proper syntax",
                    "Instead of using this pattern FILTER('Table','Table'[Column]=\"Value\") for the filter parameters of a CALCULATE or CALCULATETABLE function, use one of the options below. As far as whether to use the KEEPFILTERS function, see the second reference link below.\nOption 1: KEEPFILTERS('Table'[Column]=\"Value\")\nOption 2: 'Table'[Column]=\"Value\"",
                    "https://docs.microsoft.com/power-bi/guidance/dax-avoid-avoid-filter-as-filter-argument  Reference: https://www.sqlbi.com/articles/using-keepfilters-in-dax",
                ),
                (
                    37,
                    lambda obj, tom: re.search(
                            r"\]\s*\/(?!\/)(?!\*)\" or \"\)\s*\/(?!\/)(?!\*)",
                            obj.Expression,
                            flags=re.IGNORECASE,
                        ),
                    "DAX Expressions",
                    False,
                    "Measure",
                    "Warning",
                    "Query Operation",
                    "Use the DIVIDE function for division",
                    'Use the DIVIDE  function instead of using "/". The DIVIDE function resolves divide-by-zero cases. As such, it is recommended to use to avoid errors.',
                    "https://docs.microsoft.com/power-bi/guidance/dax-divide-function-operator",
                ),
                (
                    38,
                    lambda obj, tom: any(
                            tom.unqualified_columns(object=obj, dependencies=dependencies)
                        ),
                    "DAX Expressions",
                    True,
                    "Measure",
                    "Error",
                    "Usability",
                    "Column references should be fully qualified",
                    "Using fully qualified column references makes it easier to distinguish between column and measure references, and also helps avoid certain errors. When referencing a column in DAX, first specify the table name, then specify the column name in square brackets.",
                    "https://www.elegantbi.com/post/top10bestpractices",
                ),
                (
                    39,
                    lambda obj, tom: any(
                            tom.fully_qualified_measures(object=obj, dependencies=dependencies)
                        ),
                    "DAX Expressions",
                    True,
                    "Measure",
                    "Error",
                    "Usability",
                    "Measure references should be unqualified",
                    "Using unqualified measure references makes it easier to distinguish between column and measure references, and also helps avoid certain errors. When referencing a measure using DAX, do not specify the table name. Use only the measure name in square brackets.",
                    "https://www.elegantbi.com/post/top10bestpractices",
                ),
                (
                    40,
                    lambda obj, tom: obj.IsActive is False
                        and not any(
                            re.search(
                                r"USERELATIONSHIP\s*\(\s*\'*"
                                + obj.FromTable.Name
                                + r"'*\["
                                + obj.FromColumn.Name
                                + r"\]\s*,\s*'*"
                                + obj.ToTable.Name
                                + r"'*\["
                                + obj.ToColumn.Name
                                + r"\]",
                                m.Expression,
                                flags=re.IGNORECASE,
                            )
                            for m in tom.all_measures()
                        ),
                    "DAX Expressions",
                    False,
                    "Relationship",
                    "Warning",
                    "Usability",
                    "Inactive relationships that are never activated",
                    "Inactive relationships are activated using the USERELATIONSHIP function. If an inactive relationship is not referenced in any measure via this function, the relationship will not be used. It should be determined whether the relationship is not necessary or to activate the relationship via this method.",
                    "https://dax.guide/userelationship",
                ),
                (
                    41,
                    lambda obj, tom: (obj.IsHidden or obj.Parent.IsHidden)
                        and not any(tom.used_in_relationships(object=obj))
                        and not any(tom.used_in_hierarchies(column=obj))
                        and not any(tom.used_in_sort_by(column=obj))
                        and any(tom.depends_on(object=obj, dependencies=dependencies)),
                    "Maintenance",
                    True,
                    "Column",
                    "Warning",
                    "Usability",
                    "Remove unnecessary columns",
                    "Hidden columns that are not referenced by any DAX expressions, relationships, hierarchy levels or Sort By-properties should be removed.",
                    ""
                ),
                (
                    42,
                    lambda obj, tom: obj.IsHidden
                        and not any(tom.referenced_by(object=obj, dependencies=dependencies)),
                    "Maintenance",
                    True,
                    "Measure",
                    "Warning",
                    "Usability",
                    "Remove unnecessary measures",
                    "Hidden measures that are not referenced by any DAX expressions should be removed for maintainability.",
                    ""
                ),
                (
                    43,
                    lambda obj, tom: any(tom.used_in_relationships(object=obj)) is False
                        and obj.CalculationGroup is None,
                    "Maintenance",
                    False,
                    "Table",
                    "Warning",
                    "Usability",
                    "Ensure tables have relationships",
                    "This rule highlights tables which are not connected to any other table in the model with a relationship.",
                    ""
                ),
                (
                    44,
                    lambda obj, tom: obj.CalculationGroup is not None
                        and not any(obj.CalculationGroup.CalculationItems),
                    "Maintenance",
                    False,
                    "Table",
                    "Warning",
                    "Usability",
                    "Calculation groups with no calculation items",
                    "Calculation groups have no function unless they have calculation items.",
                    ""
                ),
                (
                    45,
                    lambda obj, tom: obj.IsHidden is False and len(obj.Description) == 0,
                    "Maintenance",
                    False,
                    ["Column", "Measure", "Table"],
                    "Info",
                    "Usability",
                    "Visible objects with no description",
                    "Add descriptions to objects. These descriptions are shown on hover within the Field List in Power BI Desktop. Additionally, you can leverage these descriptions to create an automated data dictionary.",
                    ""
                ),
                (
                    46,
                    lambda obj, tom: (re.search(r"date", obj.Name, flags=re.IGNORECASE))
                        and (obj.DataType == TOM.DataType.DateTime)
                        and (obj.FormatString != "mm/dd/yyyy"),
                    "Formatting",
                    False,
                    "Column",
                    "Warning",
                    "Usability",
                    "Provide format string for 'Date' columns",
                    'Columns of type "DateTime" that have "Month" in their names should be formatted as "mm/dd/yyyy".',
                    ""
                ),
                (
                    47,
                    lambda obj, tom: (
                            (obj.DataType == TOM.DataType.Int64)
                            or (obj.DataType == TOM.DataType.Decimal)
                            or (obj.DataType == TOM.DataType.Double)
                        )
                        and (str(obj.SummarizeBy) != "None")
                        and not ((obj.IsHidden) or (obj.Parent.IsHidden)),
                    "Formatting",
                    False,
                    "Column",
                    "Warning",
                    "Query Operation",
                    "Do not summarize numeric columns",
                    'Numeric columns (integer, decimal, double) should have their SummarizeBy property set to "None" to avoid accidental summation in Power BI (create measures instead).',
                    ""
                ),
                (
                    48,
                    lambda obj, tom: obj.IsHidden is False and len(obj.FormatString) == 0,
                    "Formatting",
                    False,
                    "Measure",
                    "Info",
                    "Usability",
                    "Provide format string for measures",
                    "Visible measures should have their format string property assigned.",
                    ""
                ),
                (
                    49,
                    lambda obj, tom: len(obj.DataCategory) == 0
                        and any(
                            obj.Name.lower().startswith(item.lower())
                            for item in [
                                "country",
                                "city",
                                "continent",
                                "latitude",
                                "longitude",
                            ]
                        ),
                    "Formatting",
                    False,
                    "Column",
                    "Info",
                    "Usability",
                    "Add data category for columns",
                    "Add Data Category property for appropriate columns.",
                    "https://docs.microsoft.com/power-bi/transform-model/desktop-data-categorization",
                ),
                (
                    50,
                    lambda obj, tom: "%" in obj.FormatString
                        and obj.FormatString != "#,0.0%;-#,0.0%;#,0.0%",
                    "Formatting",
                    False,
                    "Measure",
                    "Warning",
                    "Usability",
                    "Percentages should be formatted with thousands separators and 1 decimal",
                    "For a better user experience, percengage measures should be formatted with a '%' sign.",
                    ""
                ),
                (
                    51,
                    lambda obj, tom: "$" not in obj.FormatString
                        and "%" not in obj.FormatString
                        and obj.FormatString not in ["#,0", "#,0.0"],
                    "Formatting",
                    False,
                    "Measure",
                    "Warning",
                    "Usability",
                    "Whole numbers should be formatted with thousands separators and no decimals",
                    "For a better user experience, whole numbers should be formatted with commas.",
                    ""
                ),
                (
                    52,
                    lambda obj, tom: obj.IsHidden is False
                        and any(
                            r.FromColumn.Name == obj.Name
                            and r.FromCardinality == TOM.RelationshipEndCardinality.Many
                            for r in tom.used_in_relationships(object=obj)
                        ),
                    "Formatting",
                    False,
                    "Column",
                    "Info",
                    "Usability",
                    "Hide foreign keys",
                    "Foreign keys should always be hidden as they should not be used by end users.",
                    ""
                ),
                (
                    53,
                    lambda obj, tom: any(
                            r.ToTable.Name == obj.Table.Name
                            and r.ToColumn.Name == obj.Name
                            and r.ToCardinality == TOM.RelationshipEndCardinality.One
                            for r in tom.used_in_relationships(object=obj)
                        )
                        and obj.IsKey is False
                        and obj.Table.DataCategory != "Time",
                    "Formatting",
                    False,
                    "Column",
                    "Info",
                    "Usability",
                    "Mark primary keys",
                    "Set the 'Key' property to 'True' for primary key columns within the column properties.",
                    ""
                ),
                (
                    54,
                    lambda obj, tom: (re.search(r"month", obj.Name, flags=re.IGNORECASE))
                        and not (re.search(r"months", obj.Name, flags=re.IGNORECASE))
                        and (obj.DataType == TOM.DataType.String)
                        and len(str(obj.SortByColumn)) == 0,
                    "Formatting",
                    False,
                    "Column",
                    "Info",
                    "Usability",
                    "Month (as a string) must be sorted",
                    "This rule highlights month columns which are strings and are not sorted. If left unsorted, they will sort alphabetically (i.e. April, August...). Make sure to sort such columns so that they sort properly (January, February, March...).",
                    ""
                ),
                (
                    55,
                    lambda obj, tom: obj.FromColumn.DataType != TOM.DataType.Int64
                        or obj.ToColumn.DataType != TOM.DataType.Int64,
                    "Formatting",
                    False,
                    "Relationship",
                    "Warning",
                    "Query Operation",
                    "Relationship columns should be of integer data type",
                    "It is a best practice for relationship columns to be of integer data type. This applies not only to data warehousing but data modeling as well.",
                    ""
                ),
                (
                    56,
                    lambda obj, tom: re.search(r"month", obj.Name, flags=re.IGNORECASE)
                        and obj.DataType == TOM.DataType.DateTime
                        and obj.FormatString != "MMMM yyyy",
                    "Formatting",
                    False,
                    "Column",
                    "Warning",
                    "Usability",
                    'Provide format string for "Month" columns',
                    'Columns of type "DateTime" that have "Month" in their names should be formatted as "MMMM yyyy".',
                    ""
                ),
                (
                    57,
                    lambda obj, tom: obj.Name.lower().startswith("is")
                        and obj.DataType == TOM.DataType.Int64
                        and not (obj.IsHidden or obj.Parent.IsHidden)
                        or obj.Name.lower().endswith(" flag")
                        and obj.DataType != TOM.DataType.String
                        and not (obj.IsHidden or obj.Parent.IsHidden),
                    "Formatting",
                    False,
                    "Column",
                    "Info",
                    "Usability",
                    "Format flag columns as Yes/No value strings",
                    "Flags must be properly formatted as Yes/No as this is easier to read than using 0/1 integer values.",
                    ""
                ),
                (
                    58,
                    lambda obj, tom: obj.Name[0] == " " or obj.Name[-1] == " ",
                    "Formatting",
                    False,
                    ["Table", "Column", "Measure", "Partition", "Hierarchy"],
                    "Error",
                    "Usability",
                    "Objects should not start or end with a space",
                    "Objects should not start or end with a space. This usually happens by accident and is difficult to find.",
                    ""
                ),
                (
                    59,
                    lambda obj, tom: obj.Name[0] != obj.Name[0].upper(),
                    "Formatting",
                    False,
                    ["Table", "Column", "Measure", "Partition", "Hierarchy"],
                    "Info",
                    "Usability",
                    "First letter of objects must be capitalized",
                    "The first letter of object names should be capitalized to maintain professional quality.",
                    ""
                ),
                (
                    60,
                    lambda obj, tom: re.search(r"[\t\r\n]", obj.Name),
                    "Naming Conventions",
                    False,
                    ["Table", "Column", "Measure", "Partition", "Hierarchy"],
                    "Warning",
                    "Usability",
                    "Object names must not contain special characters",
                    "Object names should not include tabs, line breaks, etc.",
                    ""
                ),
            ],
        columns=[
                "RuleId",
                "Expression",
                "Category",
                "WithDependency",
                "Scope",
                "Severity",
                "ImpactArea",
                "RuleName",
                "Description",
                "URL"
            ],
    )

    return rules

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def scan_semantic_model_bpa(
    workspace: str, 
    semantic_model: str, 
    current_run: int, 
    mode: str,
    now: datetime
):
    """
    Return the results of the Best Practice Analyzer scan for a semantic model.

    Parameters
    ----------
    workspace : str
        The Fabric workspace ID.
    semantic_model : str 
        The semantic model ID.
    current_run: int
        Calculated current run of semantic model
    mode: string
        Allowed values: 'minimal', 'normal' 
    now: datetime
        Current Timestamp
    Returns
    -------
    spark.Dataframe
        Final result of BPA of the semantic model
    """

    try:
        
        # Initialize BPA violations_df
        violations_df = pd.DataFrame(columns=["ObjectName", "Scope", "RuleId"])
        check_dependencies = True
        with labs.service_principal_authentication(
                key_vault_uri=f"https://{keyvault_name}.vault.azure.net/",
                key_vault_tenant_id=keyvault_sp_tenantId_secret_name,
                key_vault_client_id=keyvault_sp_clientId_secret_name,
                key_vault_client_secret=keyvault_sp_secret_secret_name,
                ):
            workspace_name = fabric.resolve_workspace_name(workspace)

            with connect_semantic_model(
                    dataset=semantic_model,
                    workspace =workspace_name,
                    readonly=True,
                ) as tom:

                # Dependencies for more abstract analysis
                if check_dependencies:
                    dep = get_model_calc_dependencies(
                        dataset=semantic_model, workspace=workspace_name
                )
                else:
                    dep = pd.DataFrame(
                        columns=[
                            "Table Name",
                            "Object Name",
                            "Object Type",
                            "Expression",
                            "Referenced Table",
                            "Referenced Object",
                            "Referenced Object Type",
                            "Full Object Name",
                            "Referenced Full Object Name",
                            "Parent Node",
                        ]
                    )

                
                # Init BPA rules v2
                rules = model_bpa_rules_v2(dep)

                # Choose set of rules based on selected mode
                if mode == 'minimal':
                    rules = rules.loc[rules['Severity'].isin(['Warning','Error']) & (rules['WithDependency'] == False) & (rules['Category'].isin(['Performance', 'Maintenance']))]
                    check_dependencies = False


                # lightweight mode is not implemented yet
                elif mode == 'lightweight':
                    rules = rules.loc[rules['Severity'].isin(['Warning','Error']) & (rules['WithDependency'] == False)]
                    check_dependencies = False

                else:
                    # for mode = 'normal'
                    check_dependencies = True
                

                
                # Get meta data
                all_relationships = tom.model.Relationships
                all_columns = tom.all_columns()
                all_measures = tom.all_measures()
                all_hierarchies = tom.all_hierarchies()
                all_tables = tom.model.Tables
                all_roles = tom.model.Roles
                model = tom.model
                all_calculation_items = tom.all_calculation_items()
                all_rls = tom.all_rls()
                all_partitions = tom.all_partitions()
                
                # Define Scopes
                scope_to_dataframe = {
                        "Relationship": (
                            tom.model.Relationships,
                            lambda obj: create_relationship_name(
                                obj.FromTable.Name,
                                obj.FromColumn.Name,
                                obj.ToTable.Name,
                                obj.ToColumn.Name,
                            ),
                        ),
                        "Column": (
                            all_columns,
                            lambda obj: format_dax_object_name(obj.Parent.Name, obj.Name),
                        ),
                        "Measure": (
                            all_measures, lambda obj: obj.Name
                            ),
                        "Hierarchy": (
                            all_hierarchies,
                            lambda obj: format_dax_object_name(obj.Parent.Name, obj.Name),
                        ),
                        "Table": (
                            all_tables,
                            lambda obj: obj.Name
                            ),
                        "Role": (
                            all_roles, 
                            lambda obj: obj.Name
                            ),
                        "Model": (
                            model, 
                            lambda obj: obj.Model.Name
                            ),
                        "Calculation Item": (
                            all_calculation_items,
                            lambda obj: format_dax_object_name(obj.Parent.Table.Name, obj.Name),
                        ),
                        "Row Level Security": (
                            all_rls,
                            lambda obj: format_dax_object_name(obj.Parent.Name, obj.Name),
                        ),
                        "Partition": (
                            all_partitions,
                            lambda obj: format_dax_object_name(obj.Parent.Name, obj.Name),
                        ),
                    }
                
                # Iterate BPA rules
                for i, r in rules.iterrows():
                    try: 
                    
                        rule_id = r["RuleId"]
                        expr = r["Expression"]
                        scopes = r["Scope"]

                        
                        if isinstance(scopes, str):
                            scopes = [scopes]

                        for scope in scopes:
                            func = scope_to_dataframe[scope][0]
                            nm = scope_to_dataframe[scope][1]


                            if scope == "Model":
                                x = []
                                if expr(func, tom):
                                    x = ["Model"]

                            elif scope == "Measure":
                                x = [nm(obj) for obj in all_measures if expr(obj, tom)]
                            elif scope == "Column":
                                x = [nm(obj) for obj in all_columns if expr(obj, tom)]
                            elif scope == "Partition":
                                x = [nm(obj) for obj in all_partitions if expr(obj, tom)]
                            elif scope == "Hierarchy":
                                x = [nm(obj) for obj in all_hierarchies if expr(obj, tom)]
                            elif scope == "Table":
                                x = [nm(obj) for obj in all_tables if expr(obj, tom)]
                            elif scope == "Relationship":
                                x = [nm(obj) for obj in all_relationships if expr(obj, tom)]
                            elif scope == "Role":
                                x = [nm(obj) for obj in all_roles if expr(obj, tom)]
                            elif scope == "Row Level Security":
                                x = [nm(obj) for obj in all_rls if expr(obj, tom)]
                            elif scope == "Calculation Item":
                                x = [nm(obj) for obj in all_calculation_items if expr(obj, tom)]

                    

                        
                        # Add violation to violation_df if are any
                        if len(x) > 0:
                            new_data = {
                                        "ObjectName": x,
                                        "Scope": scope,
                                        "RuleId": rule_id
                                    }
                            violations_df = pd.concat(
                                        [violations_df, pd.DataFrame(new_data)], ignore_index=True
                                ) 
                    except Exception as e:
                        print(f"Rule {rule_id} got an error: {str(e).splitlines()[0] if e else ''}")  
            
            
            # Extend dataframe with workspace and semantic model id
            final_df = violations_df.copy()
            final_df["WorkspaceId"] = workspace.upper()
            final_df["SemanticModelId"] = semantic_model.upper()
            final_df["Timestamp"] = now
            final_df["RunId"] = current_run
            final_df["RunName"] = f"Run-{str(current_run)}"
            final_df["SemanticModelRunKey"] = f"{semantic_model.upper()}-{current_run}"
            
            return final_df

    except Exception as ex:
        print(ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### VertiPaq Analyzer
# 
# Defines `scan_semantic_model_vertipaq`, which collects VertiPaq storage statistics for a semantic model using `INFO.STORAGETABLECOLUMNS`, `INFO.STORAGETABLECOLUMNSEGMENTS`, and `INFO.STORAGETABLES`. Metrics are aggregated at the **model** level (always) and at the **table** and **relationship** level (when `analyzer_mode != 'minimal'`); column-level metrics are intentionally not collected.
# </newCode>
# </invoke>

# CELL ********************

def scan_semantic_model_vertipaq(
    workspace: str, 
    semantic_model: str, 
    current_run: int, 
    mode: str,
    now: datetime
    ):

    # Optimize pd df copy
    pd.options.mode.copy_on_write = True
    warnings.filterwarnings(
        "ignore", message="createDataFrame attempted Arrow optimization*"
    )

    try:


        with labs.service_principal_authentication(
                key_vault_uri=f"https://{keyvault_name}.vault.azure.net/",
                key_vault_tenant_id=keyvault_sp_tenantId_secret_name,
                key_vault_client_id=keyvault_sp_clientId_secret_name,
                key_vault_client_secret=keyvault_sp_secret_secret_name,
                ):
            workspace_name = fabric.resolve_workspace_name(workspace)

            with connect_semantic_model(
                    dataset=semantic_model,
                    workspace =workspace_name,
                    readonly=True,
                ) as tom:

                # Get meta data
                compat_level = tom.model.Model.Database.CompatibilityLevel
                table_count = tom.model.Tables.Count
            

            # Get Table Statistics
            dict_df = execute_dax_to_pandas(
                    workspace,
                    semantic_model,
                    """
                            EVALUATE
                            SELECTCOLUMNS(
                                FILTER(INFO.STORAGETABLECOLUMNS(), [COLUMN_TYPE] = "BASIC_DATA"),
                                [DIMENSION_NAME],
                                [DICTIONARY_SIZE]
                            )
                    """,
                    )

            # Fix: use dict_df, not undefined 'df'
            dict_df["[DICTIONARY_SIZE]"] = dict_df["[DICTIONARY_SIZE]"].astype(int)
            dict_sum = dict_df.groupby("[DIMENSION_NAME]", observed=True)["[DICTIONARY_SIZE]"].sum()

            data = execute_dax_to_pandas(
                            workspace,
                            semantic_model,
                            """
                            EVALUATE
                            SELECTCOLUMNS(
                                INFO.STORAGETABLECOLUMNSEGMENTS(),
                                [TABLE_ID],
                                [DIMENSION_NAME],
                                [USED_SIZE]
                            )
                            """,
                        )

            data["[USED_SIZE]"] = data["[USED_SIZE]"].astype(int)
            data_sum = (
                        data[
                            ~data["[TABLE_ID]"].str.startswith("R$")
                            & ~data["[TABLE_ID]"].str.startswith("U$")
                            & ~data["[TABLE_ID]"].str.startswith("H$")
                        ]
                        .groupby("[DIMENSION_NAME]", observed=True)["[USED_SIZE]"]
                        .sum()
                    )
            hier_sum = (
                        data[data["[TABLE_ID]"].str.startswith("H$")]
                        .groupby("[DIMENSION_NAME]", observed=True)["[USED_SIZE]"]
                        .sum()
                    )
            rel_sum = (
                        data[data["[TABLE_ID]"].str.startswith("R$")]
                        .groupby("[DIMENSION_NAME]", observed=True)["[USED_SIZE]"]
                        .sum()
                    )
            uh_sum = (
                        data[data["[TABLE_ID]"].str.startswith("U$")]
                        .groupby("[DIMENSION_NAME]", observed=True)["[USED_SIZE]"]
                        .sum()
                    )
            rc = execute_dax_to_pandas(
                            workspace,
                            semantic_model,
                            """
                                EVALUATE
                                SELECTCOLUMNS(
                                    FILTER(
                                        INFO.STORAGETABLES(),
                                        MID([TABLE_ID], 2, 1) <> "$"
                                    ),
                                    "DIMENSION_NAME", [DIMENSION_NAME],
                                    "ROWS_COUNT",     [ROWS_COUNT]
                                )
                            """,
                        )
                    

                    
                    
            # Calculate semantic model sizes
            model_dict_sum = dict_sum.sum()
            model_data_sum = data_sum.sum()
            model_hier_sum = hier_sum.sum()
            model_rel_sum = rel_sum.sum()
            model_uh_sum = uh_sum.sum()

            model_size = (
                            model_dict_sum
                            + model_data_sum
                            + model_hier_sum
                            + model_rel_sum
                            + model_uh_sum
                    )
                

       
        
            with connect_semantic_model(
                            dataset=semantic_model,
                            workspace =workspace_name,
                            readonly=True,
                ) as tom:
            
                if mode != 'minimal':
                    # Calculate table statistics
                    rows = []
                    for t in tom.model.Tables:
                        t_name = t.Name
                        t_type = (
                                "Calculation Group"
                                if t.CalculationGroup
                                else (
                                    "Calculated Table"
                                    if tom.is_calculated_table(table_name=t.Name)
                                    else "Table"
                                    )
                                )
                        ref = bool(t.RefreshPolicy)
                        ref_se = t.RefreshPolicy.SourceExpression if ref else None

                        new_table_data = {
                                            "Name": t_name,
                                            "Description": t.Description,
                                            "Hidden": t.IsHidden,
                                            "DataCategory": t.DataCategory,
                                            "Type": t_type,
                                            "RefreshPolicy": ref,
                                            "SourceExpression": ref_se,
                                    }

                        dict_size = dict_sum.get(t_name, 0)
                        data_size = data_sum.get(t_name, 0)
                        h_size = hier_sum.get(t_name, 0)
                        r_size = rel_sum.get(t_name, 0)
                        u_size = uh_sum.get(t_name, 0)
                        total_size = data_size + dict_size + h_size + r_size + u_size
                                    
                        new_table_data.update(
                                                {
                                                    "RowCount": (
                                                        rc[rc["[DIMENSION_NAME]"] == t_name]["[ROWS_COUNT]"].iloc[0]
                                                        if not rc.empty
                                                        else 0
                                                    ),
                                                    "TotalSize": total_size,
                                                    "DictionarySize": dict_size,
                                                    "DataSize": data_size,
                                                    "HierarchySize": h_size,
                                                    "RelationshipSize": r_size,
                                                    "UserHierarchySize": u_size,
                                                    "Partitions": int(len(t.Partitions)),
                                                    "Columns": sum(
                                                        1 for c in t.Columns if str(c.Type) != "RowNumber"
                                                    ),
                                                    "PercentageOfDB": round((total_size / model_size) * 100, 2),
                                                }
                                    )

                        rows.append(new_table_data)
                        # End vertipaq table statistics logic 
                                
                    # Tables
                    all_vp_tables_df = pd.DataFrame(rows)
                    all_vp_tables_df['SemanticModelId'] = semantic_model.upper()
                    all_vp_tables_df['WorkspaceId'] = workspace.upper()
                    all_vp_tables_df["RunId"] = current_run
                    all_vp_tables_df["SemanticModelRunKey"] = f"{semantic_model.upper()}-{current_run}"


                    # Get Relationship Statistics
                    # This list_relationship is from sempy, not from semantic-link-labs
                    all_vp_relationships_df = list_relationships(dataset=semantic_model, extended=True, workspace=workspace)

                    # Relationships
                    all_vp_relationships_df['SemanticModelId'] = semantic_model.upper()
                    all_vp_relationships_df['WorkspaceId'] = workspace.upper()
                    all_vp_relationships_df["RunId"] = current_run
                    all_vp_relationships_df["SemanticModelRunKey"] = f"{semantic_model.upper()}-{current_run}"
                    all_vp_relationships_df.rename(columns={"From Table": "FromTable", "From Column": "FromColumn", "To Table": "ToTable", "To Column": "ToColumn", "Security Filtering Behavior": "SecurityFilteringBehavior", "Cross Filtering Behavior": "CrossFilteringBehavior", "Join On Date Behavior": "JoinOnDateBehavior", "Rely On Referential Integrity": "RelyOnReferentialIntegrity", "Modified Time": "ModifiedTime", "Relationship Name": "RelationshipName", "From Object": "FromObject", "To Object": "ToObject", "Used Size": "UsedSize"}, inplace=True)


                else: 
                    all_vp_tables_df = None
                    all_vp_relationships_df = None
                            
                        # Get Model Statistics
                semantic_model_vp_df = pd.DataFrame(
                                [
                                    (
                                        workspace.upper(),
                                        semantic_model.upper(),
                                        now,
                                        current_run,
                                        f"Run-{str(current_run)}",
                                        f"{semantic_model.upper()}-{current_run}",
                                        mode,
                                        compat_level,
                                        table_count,
                                        model_size,
                                        model_dict_sum,
                                        model_data_sum,
                                        model_hier_sum,
                                        model_rel_sum,
                                        model_uh_sum
                                    )

                                ],
                columns=[
                                        "WorkspaceId",
                                        "SemanticModelId",
                                        "RunTimestamp",
                                        "RunId",
                                        "RunName",
                                        "SemanticModelRunKey",
                                        "AnalyzerMode",
                                        "CompatibilityLevel",
                                        "TableCount",
                                        "TotalSize",
                                        "DictionarySize",
                                        "DataSize",
                                        "HierarchySize",
                                        "RelationshipSize",
                                        "UserHierarchySize"
                                    ],
                    )


        return {'model': semantic_model_vp_df, 'tables': all_vp_tables_df, 'relationships': all_vp_relationships_df}

    except Exception as ex:
        print(f"Exception: {ex}")
        return {'model': None, 'tables': None, 'relationships': None}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Semantic Model Unified Analyzer (BPA & VertiPaq)
# 
# Defines `run_semantic_model_analyzer_e2e`, the end-to-end orchestrator for a single semantic model. It determines the next `RunId`, executes the VertiPaq and BPA scans, and appends the results to the FUAM Lakehouse Delta tables (`semantic_model_analyzer_runs`, `semantic_model_bpa_results`, `semantic_model_vertipaq_tables`, `semantic_model_vertipaq_relationships`).
# </newCode>
# </invoke>

# CELL ********************

def run_semantic_model_analyzer_e2e(workspace: str, semantic_model: str, mode: str):
    # Validate mode
    if mode == 'minimal':
        mode == 'minimal'

    # lightweight is not implemented yet
    elif mode == 'lightweight':
        mode == 'lightweight'

    else:
        mode == 'normal'

    # Get current timestamp
    now = datetime.now()

    # Get Total runs of semantic model
    # initial run value
    current_run = 1
    # check older runs, if any set current_run to new value
    try:
        runs_query = f"SELECT COUNT(*) AS total_runs FROM FUAM_Lakehouse.semantic_model_analyzer_runs WHERE SemanticModelId = '{semantic_model.upper()}' GROUP BY SemanticModelId"
        runs_df = spark.sql(runs_query)
        total_run = runs_df.select('total_runs').collect()[0]["total_runs"]
        current_run = total_run + 1
    except:
        print("First run for this semantic model or other expections.")

    
    # Run VertiPaq Analyzer
    print("Start Vertipaq Analyzer")
    scan_vertipaq_results = scan_semantic_model_vertipaq(workspace=workspace, semantic_model=semantic_model, current_run=current_run, mode=mode, now=now)
    print("Vertipaq Analyzer finished")
    # Run BPA
    print("Start BPA Analyzer")
    sm_bpa_run_result_df = scan_semantic_model_bpa(workspace=workspace, semantic_model=semantic_model, current_run=current_run, mode=mode, now=now)
    print("BPA Analyzer finished")

    # Log current run
    sm_analyzer_run_df = pd.DataFrame(
        [
            (
                f"{semantic_model.upper()}-{current_run}"
            )

        ],
        columns=[
                "SemanticModelRunKey"
            ],
    )

    # Merge Run head row from BPA and VertiPaq Analyzer
    sm_analyzer_run_df = pd.merge(sm_analyzer_run_df, scan_vertipaq_results['model'], on="SemanticModelRunKey")

    # Write Analyzer run log to Lakehouse table
    sm_run_spark_df = spark.createDataFrame(sm_analyzer_run_df)
    sm_run_spark_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable('semantic_model_analyzer_runs')

    
    # Write Run Results to Lakehouse table
    sm_bpa_run_result_spark_df = spark.createDataFrame(sm_bpa_run_result_df)
    sm_bpa_run_result_spark_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable('semantic_model_bpa_results')

    if mode != 'minimal':
        # Write VertiPaq Table Run Results to Lakehouse table
        sm_vp_run_table_result_spark_df = spark.createDataFrame(scan_vertipaq_results['tables'])
        sm_vp_run_table_result_spark_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable('semantic_model_vertipaq_tables')

        # Write VertiPaq Relationship Run Results to Lakehouse table
        sm_vp_run_relationship_result_spark_df = spark.createDataFrame(scan_vertipaq_results['relationships'])
        sm_vp_run_relationship_result_spark_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable('semantic_model_vertipaq_relationships')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Run all
# 
# Selects the **top _n_ semantic models per capacity** by CU consumption for the previous day (from `capacity_metrics_by_item_by_operation_by_day`, filtered to import / composite-mode models in `semantic_models`) and iterates over the resulting set. For each model the SPN's workspace access is verified (and granted on demand), then the unified analyzer is executed. Errors are caught per model so a single failure does not stop the overall run.
# </newCode>
# </invoke>

# CELL ********************

# Get date from yesterday
yesterday = pd.Timestamp.now() - pd.Timedelta(days=1)
formatted_date = yesterday.strftime('%Y-%m-%d')
#formatted_date = "2026-05-29"
print(f"INFO: Scoped day is {formatted_date} ")

# Get Max of TotalCU from last week for semantic models (datasets)
# filtered for TopN semantic models
sql_top_n_items_by_cu = f'''
SELECT
    *
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (Partition By CapacityId Order by TotalCUs DESC) AS RowNumber
    FROM (
        SELECT
            cap.CapacityId,
            cap.WorkspaceId,
            cap.ItemId,
            --Date,
            SUM(cap.TotalCUs) AS TotalCUs
        FROM FUAM_Lakehouse.capacity_metrics_by_item_by_operation_by_day cap
        INNER JOIN FUAM_Lakehouse.semantic_models sm ON ( sm.WorkspaceId = cap.WorkspaceId AND sm.SemanticModelId = cap.ItemId )
        WHERE
            sm.ContentProviderType IN ('PbixInCompositeMode', 'PbixInImportMode')
            AND Date = '{formatted_date}'
        GROUP BY 
            cap.CapacityId,
            cap.WorkspaceId,
            cap.ItemId
    ) agg
) n
WHERE RowNumber <= {top_n_semantic_models_per_capacity}
'''

# Fetch Lakehouse data
df_top_n_items_by_cu = spark.sql(sql_top_n_items_by_cu)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# For debug
if display_data:
    display(df_top_n_items_by_cu)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

columns_to_extract = ['WorkspaceId', 'ItemId']

# Iterate over each row in the DataFrame
for row in df_top_n_items_by_cu.select(columns_to_extract).collect():

    try:
    
        # Change string to lower case
        workspace_id = row['WorkspaceId'].lower()
        item_id = row['ItemId'].lower()
        #scoped_date = row['Date']

        # Print current item
        if display_data:
            print(f"INFO: Analysing Semantic Model: {item_id} in Workspace: {workspace_id}")


        # Main calls
        # Call unified Semantic Model Analyzer
        
        try:
                with labs.service_principal_authentication(
                    key_vault_uri=f"https://{keyvault_name}.vault.azure.net/",
                    key_vault_tenant_id=keyvault_sp_tenantId_secret_name,
                    key_vault_client_id=keyvault_sp_clientId_secret_name,
                    key_vault_client_secret=keyvault_sp_secret_secret_name,
                ):
                    execute_dax_to_pandas(workspace_id = workspace_id, dataset_id = item_id , query = """ EVALUATE {{1}}""")
        
        except Exception as ex:
            print("WARNING: SPN doesn't have access to workspace. This notebook grants Member access of the SPN and will log this action in the FUAM_Lakehouse audit table.")
            add_member_to_workspace(analyzer_entity=enterprise_app_object_id, workspace_id=workspace_id, analyzer_principal_type="app")
                
        run_semantic_model_analyzer_e2e(workspace = workspace_id, semantic_model = item_id, mode = analyzer_mode)

    except Exception as ex:
        print(f"Error with item {item_id} in workspace {workspace_id}")

        
        
    

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
