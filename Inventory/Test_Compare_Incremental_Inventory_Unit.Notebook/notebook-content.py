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
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Regression test: original vs candidate inventory notebook
# 
# Reference-runs both notebooks with `notebookutils.notebook.run` and compares the tables they write.
# 
# 1. **Cleanup** - drops the target tables in the **candidate** lakehouse only. The original lakehouse is never modified by this notebook.
# 2. **Snapshot** - records the latest Delta version of every table, so we can tell whether this run actually wrote it.
# 3. **Run** - runs the original notebook, then the candidate.
# 4. **Compare** - per table: written by this run, columns and types, row counts, rows present on one side only.
# 
# **Lakehouse setup:** reference runs share this notebook's Spark session and default lakehouse. This notebook's default lakehouse must be the **original** lakehouse, so the original notebook runs unchanged. The candidate is run with `useRootDefaultLakehouse: True` and `target_lakehouse = <candidate lakehouse>`, which redirects its tables and files. After the candidate run the test checks that it didn't change any table in the original lakehouse.
# 
# Don't edit or save notebooks in this workspace while the test runs - the scan includes this workspace, so the `notebooks` table would differ between the two runs.


# PARAMETERS CELL ********************

original_notebook = "01_Transfer_Incremental_Inventory_Unit"
candidate_notebook = "01_Transfer_Incremental_Inventory_Unit_v2"
original_lakehouse = "FUAM_Lakehouse"
candidate_lakehouse = "FUAM_Lakehouse_new"

run_notebooks = True        # False = only compare the tables that already exist
cleanup_candidate = True    # drop the target tables in the candidate lakehouse before the runs
save_results = True         # write the summary to <candidate lakehouse>/Files/comparison_results/

run_timeout_minutes = 90    # per notebook run
max_diff_rows = 20

# List columns whose element order differs between runs - their elements are sorted before comparing (any table)
unordered_list_columns = ["relations"]

# Columns left out of the comparison, per table (e.g. values that change between the two runs)
exclude_columns = {"notebooks": ["LastUpdatedDateTime"]}

# Rows left out of the comparison, per table: {table: {column: [values]}}
# This test notebook is modified by its own execution, so its row differs between the runs
exclude_rows = {"notebooks": {"Name": ["Test_Compare_Incremental_Inventory_Unit"]}}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import ast
import json
import datetime
import pandas as pd
import sempy.fabric as fabric
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# Tables written by 01_Transfer_Incremental_Inventory_Unit
TARGET_TABLES = [
    "workspaces_scanned_users", "datamarts", "semantic_models", "semantic_model_datasources",
    "dataflows", "dataflow_datasources", "reports", "report_datasources", "dashboards",
    "eventhouses", "kql_databases", "lakehouses", "warehouses", "eventstreams", "pipelines",
    "notebooks", "environments", "reflexes", "ml_models", "datasource_instances",
    "misconfigured_datasource_instances", "item_users",
]

assert original_lakehouse != candidate_lakehouse, "Original and candidate lakehouse must be different"

# The original notebook is reference-run in this session, so it writes to this notebook's default lakehouse
runtime_context = notebookutils.runtime.context
current_default = runtime_context.get("defaultLakehouseName")
assert current_default == original_lakehouse, f"Set {original_lakehouse} as this notebook's default lakehouse (current: {current_default}, context keys: {sorted(runtime_context.keys())})"

workspace_id = fabric.get_workspace_id()

lakehouse_ids = {
    "original": notebookutils.lakehouse.get(original_lakehouse).id,
    "candidate": notebookutils.lakehouse.get(candidate_lakehouse).id,
}
roots = {side: f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lh_id}" for side, lh_id in lakehouse_ids.items()}


def table_path(side, table):
    # Schema-enabled lakehouses keep tables under Tables/dbo
    for path in (f"{roots[side]}/Tables/{table}", f"{roots[side]}/Tables/dbo/{table}"):
        if DeltaTable.isDeltaTable(spark, path):
            return path
    return None


def list_tables(side):
    names = set()
    for folder in (f"{roots[side]}/Tables", f"{roots[side]}/Tables/dbo"):
        if notebookutils.fs.exists(folder):
            names |= {f.name.rstrip("/") for f in notebookutils.fs.ls(folder) if f.isDir}
    names.discard("dbo")
    return names


def latest_version(path):
    if path is None:
        return None
    return DeltaTable.forPath(spark, path).history(1).first()["version"]


print(json.dumps(lakehouse_ids, indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 1. Cleanup candidate lakehouse

# CELL ********************

if cleanup_candidate and run_notebooks:
    for table in TARGET_TABLES:
        path = table_path("candidate", table)
        if path:
            notebookutils.fs.rm(path, True)
            print(f"Dropped {candidate_lakehouse}.{table}")
    print("Cleanup done")
else:
    print("Cleanup skipped")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 2. Snapshot table versions

# CELL ********************

snapshot_tables = set(TARGET_TABLES) | list_tables("candidate")
versions_before = {side: {t: latest_version(table_path(side, t)) for t in snapshot_tables} for side in roots}

display(pd.DataFrame(versions_before).sort_index())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 3. Run both notebooks (one after the other)

# CELL ********************

def run_notebook(notebook_name, arguments=None):
    print(f"{datetime.datetime.now():%H:%M:%S} Running {notebook_name}")
    notebookutils.notebook.run(notebook_name, run_timeout_minutes * 60, arguments or {})
    print(f"{datetime.datetime.now():%H:%M:%S} Finished {notebook_name}")


def original_side_state():
    tables = list_tables("original")
    return {t: latest_version(table_path("original", t)) for t in tables}


if run_notebooks:
    run_notebook(original_notebook)

    original_state = original_side_state()
    run_notebook(candidate_notebook, {
        "useRootDefaultLakehouse": True,
        "target_lakehouse": candidate_lakehouse,
    })

    # Any write the candidate didn't redirect lands in the original lakehouse - fail loudly
    changed = sorted(t for t, v in original_side_state().items() if original_state.get(t) != v)
    if changed:
        raise RuntimeError(f"{candidate_notebook} changed tables in {original_lakehouse}: {', '.join(changed)}")
else:
    print("Notebook runs skipped - comparing the existing tables")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 4. Compare tables
# 
# Status values:
# - **MATCH** - same columns, types and rows (row order is ignored)
# - **DIFF** - see the column and row details
# - **NOT_WRITTEN_BY_EITHER** - neither run wrote the table (e.g. no data of that type, or the existing `dataflow_datasources` bug)
# - **WRITTEN_BY_ONE_ONLY** - only one run wrote the table
# - **MISSING_ORIGINAL / MISSING_CANDIDATE / MISSING_BOTH** - table doesn't exist (compare-only mode)
# 
# Columns that exist on one side only and are entirely null are listed as *stale*: `mergeSchema` keeps columns from older runs in existing tables. They don't count as a difference.
# 
# Columns in `unordered_list_columns` (e.g. `relations`) hold lists whose element order changes between runs. Their elements are sorted before comparing, so only content differences count.
# 
# `exclude_columns` and `exclude_rows` remove columns and rows from both sides before comparing (e.g. `notebooks.LastUpdatedDateTime` and the row of this test notebook, which its own execution modifies).

# CELL ********************

unordered = {c.lower() for c in unordered_list_columns}


@F.udf("string")
def sort_list(value):
    # Lists are stored as JSON or as a Python literal, e.g. "[{'dependentOnArtifactId': ..., 'relationType': ...}, ...]"
    if value is None:
        return None
    try:
        items = json.loads(value)
    except ValueError:
        try:
            items = ast.literal_eval(value)
        except Exception:
            return value
    if not isinstance(items, list):
        return value
    return "[" + ", ".join(sorted(json.dumps(i, sort_keys=True, default=str) for i in items)) + "]"


def all_null(df, column):
    return df.filter(F.col(f"`{column}`").isNotNull()).limit(1).count() == 0


def apply_exclusions(df, table):
    # Column names are matched case-insensitively, like the rest of the comparison
    names = {c.lower(): c for c in df.columns}
    for column, values in exclude_rows.get(table, {}).items():
        if column.lower() in names:
            col = F.col(f"`{names[column.lower()]}`")
            df = df.filter(col.isNull() | ~col.isin(values))
    drop = [names[c.lower()] for c in exclude_columns.get(table, []) if c.lower() in names]
    return df.drop(*drop)


def compare_table(table):
    result = {"table": table}
    paths = {side: table_path(side, table) for side in roots}

    if run_notebooks:
        written = {}
        for side in roots:
            before = versions_before[side].get(table)
            after = latest_version(paths[side])
            written[side] = after is not None and (before is None or after > before)
        result["written_original"] = written["original"]
        result["written_candidate"] = written["candidate"]
        if not written["original"] and not written["candidate"]:
            result["status"] = "NOT_WRITTEN_BY_EITHER"
            return result, None
        if written["original"] != written["candidate"]:
            result["status"] = "WRITTEN_BY_ONE_ONLY"
            return result, None

    if paths["original"] is None or paths["candidate"] is None:
        if paths["original"] is None and paths["candidate"] is None:
            result["status"] = "MISSING_BOTH"
        else:
            result["status"] = "MISSING_ORIGINAL" if paths["original"] is None else "MISSING_CANDIDATE"
        return result, None

    df_o = spark.read.format("delta").load(paths["original"])
    df_c = spark.read.format("delta").load(paths["candidate"])
    df_o, df_c = apply_exclusions(df_o, table), apply_exclusions(df_c, table)
    types_o, types_c = dict(df_o.dtypes), dict(df_c.dtypes)
    # Delta column names are case-insensitive, so older tables can keep a different casing (e.g. FUAM_ConnectionString)
    names_o = {c.lower(): c for c in df_o.columns}
    names_c = {c.lower(): c for c in df_c.columns}
    common = [c for c in df_o.columns if c.lower() in names_c]
    cand_name = {c: names_c[c.lower()] for c in common}
    only_o = [c for c in df_o.columns if c.lower() not in names_c]
    only_c = [c for c in df_c.columns if c.lower() not in names_o]

    stale_o = [c for c in only_o if all_null(df_o, c)]
    stale_c = [c for c in only_c if all_null(df_c, c)]
    only_o = [c for c in only_o if c not in stale_o]
    only_c = [c for c in only_c if c not in stale_c]
    type_diffs = [f"{c}: {types_o[c]} -> {types_c[cand_name[c]]}" for c in common if types_o[c] != types_c[cand_name[c]]]

    # Compare as strings so type differences (reported above) don't hide value differences.
    # Dots in flattened column names (e.g. sensitivityLabel.labelId) are replaced, otherwise Spark reads them as struct fields
    safe = {c: c.replace(".", "__") for c in common}

    def as_strings(df, names):
        dtypes = dict(df.dtypes)

        def to_string(c):
            col = F.col(f"`{names[c]}`")
            if c.lower() not in unordered:
                return col.cast("string")
            return sort_list(F.to_json(col) if dtypes[names[c]].startswith("array") else col.cast("string"))

        return df.select([to_string(c).alias(safe[c]) for c in common])

    s_o, s_c = as_strings(df_o, {c: c for c in common}), as_strings(df_c, cand_name)
    only_in_o = s_o.exceptAll(s_c)
    only_in_c = s_c.exceptAll(s_o)
    n_only_o, n_only_c = only_in_o.count(), only_in_c.count()

    # Columns whose values differ between the two tables - shows where the row differences come from
    diff_cols = []
    if n_only_o or n_only_c:
        for c in common:
            col_o, col_c = s_o.select(safe[c]), s_c.select(safe[c])
            if col_o.exceptAll(col_c).limit(1).count() or col_c.exceptAll(col_o).limit(1).count():
                diff_cols.append(c)

    result.update({
        "rows_original": df_o.count(),
        "rows_candidate": df_c.count(),
        "rows_only_original": n_only_o,
        "rows_only_candidate": n_only_c,
        "columns_with_value_diffs": ", ".join(diff_cols),
        "cols_only_original": ", ".join(only_o),
        "cols_only_candidate": ", ".join(only_c),
        "type_differences": "; ".join(type_diffs),
        "stale_null_cols": ", ".join([f"original.{c}" for c in stale_o] + [f"candidate.{c}" for c in stale_c]),
    })
    identical = not (only_o or only_c or type_diffs or n_only_o or n_only_c)
    result["status"] = "MATCH" if identical else "DIFF"

    diff_rows = None
    if n_only_o or n_only_c:
        diff_rows = (only_in_o.withColumn("_side", F.lit("original_only"))
                     .unionByName(only_in_c.withColumn("_side", F.lit("candidate_only")))
                     .limit(max_diff_rows))
    return result, diff_rows


# Also compare tables the candidate adds beyond the original list
tables_to_compare = TARGET_TABLES + sorted(list_tables("candidate") - set(TARGET_TABLES))

results, diffs = [], {}
for table in tables_to_compare:
    result, diff_rows = compare_table(table)
    results.append(result)
    if diff_rows is not None:
        diffs[table] = diff_rows

df_summary = pd.DataFrame(results)
display(df_summary)

# Plain-text details of the failing tables (easy to copy from the output)
for r in results:
    if r["status"] not in ("MATCH", "NOT_WRITTEN_BY_EITHER", "MISSING_BOTH"):
        print(json.dumps(r, indent=2, default=str))

PASS_STATES = {"MATCH", "NOT_WRITTEN_BY_EITHER", "MISSING_BOTH"}
failed = df_summary[~df_summary["status"].isin(PASS_STATES)]
print("RESULT:", "PASS" if failed.empty else f"FAIL - {len(failed)} table(s): {', '.join(failed['table'])}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Rows that exist on one side only (_side column shows which)
for table, diff_rows in diffs.items():
    print(f"{table} - up to {max_diff_rows} differing rows")
    # display() resolves columns by name, so dots in flattened names (e.g. sensitivityLabel.labelId) would be read as struct fields
    display(diff_rows.toDF(*[c.replace(".", "__") for c in diff_rows.columns]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if save_results:
    result_path = f"{roots['candidate']}/Files/comparison_results/{datetime.datetime.now():%Y%m%d_%H%M%S}.json"
    notebookutils.fs.put(result_path, df_summary.to_json(orient="records", indent=2), True)
    print(f"Summary saved to {result_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
