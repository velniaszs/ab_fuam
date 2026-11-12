# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### FUAM Table Initialization
# 
# This notebook initializes all necessary tables for FUAM. It should be run after deployment


# CELL ********************

# MAGIC %%configure -f
# MAGIC 
# MAGIC { 
# MAGIC         "defaultLakehouse": { 
# MAGIC             "name":  "FUAM_Lakehouse"
# MAGIC             }
# MAGIC     }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install semantic-link-labs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import explode, sequence
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import pandas as pd  
import sempy_labs as labs
from pyspark.sql import functions as F



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Definition of static tables

# CELL ********************

# Define the data as a dictionary
capacity_regions = pd.DataFrame(
    [
    ("Asia Pacific", "Australia East", 31.2532, 146.9211, "New South Wales"),
    ("Asia Pacific", "Australia Southeast", 36.9848, 143.3906, "Victoria"),
    ("Asia Pacific", "Central India", 18.5204, 73.8567, "Pune"),
    ("Asia Pacific", "East Asia", 22.3193, 114.1694, "Hong Kong"),
    ("Asia Pacific", "Japan East", 35.6764, 139.65, "Tokyo"),
    ("Asia Pacific", "Korea Central", 37.5519, 126.9918, "Seoul"),
    ("Asia Pacific", "Southeast Asia", 1.3521, 103.8198, "Singapore"),
    ("Asia Pacific", "South India", 13.0827, 80.2707, "Chennai"),
    ("Europe", "North Europe", 53.7798, 7.3055, "Ireland"),
    ("Europe", "West Europe", 52.1326, 5.2913, "Netherlands"),
    ("Europe", "France Central", 48.8566, 2.3522, "Paris"),
    ("Europe", "Germany West Central", 50.1109, 8.6821, "Frankfurt am Main"),
    ("Europe", "Norway East", 59.9139, 10.7522, "Oslo"),
    ("Europe", "Sweden Central", 60.6749, 17.1413, "Gävle"),
    ("Europe", "Switzerland North", 47.3769, 8.5417, "Zürich"),
    ("Europe", "Switzerland West", 46.2044, 6.1432, "Geneva"),
    ("Europe", "UK South", 51.5072, -0.1276, "London"),
    ("Europe", "UK West", 51.4837, -3.1681, "Cardiff"),
    ("Americas", "Brazil South", -23.5558, -46.6396, "São Paulo State"),
    ("Americas", "Canada Central", 43.6532, -79.3832, "Toronto"),
    ("Americas", "Canada East", 46.8131, -71.2075, "Quebec City"),
    ("Americas", "East US", 37.4316, -78.6569, "Virginia"),
    ("Americas", "East US 2", 37.4316, -78.6569, "Virginia"),
    ("Americas", "North Central US", 40.6331, -89.3985, "Illinois"),
    ("Americas", "South Central US", 31.9686, -99.9018, "Texas"),
    ("Americas", "West US", 36.7783, -119.4179, "California"),
    ("Americas", "West US 2", 47.7511, -120.7401, "Washington"),
    ("Americas", "West US 3", 34.0489, -111.0937, "Arizona"),
    ("Middle East and Africa", "South Africa North", -26.2056, 28.0337, "Johannesburg"),
    ("Middle East and Africa", "UAE North", 25.2048, 55.2708, "Dubai")
],
    columns=[
        "Continent",
        "FabricRegion",
        "Latitude",
        "Longitude",	
        "Location"
    ]
)

# Create a DataFrame
capacity_regions_df = pd.DataFrame(capacity_regions)

# Write Capacity regions to Lakehouse table
fc_convert_dict = {'Continent': str, 'FabricRegion': str, 'Latitude': str, 'Longitude': str, 'Location': str}
rules_catalog_df = capacity_regions_df.astype(fc_convert_dict)
fc_spark_df = spark.createDataFrame(capacity_regions_df)

fc_spark_df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("capacity_regions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add capacity regions static table
sm_content_provider_types_df = pd.DataFrame(
    [
    ("InCompositeMode",             "Composite Mode",           False, True),
    ("InDirectQueryMode",           "DirectQuery Mode",         False, True),
    ("InImportMode",                "Import Mode",              False, True),
    ("PbixInCompositeMode",         "Composite Mode",           True,  False),
    ("PbixInDirectQueryMode",       "DirectQuery Mode",         True,  False),
    ("PbixInImportMode",            "Import Mode",              True,  False),
    ("PbixInLiveConnectionMode",    "Live Connection Mode",     False,  False),
    ("RealTimeInPushMode",          "Push Mode",                False, False),
    ("RealTimeInStreamingMode",     "Streaming Mode",           False, False),
    ("Unknown",                     "Semantic Model in App",    False, False),
    ("UsageMetricsUserReport",      "Usage Metrics",            False, False),
    ("UsageMetricsUserDashboard",   "Usage Metrics",            False, False),
    ("CSV",                         "CSV",                      False, False),
    ("Excel",                       "Excel",                    False, False),
],
    columns=[
        "ContentProviderType",
        "MappedName",
        "IsIncludedInOptimizationModule",
        "IsDefaultSemanticModel"
    ]
)

# Write Calendar Hours to Lakehouse table
fc_convert_dict = {'ContentProviderType': str, 'MappedName': str, 'IsIncludedInOptimizationModule': bool, "IsDefaultSemanticModel": bool}
sm_content_provider_types_df = sm_content_provider_types_df.astype(fc_convert_dict)
fc_spark_df = spark.createDataFrame(sm_content_provider_types_df)

fc_spark_df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("semantic_model_content_types")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add BPA rules (configured for FUAM) static table
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

rules_catalog_df = rules.drop(columns=['Expression'])

# Write BPA Rules to Lakehouse table
convert_dict = {'RuleId': int, 'Category': str, 'WithDependency': bool, 'Scope': str, 'Severity': str, 'ImpactArea': str, 'RuleName': str, 'Description': str, 'URL': str,}
rules_catalog_df = rules_catalog_df.astype(convert_dict)
spark_df = spark.createDataFrame(rules_catalog_df)
#display(spark_df)
spark_df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("semantic_model_bpa_rule_catalog")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Generate Standard Tables from Table Definitions
# This part automatically deploys missing tables or columns to the FUAM_Lakehouse

# CELL ********************

if spark.catalog.tableExists("FUAM_Config_Lakehouse.FUAM_Table_Definitions"):
    print("Table FUAM_Config_Lakehouse.FUAM_Table_Definitions already created")
else:
    print("Table FUAM_Config_Lakehouse.FUAM_Table_Definitions needs to be created")
    notebookutils.lakehouse.loadTable(
    {
        "relativePath": f"Files/table_definitions/table_definitions.snappy.parquet",
        "pathType": "File",
        "mode": "Overwrite",
        "recursive": False,
        "formatOptions": {
            "format": "Parquet"
        }
    }, "FUAM_Table_Definitions", "FUAM_Config_Lakehouse")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time



i = 0 
while i < 5:
    try:
        df_tables = spark.sql("""
        SELECT DISTINCT Table_Name FROM FUAM_Config_Lakehouse.FUAM_Table_Definitions
        """)
        df_columns = spark.sql("""
        SELECT DISTINCT * FROM FUAM_Config_Lakehouse.FUAM_Table_Definitions
        """).toPandas()
        break
    except:
        i = i + 1
        time.sleep(60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_definitions =[]
tables = [data[0] for data in df_tables.select('Table_Name').collect()]

for table_name in tables:
    print(table_name)
    table_definition = {}
    table_definition['table'] = table_name

    create_stmnt = "CREATE TABLE IF NOT EXISTS FUAM_Lakehouse." + table_name + '('

    table_cols = df_columns[df_columns["Table_Name"] == table_name]
    table_cols = table_cols[["Table_Name", "Column_Name", "Data_Type"]]
    table_cols
    for index, col in table_cols.iterrows():
        if 'struct' not in col['Data_Type']:
            create_stmnt = create_stmnt + '`' + col['Column_Name'] + '` ' + col['Data_Type'] + ', '
    create_stmnt =  create_stmnt[:-2] + ')'
    table_definition['create_sql'] = create_stmnt
    table_definitions.append(table_definition)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import table definitions and create tables in FUAM_Lakehouse
existing_tables = [table['name'] for table in notebookutils.lakehouse.listTables("FUAM_Lakehouse")]
for table_definition in table_definitions:
    if not(table_definition['table'] in existing_tables):
        print("Create table " + table_definition['table'])
        spark.sql(table_definition['create_sql'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fuam_workspace = spark.conf.get("trident.workspace.id")
all_existing_columns = labs.lakehouse.get_lakehouse_columns('FUAM_Lakehouse', fuam_workspace)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table_name in tables:
    fuam_table_info = df_columns[df_columns["Table_Name"] == table_name]

    existing_table_info = all_existing_columns[all_existing_columns["Table Name"] == table_name]
    existing_table_cols = [row["Column Name"] for index, row in existing_table_info.iterrows()]

    for index , row in fuam_table_info.iterrows():
        
        col = row["Column_Name"]
        col_type = row["Data_Type"]
        if (col not in existing_table_cols) & ('struct' not in col_type) :
            print(f"Add column {col} to table {table_name}")
            spark.sql(f" ALTER TABLE FUAM_Lakehouse.{table_name} ADD COLUMN `{col}` {col_type}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Migration Fixes
# Because of changes at the underlying data sources this section migrates your FUAM Lakehouse data to the latest version. 
# For instance all the CapacityIds will be changed to uppercase, etc.

# CELL ********************

def check_column_upper(table , col ) :
    df = spark.read.table(table).select(col).distinct()

    non_uppercase_count = df.filter(~F.upper(F.col(col)).eqNullSafe(F.col(col))).count()
    all_uppercase = non_uppercase_count == 0

    if all_uppercase:
        print("No lower case values detected")
    else:
       print("Lower case values detected") 

    return all_uppercase

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fix FUAM_Lakehouse.capacities
if check_column_upper("FUAM_Lakehouse.capacities", "CapacityId") == False :

    sql = """
    with latest as (
    SELECT 
    UPPER(CapacityId) CapacityId_Upper , MAX(fuam_modified_at) last_mod
    FROM FUAM_Lakehouse.capacities
    GROUP BY UPPER(CapacityId))
    
    SELECT cap.admins ,
    cap.capacityUserAccessRight ,
    cap.displayName ,
    UPPER(cap.CapacityId) CapacityId ,
    cap.region ,
    cap.sku ,
    cap.state ,
    cap.users ,
    cap.fuam_modified_at ,
    cap.fuam_deleted
    
    
    FROM FUAM_Lakehouse.capacities cap
    INNER JOIN latest on UPPER(cap.CapacityID) = CapacityId_Upper AND cap.fuam_modified_at = last_mod
    """

    df = spark.sql(sql)
    df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("FUAM_Lakehouse.capacities")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fix FUAM_Lakehouse.capacity_metrics_by_item_kind_by_day
if check_column_upper("FUAM_Lakehouse.capacity_metrics_by_item_kind_by_day", "CapacityId") == False :

    sql = """SELECT 
    UPPER(CapacityId) AS CapacityId,
    WorkspaceId,
    Date,
    ItemKind,
    DurationInSec,
    TotalCUs,
    ThrottlingInMin,
    UserCount,
    SuccessOperationCount,
    RejectedOperationCount,
    OperationCount,
    InvalidOperationCount,
    FailureOperationCount,
    CancelledOperationCount,
    DateKey
    FROM FUAM_Lakehouse.capacity_metrics_by_item_kind_by_day"""

    df = spark.sql(sql)
    df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("FUAM_Lakehouse.capacity_metrics_by_item_kind_by_day")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fix FUAM_Lakehouse.capacity_metrics_by_timepoint

if check_column_upper("FUAM_Lakehouse.capacity_metrics_by_timepoint", "CapacityId") == False :
    sql = """SELECT 
    UPPER(CapacityId) AS  CapacityId,
    TimePoint,
    BackgroundPercentage,
    InteractivePercentage,
    BackgroundNonBillablePercentage,
    InteractiveNonBillablePercentage,
    AutoscalePercentage,
    CULimitPercentage,
    TotalCUUsagePercentage,
    TotalCUs,
    SKUCUByTimePoint,
    InteractiveDelayPercentage,
    InteractiveRejectionPercentage,
    InteractiveRejectionThreshold,
    BackgroundRejectionPercentage,
    BackgroundRejectionThreshold,
    CarryOverAddedPercentage,
    CarryOverBurndownPercentage,
    CarryOverCumulativePercentage,
    OverageReferenceLine,
    ExpectedBurndownInMin
    FROM FUAM_Lakehouse.capacity_metrics_by_timepoint"""

    df = spark.sql(sql)
    df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("FUAM_Lakehouse.capacity_metrics_by_timepoint")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fix FUAM_Lakehouse.capacity_metrics_by_item_by_operation_by_day
if check_column_upper("FUAM_Lakehouse.capacity_metrics_by_item_by_operation_by_day", "CapacityId") == False :
    df = spark.sql("""
    SELECT UPPER(CapacityId) AS CapacityId ,
    WorkspaceId,
    Date,
    ItemId,
    ItemKind,
    OperationName,
    DurationInSec,
    TotalCUs,
    ThrottlingInMin,
    UserCount,
    SuccessOperationCount,
    RejectedOperationCount,
    OperationCount,
    InvalidOperationCount,
    FailureOperationCount,
    CancelledOperationCount,
    DateKey
    FROM FUAM_Lakehouse.capacity_metrics_by_item_by_operation_by_day
    """)

    
    df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("capacity_metrics_by_item_by_operation_by_day")

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
