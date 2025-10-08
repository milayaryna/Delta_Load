import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import col
from pyspark.sql.functions import lit

# Oracle DB connection settings
oracle_host = ""
oracle_port = ""
oracle_sid = ""
oracle_user = ""
oracle_password = ""
jdbc_url = f"jdbc:oracle:thin:@{oracle_host}:{oracle_port}:{oracle_sid}"

# Table category definitions (Databricks with _TEST)
table_category_mapping = {
    "COS_RMPF": [
        "COS_RMPF_CC_TEST", "COS_RMPF_EQUIPMENT_PERF_TEST", 
        "COS_RMPF_FLEET_SIZE_TEST", "COS_RMPF_FACTS_TEST"
    ],
    "COS_MP": [
        "COS_MP_MAIN_TEST", "COS_MP_CONTRACTS_TEST"
    ],
    "OBR_UPLOAD": [
        "VW_OBR_TML_OPERATIONAL_DATA_TEST", "OBR_ASC_TEST", "OBR_BM_FACTSHEET_TEST"
    ],
    "FINANCE_MASTER": ["COS_FINANCE_MASTER_RAW_DATA_TEST"],
    "INV_ONHAND_SUMMARY": ["INV_ONHAND_SUMMARY_TEST"],
    "CO_DIV_BU_TML_MAPPINGS": ["CO_DIV_BU_TML_MAPPINGS_TEST"]
}

def build_reverse_mapping():
    table_to_category = {}
    test_table_to_real_table = {}
    target_tables = set()
    for category, tables in table_category_mapping.items():
        for test_table in tables:
            real_table = test_table.replace("_TEST", "")
            table_to_category[test_table] = category
            test_table_to_real_table[test_table] = real_table
            target_tables.add(real_table)  # Oracle actual name
    return table_to_category, test_table_to_real_table, target_tables

def read_recent_audit_logs():
    audit_query = """
    SELECT *
    FROM BMUSR.TABLE_AUDIT_LOG
    WHERE ACTION_TIME >= TO_TIMESTAMP('2025-03-31 16:00:00', 'YYYY-MM-DD HH24:MI:SS')
        AND ACTION_TIME <= TO_TIMESTAMP('2025-03-31 18:00:00', 'YYYY-MM-DD HH24:MI:SS')
    ORDER BY ACTION_TIME
    """
    return spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"({audit_query})") \
        .option("user", oracle_user) \
        .option("password", oracle_password) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .load()

def refresh_full_table(test_table_name, action_time):
    # Calculate the last month from the given action time
    action_dt = action_time
    last_month = action_dt - relativedelta(months=1)
    y, m = last_month.year, last_month.month

    try:
        # Get the real table name by removing "_TEST" suffix
        real_table_name = test_table_name.replace("_TEST", "")
        
        # If the table is CO_DIV_BU_TML_MAPPINGS_TEST, drop and rebuild it
        if test_table_name == "CO_DIV_BU_TML_MAPPINGS_TEST":
            # Load the full table from Oracle (no need for YEAR, MONTH filtering)
            df = spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", real_table_name) \
                .option("user", oracle_user) \
                .option("password", oracle_password) \
                .option("driver", "oracle.jdbc.driver.OracleDriver") \
                .load()

            # Truncate the existing table (preserve schema)
            spark.sql(f"TRUNCATE TABLE {test_table_name}")

            # Align the columns of the loaded data with the target table
            target_columns = spark.table(test_table_name).columns
            df = df.select(*target_columns)

            # Create a temporary view for inserting data
            temp_view_name = f"temp_view_full_{test_table_name.lower()}"
            df.createOrReplaceTempView(temp_view_name)

            # Insert the new data into the table
            spark.sql(f"""
                INSERT INTO {test_table_name}
                SELECT * FROM {temp_view_name}
            """)

            print(f"Table {test_table_name} truncated and rebuilt from Oracle (full data).")

        else:
            # For other tables, filter data by YEAR and MONTH
            full_query = f"""
                SELECT *
                FROM {real_table_name}
                WHERE YEAR = {y} AND MONTH = {m}
            """

            df = spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", f"({full_query})") \
                .option("user", oracle_user) \
                .option("password", oracle_password) \
                .option("driver", "oracle.jdbc.driver.OracleDriver") \
                .load()

            # Delete existing data for the same year and month
            spark.sql(f"DELETE FROM {test_table_name} WHERE YEAR = {y} AND MONTH = {m}")

            # Align the columns of the loaded data with the target table
            target_columns = spark.table(test_table_name).columns
            df = df.select(*target_columns)

            # Create a temporary view for inserting data
            temp_view_name = f"temp_view_full_{test_table_name.lower()}"
            df.createOrReplaceTempView(temp_view_name)

            # Insert the new data into the table
            spark.sql(f"""
                INSERT INTO {test_table_name}
                SELECT * FROM {temp_view_name}
            """)
            print(f"Full refresh done for {test_table_name} (YEAR={y}, MONTH={m})")

    except Exception as e:
        print(f"Full refresh failed for {test_table_name} due to: {e}")

def update_delta_table(test_table_name, tml, year, month):
    if test_table_name == "VW_OBR_TML_OPERATIONAL_DATA_TEST":
        real_table_name = "VW_OBR_TML_OPERATIONAL_DATA_FOR_DBW"
    else:
        real_table_name = test_table_name.replace("_TEST", "")

    delta_query = f"""
        SELECT *
        FROM {real_table_name}
        WHERE TML = '{tml}' AND YEAR = {year} AND MONTH = {month}
    """

    try:
        df_delta = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"({delta_query})") \
            .option("user", oracle_user) \
            .option("password", oracle_password) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .load()

        spark.sql(f"DELETE FROM {test_table_name} WHERE TML = '{tml}' AND YEAR = {year} AND MONTH = {month}")

        target_df = spark.table(test_table_name)
        target_columns = target_df.columns

        missing_columns = [col for col in target_columns if col not in df_delta.columns]
        for col_name in missing_columns:
            df_delta = df_delta.withColumn(col_name, lit(None))

        df_delta = df_delta.select(*target_columns)
        df_delta.createOrReplaceTempView("temp_view_to_append")
        spark.sql(f"""
            INSERT INTO {test_table_name}
            SELECT * FROM temp_view_to_append
        """)
        print(f"Updated table {test_table_name} (TML={tml}, YEAR={year}, MONTH={month})")
    except Exception as e:
        print(f"Failed to update table {test_table_name} (TML={tml}, YEAR={year}, MONTH={month}) due to: {e}")

def process_obr_upload_partitions(partitions):
    for tml, year, month in partitions:
        for test_table_name in table_category_mapping["OBR_UPLOAD"]:
            try:
                update_delta_table(test_table_name, tml, year, month)
                print(f"OBR_UPLOAD processed: {test_table_name} (TML={tml}, YEAR={year}, MONTH={month})")
            except Exception as e:
                print(f"Error in OBR_UPLOAD: {test_table_name} (TML={tml}, YEAR={year}, MONTH={month}) due to: {e}")

def save_json(data_dict, path):
    output = {k: list(v) for k, v in data_dict.items()}
    with open(path, "w") as f:
        json.dump(output, f, indent=4)

  def main():
    table_to_category, test_table_to_real_table, target_tables = build_reverse_mapping()
    audit_df = read_recent_audit_logs()

    if audit_df.rdd.isEmpty():
        print("No audit log records found in the past 8 days. Exiting...")
        return

    category_tml_record = {}
    obr_partitions_to_refresh = set()

    for row in audit_df.collect():
        table_name = row["AFFECTED_TABLE"].upper()
        if table_name not in target_tables:
            continue

        test_table_name = [k for k, v in test_table_to_real_table.items() if v == table_name][0]

        tml = row["TML"]
        year = row["YEAR"]
        month = row["MONTH"]
        action_time = row["ACTION_TIME"]
        category = table_to_category[test_table_name]

        if tml == "ALL" and test_table_name in table_category_mapping[category]:
            refresh_full_table(test_table_name, action_time)
            category_tml_record.setdefault(category, set()).add("all")
            continue

        if not all([tml, year, month]):
            continue

        if category == "OBR_UPLOAD":
            obr_partitions_to_refresh.add((tml, year, month))
            continue

        update_delta_table(test_table_name, tml, year, month)
        category_tml_record.setdefault(category, set()).add(tml)

    if obr_partitions_to_refresh:
        process_obr_upload_partitions(obr_partitions_to_refresh)
        for tml, _, _ in obr_partitions_to_refresh:
            category_tml_record.setdefault("OBR_UPLOAD", set()).add(tml)
    else:
        print("No OBR_UPLOAD partitions to refresh.")

    if not category_tml_record:
        print("No Delta updates detected. Nothing to save.")
    else:
        category_tml_record_json = {k: list(v) for k, v in category_tml_record.items()}
        output_path = f"file:/Workspace/Users/az.admz.yhting@hutchisonports.onmicrosoft.com/data/output/updated_tml_record.json"

        # Convert dict to JSON string
        json_str = json.dumps(category_tml_record_json, indent=4)

        # Use dbutils.fs.put to write to DBFS
        dbutils.fs.put(output_path, json_str, overwrite=True)

        print("✅ Sync completed. JSON saved to updated_tml_record.json")


if __name__ == "__main__":
    main()

# Audit Oracle Views Containing 'OBR' with Spark SQL
audit_query = """
SELECT OWNER, VIEW_NAME
FROM ALL_VIEWS
WHERE VIEW_NAME LIKE '%OBR%'
ORDER BY OWNER, VIEW_NAME
"""
audit_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f"({audit_query})") \
    .option("user", oracle_user) \
    .option("password", oracle_password) \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .load()
audit_df.display()
