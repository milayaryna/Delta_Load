# Oracle Delta Table Sync & Audit with Spark

This repository contains a PySpark script to synchronize Oracle tables with Delta tables in Databricks, handle full refreshes, delta updates, and audit Oracle views containing "OBR".

---

## Features

- **Full Table Refresh** for test tables, including handling special tables like `CO_DIV_BU_TML_MAPPINGS_TEST`.
- **Delta Updates** based on TML, YEAR, MONTH.
- **OBR Upload Partition Handling** for incremental updates.
- **Audit Oracle Views** containing "OBR" using Spark SQL.
- **JSON Output** of updated TML records for tracking.
