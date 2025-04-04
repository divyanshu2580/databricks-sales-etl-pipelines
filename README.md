# databricks-sales-etl-pipelines
 An ETL pipeline in Databricks using PySpark to process and transform sales data from CSV files.
# Databricks Sales ETL Pipeline

This project demonstrates a complete ETL pipeline using Databricks and PySpark.

## 📌 Project Steps:
1. Create a folder structure in Databricks FileStore
2. Load CSV data (Orders, Products, Categories, etc.)
3. Perform inner joins and data transformation using PySpark
4. Export the consolidated sales report as a CSV

## 🛠️ Tools Used
- Databricks
- Apache Spark (PySpark)
- CSV
- Data Lake (FileStore)

## 📁 Data Structure
Place input CSVs in `/FileStore/tables/Project/Source/`

## 🔽 Output
Consolidated report saved at `/FileStore/tables/Project/Dest/sales_report.csv`
