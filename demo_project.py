# Databricks notebook source
# MAGIC %md
# MAGIC step.1 create the folder structure

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/tables/Project')

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/tables/Project/Source')
dbutils.fs.mkdirs('/FileStore/tables/Project/Dest')

# COMMAND ----------

# MAGIC %md
# MAGIC step.2 Load CSV files 

# COMMAND ----------

Orders_data = spark.read.csv('/FileStore/tables/Project/Source/Orders.csv',header=True,inferSchema=True)
Order_items_data = spark.read.csv('/FileStore/tables/Project/Source/Order_Item.csv',header=True,inferSchema=True)
Products_data = spark.read.csv('/FileStore/tables/Project/Source/Products.csv',header=True,inferSchema=True)
Categories_data = spark.read.csv('/FileStore/tables/Project/Source/Categories.csv',header=True,inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Step.3 Show/verify the data 

# COMMAND ----------

Orders_data.show()

# COMMAND ----------

Order_items_data.show()

# COMMAND ----------

Products_data.show()

# COMMAND ----------

Categories_data.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Step.4 Data transformation 
# MAGIC # Performing Inner Joins to consolidate sales data with product and Categories data

# COMMAND ----------

from pyspark.sql.functions import col

joined_data=(
    Orders_data
    .join(Order_items_data,"order_id","inner")
    .join(Products_data,"product_id","inner")
    .join(Categories_data,"category_id","inner")
    .select(
        col("order_id"),
        col('customer_id'),
        col('order_date'),
        col('product_name'),
        col('category_name'),
        col('quantity'),
        col('total_amount')
    )
)
joined_data.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Step.5 Load data into destination

# COMMAND ----------

joined_data.write.csv('/FileStore/tables/Project/Dest/sales_report.csv',header=True,mode='overwrite')

# COMMAND ----------

