# Databricks notebook source
# MAGIC %run ../src/common/config

# COMMAND ----------

# MAGIC %run ../src/common/utils 

# COMMAND ----------

config = Config()

# COMMAND ----------

customers_landing = config.get_customers_landing_external_location()
customers_archive = config.get_archive_customers_external_location()

# COMMAND ----------

archive_files(source_dir=customers_landing, archive_dir=customers_archive)

# COMMAND ----------

# ARCHIVE FILES

from datetime import datetime, timezone

source_dir = customers_landing
archive_dir = customers_archive
current_utc_time = datetime.now(timezone.utc).strftime( '%Y-%m-%d_%H:%M:%S')

# # Create the archive folder if it doesn't exist
# dbutils.fs.mkdirs(archive_dir)

# List files in the source folder
files = dbutils.fs.ls(source_dir)

# Move each file to the archive directory
for file_info in files:
    source_path = file_info.path
    filename = current_utc_time + '_' + source_path.split('/')[-1]
    destination_path = f"{archive_dir}/{filename}"
    # dbutils.fs.cp(source_path, destination_path)
    # dbutils.fs.rm(source_path)
    dbutils.fs.mv(source_path, destination_path)

    print(source_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from dev.silver.products_si
# MAGIC where product_id = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from dev.silver.products_si
# MAGIC where product_id = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from dev.silver.products_si
# MAGIC where product_id = 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.gold.sales_summary_gd
