# Databricks notebook source
# MAGIC %run ../common/utils

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

config = Config()

# COMMAND ----------

customers_landing = config.get_customers_landing_external_location()
customers_archive = config.get_archive_customers_external_location()

products_archive = config.get_archive_products_external_location()
products_landing = config.get_products_landing_external_location()

transactions_archive = config.get_archive_transactions_external_location()
transactions_landing = config.get_transactions_landing_external_location()


# COMMAND ----------


class BronzeIngestion:
    def __init__(self,db_name:str = 'bronze'):
        self.db_name = db_name

    def load_to_bronze_customers(self, source_dir:str, table_name:str, archive_dir:str):
        from pyspark.sql.functions import current_timestamp

        full_table_name = f'`{self.db_name}`.`{table_name}`'

        customers_schema = """
                            customer_id INT,
                            name STRING,
                            email STRING,
                            country STRING,
                            customer_type STRING,
                            registration_date DATE,
                            age INT,
                            gender STRING,
                            total_purchases INT
                            """

        customers_df = spark.read.format('csv') \
                        .option('header', True) \
                        .schema(customers_schema) \
                        .load(source_dir) \
                        .withColumn('inserted_time', current_timestamp()) \
                        .write.mode('append') \
                        .saveAsTable(full_table_name)

        

        archive_files(source_dir, archive_dir)
    

    def load_to_bronze_products(self, source_dir:str, table_name:str, archive_dir:str):
        from pyspark.sql.functions import current_timestamp

        full_table_name = f'`{self.db_name}`.`{table_name}`'
        
        product_schema = """
                    brand STRING,
                    category STRING,
                    name STRING,
                    price DOUBLE,
                    product_id BIGINT,
                    rating DOUBLE,
                    stock_quantity STRING
                    """

        products_df = spark.read \
                        .format('json') \
                        .option('multiLine', True) \
                        .schema(product_schema) \
                        .load(source_dir) \
                        .withColumn('inserted_time', current_timestamp()) \
                        .write.mode('append') \
                        .saveAsTable(full_table_name)

        archive_files(source_dir, archive_dir)

    
    def load_to_bronze_transactions(self, source_dir:str, table_name:str, archive_dir:str):
        from pyspark.sql.functions import current_timestamp

        full_table_name = f'`{self.db_name}`.`{table_name}`'

        transaction_schema = """
                        transaction_id STRING,
                        customer_id INT,
                        product_id INT,
                        quantity INT,
                        total_amount DOUBLE,
                        transaction_date STRING,
                        payment_method STRING,
                        store_type STRING
                        """

        transacions_df = spark.read \
                            .format('json') \
                            .schema(transaction_schema) \
                            .load(source_dir) \
                            .withColumn('inserted_time', current_timestamp()) \
                            .write.mode('append') \
                            .saveAsTable(full_table_name)

        archive_files(source_dir, archive_dir)
                        
