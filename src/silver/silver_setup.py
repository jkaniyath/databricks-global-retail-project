# Databricks notebook source
# MAGIC %run ../custom_errors/custom_error

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

config = Config()

# COMMAND ----------

silver_db_external_location = config.get_silver_external_location()

# COMMAND ----------

from typing import Optional

class SilverSetup:
    def __init__(self,  db_name:str = 'silver', db_location:str = silver_db_external_location):
        self.db_location = db_location
        self.db_name = db_name
        self.is_db_created = False
        

    def create_database(self):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.db_name} MANAGED LOCATION '{self.db_location}'")
        spark.sql(f"USE {self.db_name}")
        self.is_db_created = True

    def create_silver_customers_table(self, table_name = 'customers_si'):
        customers_schema = """
                            customer_id INT,
                            name STRING,
                            email STRING,
                            country STRING,
                            customer_type STRING,
                            registration_date DATE,
                            age INT,
                            gender STRING,
                            total_purchases INT,
                            customer_segment STRING,
                            days_since_registration INT,
                            last_updated TIMESTAMP
                           """
        # Databricks Runtime 15.4 LTS
        if self.is_db_created:
            spark.sql(f"create table if not exists {self.db_name}.{table_name} ({customers_schema}) CLUSTER BY AUTO TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        else:
            raise DatabaseNotCreatedError('silver')

    def create_silver_products_table(self, table_name:str = 'products_si'):
        product_schema = """
                        brand STRING,
                        category STRING,
                        name STRING,
                        price DOUBLE,
                        product_id BIGINT,
                        rating DOUBLE,
                        stock_quantity STRING,
                        price_category STRING,
                        stock_status STRING,
                        last_updated TIMESTAMP,
                        is_active BOOLEAN,
                        start_date TIMESTAMP,
                        end_date TIMESTAMP
                        """
                        
        # Databricks Runtime 14.2 and above
        if self.is_db_created:
            spark.sql(f"create table if not exists {self.db_name}.{table_name} ({product_schema}) CLUSTER BY AUTO TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        else:
            raise DatabaseNotCreatedError('silver')

    

    def create_silver_transactions_table(self, table_name:str = 'transactions_si'):
        transactions_schema = """
                            transaction_id STRING,
                            customer_id INT,
                            product_id INT,
                            quantity INT,
                            total_amount DOUBLE,
                            transaction_date DATE,
                            payment_method STRING,
                            store_type STRING,
                            order_status STRING,
                            last_updated TIMESTAMP
                        """
                        
        # Databricks Runtime 14.2 and above
        if self.is_db_created:
            spark.sql(f"create table if not exists {self.db_name}.{table_name} ({transactions_schema}) CLUSTER BY AUTO TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        else:
            raise DatabaseNotCreatedError('silver')

    def create_silver_meta_table(self, table_name:str = 'meta_si'):
        meta_table_schema = """
                                table_name STRING ,
                                last_inserted_time TIMESTAMP
                             """

        if not spark.catalog.tableExists(f"{self.db_name}.{table_name}"):
            if self.is_db_created:
                spark.sql(f"create table if not exists {self.db_name}.{table_name} ({meta_table_schema}) ")
                spark.sql(f""" insert into  {self.db_name}.{table_name}
                          values ('sales_summary_gd', '1900-01-01T00:00:00.000+00:00') ,
                          ('customers_stats_gd', '1900-01-01T00:00:00.000+00:00'),
                          ('product_sales_summary_gd', '1900-01-01T00:00:00.000+00:00')

                          """)
            else:
                raise DatabaseNotCreatedError('silver')

        

    def start_silver_setup(self):
        self.create_database()
        self.create_silver_customers_table()
        self.create_silver_products_table()
        self.create_silver_transactions_table()
        self.create_silver_meta_table()



# COMMAND ----------


