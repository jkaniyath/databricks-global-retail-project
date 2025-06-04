# Databricks notebook source
# MAGIC %run ../custom_errors/custom_error

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

config = Config()

# COMMAND ----------

bronze_location_db_external_location = config.get_bronze_external_location()

# COMMAND ----------



class BronzeSetup:
    def __init__(self,  db_name:str = 'bronze', db_location:str = bronze_location_db_external_location):
        self.db_location = db_location
        self.db_name = db_name
        self.is_db_created = False
        

    def create_database(self):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.db_name} MANAGED LOCATION '{self.db_location}'")
        spark.sql(f"USE {self.db_name}")
        self.is_db_created = True
    

    def create_bronze_customers_table(self, table_name:str = 'customers_bz'):
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
                            inserted_time TIMESTAMP
                            """
        if self.is_db_created:
            spark.sql(f"CREATE TABLE IF NOT EXISTS {self.db_name}.{table_name} ({customers_schema}) CLUSTER BY (customer_id, inserted_time )")
        else:
            raise DatabaseNotCreatedError('bronze')

    def create_bronze_products_table(self, table_name:str = 'products_bz'):
        product_schema = """
                            brand STRING,
                            category STRING,
                            name STRING,
                            price DOUBLE,
                            product_id BIGINT,
                            rating DOUBLE,
                            stock_quantity STRING,
                            inserted_time TIMESTAMP
                            """
        if self.is_db_created:
            spark.sql(f"CREATE TABLE IF NOT EXISTS {self.db_name}.{table_name} ({product_schema}) CLUSTER BY (product_id, inserted_time )")
        else:
            raise DatabaseNotCreatedError('bronze')

    def create_bronze_transactions_table(self, table_name:str = 'transactions_bz'):
        transaction_schema = """
                                transaction_id STRING,
                                customer_id INT,
                                product_id INT,
                                quantity INT,
                                total_amount DOUBLE,
                                transaction_date STRING,
                                payment_method STRING,
                                store_type STRING,
                                inserted_time TIMESTAMP
                                """
        if self.is_db_created:
            spark.sql(f"CREATE TABLE IF NOT EXISTS {self.db_name}.{table_name} ({transaction_schema}) CLUSTER BY (inserted_time )")
        else:
            raise DatabaseNotCreatedError('bronze')

    def start_bronze_setup(self):
        self.create_database()
        self.create_bronze_customers_table()
        self.create_bronze_products_table()
        self.create_bronze_transactions_table()


# COMMAND ----------


