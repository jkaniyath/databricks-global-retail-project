# Databricks notebook source
# MAGIC %run ../custom_error

# COMMAND ----------


from typing import Optional

class GoldSetup:
    def __init__(self, env:Optional[str] = None, db_name:str = 'gold'):
        self.env = env
        self.db_name = f'{self.env + "." if self.env else ""}' + db_name
        self.is_db_created = False

    def create_database(self):
        # Dynamically create a database name with an optional environment prefix,
        # and create the corresponding database (namespace) if it does not exist
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
        self.is_db_created = True

    def create_gold_sales_summary_table(self, table_name:str = 'sales_summary_gd'):
        sales_summary_schema = """
                            transaction_date DATE,
                            store_type STRING,
                            payment_method STRING,
                            total_sales DOUBLE,
                            total_transactions BIGINT NOT NULL,
                            avg_order_value DOUBLE,
                            total_quantity_sold BIGINT,
                            last_updated TIMESTAMP
                            """
        if self.is_db_created:
            spark.sql(f"CREATE TABLE IF NOT EXISTS {self.db_name}.{table_name} ({sales_summary_schema}) CLUSTER BY (transaction_date, store_type, payment_method )")
        else:
            raise DatabaseNotCreatedError('gold')

    def create_gold_customer_stats_table(self, table_name:str = 'customers_stats_gd'):
        customers_stats_schema = """
                            customer_id INT,
                            name STRING,
                            email STRING,
                            first_order_date DATE,
                            last_order_date DATE,
                            distinct_transaction_dates INT,
                            total_transactions BIGINT ,
                            avg_order_value DOUBLE,
                            total_spent DOUBLE,
                            recency_days INT,
                            last_updated TIMESTAMP
                            """
        if self.is_db_created:
            spark.sql(f"CREATE TABLE IF NOT EXISTS {self.db_name}.{table_name} ({customers_stats_schema}) CLUSTER BY (customer_id )")
        else:
            raise DatabaseNotCreatedError('gold')

    def create_gold_product_sales_summary_table(self, table_name:str = 'product_sales_summary_gd'):

        product_sales_summary_schema = """
                                        product_id LONG,
                                        name STRING,
                                        brand STRING,
                                        category STRING,
                                        price_category STRING,
                                        transaction_date DATE,
                                        total_quantity_sold LONG,
                                        total_revenue DOUBLE,
                                        num_transactions LONG,
                                        last_updated TIMESTAMP
                                        """

        if self.is_db_created:
            spark.sql(f"CREATE TABLE IF NOT EXISTS {self.db_name}.{table_name} ({product_sales_summary_schema}) CLUSTER BY (product_id )")
        else:
            raise DatabaseNotCreatedError('gold')


    def start_gold_setup(self):
        self.create_database()
        self.create_gold_sales_summary_table()
        self.create_gold_customer_stats_table()
        self.create_gold_product_sales_summary_table()

# COMMAND ----------

gold_setup = GoldSetup()

# COMMAND ----------

gold_setup.start_gold_setup()
