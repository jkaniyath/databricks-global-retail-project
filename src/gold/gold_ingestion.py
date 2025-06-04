# Databricks notebook source
# MAGIC %run ../common/utils

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

config = Config()

# COMMAND ----------


class GoldIngestion:
    def __init__(self,db_name:str = 'gold'):
        self.db_name = db_name
    
    def load_to_gold_sales_summary(self, source_table = 'silver.transactions_si',  target_table_name:str='sales_summary_gd'):
        from pyspark.sql.functions import sum, avg, col, round, count, lit , current_timestamp, max

        full_target_table_name =  f'`{self.db_name}`.`{target_table_name}`'


        last_commit_time = get_last_commit_time('sales_summary_gd')

        raw_transactions_df = spark.read.format('delta') \
                                .option('readChangeFeed', True) \
                                .option('startingVersion', 0) \
                                .table(source_table) \
                                .filter(col('_change_type').isin(['insert', 'update_postimage'])) \
                                .filter(col('_commit_timestamp') > last_commit_time)
    
        if not raw_transactions_df.isEmpty():               
            # Analyze sales trends over time segmented by store and payment behavior.
            transactions_summary_df = raw_transactions_df .filter(col('order_status') != 'Cancelled') \
                                        .groupBy('transaction_date', 'store_type', 'payment_method') \
                                        .agg(round(sum('total_amount'), 2).alias('total_sales'), 
                                            count('transaction_id').alias('total_transactions'),
                                            round(avg('total_amount'), 2).alias('avg_order_value'),
                                            sum('quantity').alias('total_quantity_sold') ) \
                                        .withColumn('last_updated', current_timestamp())

            transactions_summary_df.createOrReplaceTempView('transactions_summary_view')

            spark.sql(f""" MERGE INTO {full_target_table_name} AS target
                            USING transactions_summary_view AS source
                            ON target.transaction_date = source.transaction_date 
                                and target.store_type = source.store_type
                                and target.payment_method = source.payment_method
                            WHEN MATCHED THEN 
                                UPDATE SET 
                                    target.total_sales = target.total_sales + source.total_sales,
                                    target.total_transactions = target.total_transactions + source.total_transactions,
                                    target.avg_order_value = round((target.total_sales + source.total_sales)/(target.total_transactions + source.total_transactions), 2),
                                    target.total_quantity_sold = target.total_quantity_sold + source.total_quantity_sold,
                                    target.last_updated = source.last_updated
                            WHEN NOT MATCHED THEN
                                INSERT * """)
            
        
            max_commited_time = raw_transactions_df.agg(max(col('_commit_timestamp')).alias('max_commited_time')).collect()[0][0].strftime('%Y-%m-%dT%H:%M:%S.000+00:00')

            spark.sql(f""" update silver.meta_si 
                            set last_inserted_time = '{max_commited_time}'
                            where table_name = 'sales_summary_gd'
                            """)
        
    def load_to_gold_customers_stats(self, target_table_name:str='customers_stats_gd'):

        from pyspark.sql.functions import broadcast, datediff, col, min, max, avg, sum , count, countDistinct, round , current_timestamp, current_date

        full_target_table_name =  f'`{self.db_name}`.`{target_table_name}`'

        last_commit_time = get_last_commit_time('customers_stats_gd')


        raw_transactions_df = spark.read.format('delta') \
                                .option('readChangeFeed', True) \
                                .option('startingVersion', 0) \
                                .table('silver.transactions_si') \
                                .filter(col('_change_type').isin(['insert', 'update_postimage'])) \
                                .withColumnRenamed('customer_id', 'transactions_customer_id') \
                                .filter(col('_commit_timestamp') > last_commit_time)
                         
        if not raw_transactions_df.isEmpty():
            raw_customers_df = spark.table('silver.customers_si')

            joined_df = raw_transactions_df.join(raw_customers_df, raw_customers_df.customer_id == raw_transactions_df.transactions_customer_id, 'inner') \
                            .filter(col('order_status') != 'Cancelled') \
                            .select('customer_id', 'name', 'email','transaction_id', 'transaction_date', 
                                    'total_amount' ) \
                            .groupBy('customer_id', 'name', 'email') \
                            .agg(min(col('transaction_date')).alias('first_order_date'), 
                                max(col('transaction_date')).alias('last_order_date'),
                                countDistinct(col('transaction_date')).alias('distinct_transaction_dates'), 
                                count(col('transaction_id')).alias('total_transactions'),
                                round(avg(col('total_amount')), 2).alias('avg_order_value'),
                                round(sum(col('total_amount')), 2).alias('total_spent')) \
                            .withColumn('recency_days', datediff(current_date(), col('last_order_date'))) \
                            .withColumn('last_updated', current_timestamp())
                    
            joined_df.createOrReplaceTempView('customers_stats_view')

            

            spark.sql(f""" MERGE INTO {full_target_table_name} as target
                            USING customers_stats_view as source
                            on target.customer_id = source.customer_id 
                            WHEN MATCHED THEN
                            UPDATE SET
                                target.name = source.name,
                                target.email = source.email,
                                target.first_order_date = least(target.first_order_date, source.first_order_date),
                                target.last_order_date = greatest(target.last_order_date, source.last_order_date),
                                target.distinct_transaction_dates = target.distinct_transaction_dates + source.distinct_transaction_dates,
                                target.total_transactions = target.total_transactions + source.total_transactions, 
                                target.avg_order_value = round((target.total_spent + source.total_spent) / (target.total_transactions + source.total_transactions), 2),
                                target.total_spent = round(target.total_spent + source.total_spent,2),
                                target.recency_days = date_diff(current_date(), greatest(target.last_order_date, source.last_order_date)),
                                target.last_updated = source.last_updated
                            WHEN NOT MATCHED THEN
                                INSERT *  """)
            
            
            max_commited_time = raw_transactions_df.agg(max(col('_commit_timestamp')).alias('max_commited_time')).collect()[0][0].strftime('%Y-%m-%dT%H:%M:%S.000+00:00')

            spark.sql(f""" update silver.meta_si 
                            set last_inserted_time = '{max_commited_time}'
                            where table_name = 'customers_stats_gd'
                            """)

    def load_to_gold_product_sales_summary(self, target_table_name:str='product_sales_summary_gd'):

        # Daily product_sales_summary

        # Track daily sales trends per product.

        # Analyze brand/category performance.

        # Identify top-selling or underperforming products.

        # Compare price category behavior (e.g., premium vs. budget).


        from pyspark.sql.functions import broadcast, datediff, col, min, max, avg, sum , count, round , current_timestamp

        full_target_table_name =  f'`{self.db_name}`.`{target_table_name}`'

        last_commit_time = get_last_commit_time('product_sales_summary_gd')


        raw_transactions_df = spark.read.format('delta') \
                                .option('readChangeFeed', True) \
                                .option('startingVersion', 0) \
                                .table('silver.transactions_si') \
                                .filter(col('_change_type').isin(['insert', 'update_postimage'])) \
                                .withColumnRenamed('product_id', 'transactions_product_id') \
                                .filter(col('_commit_timestamp') > last_commit_time) 

        if not raw_transactions_df.isEmpty():
            active_products_df = spark.table('silver.products_si') \
                                    .filter(col('is_active') == True) \
                                    

            joined_df = raw_transactions_df.join(active_products_df, raw_transactions_df.transactions_product_id == active_products_df.product_id, 'inner') \
                        .select('product_id', 'name', 'brand', 'category', 'price_category', 'quantity', 'total_amount', 'transaction_date') \
                        .groupBy('product_id', 'name', 'brand', 'category', 'price_category', 'transaction_date') \
                        .agg(sum(col('quantity')).alias('total_quantity_sold'),
                             round(sum(col('total_amount')),2).alias('total_revenue'),
                             count('*').alias('num_transactions')) \
                        .withColumn('last_updated', current_timestamp())
            
            joined_df.createOrReplaceTempView('product_sales_summary_view')

            spark.sql(f""" 
                        MERGE INTO {full_target_table_name} AS target
                        USING product_sales_summary_view AS source
                        ON target.product_id = source.product_id  AND target.name = source.name 
                        AND target.brand = source.brand AND target.category = source.category
                        AND target.price_category = source.price_category  AND target.transaction_date = source.transaction_date      
                        WHEN MATCHED THEN 
                            UPDATE SET 
                                target.total_quantity_sold = target.total_quantity_sold + source.total_quantity_sold,
                                target.total_revenue = target.total_revenue + source.total_revenue,
                                target.num_transactions = target.num_transactions + source.num_transactions,
                                target.last_updated = source.last_updated
                        WHEN NOT MATCHED 
                            THEN INSERT *
                      """)
            
            max_commited_time = raw_transactions_df.agg(max(col('_commit_timestamp')).alias('max_commited_time')).collect()[0][0].strftime('%Y-%m-%dT%H:%M:%S.000+00:00')

            spark.sql(f""" update silver.meta_si 
                            set last_inserted_time = '{max_commited_time}'
                            where table_name = 'product_sales_summary_gd'
                            """)



# COMMAND ----------



# COMMAND ----------




# COMMAND ----------



# COMMAND ----------




# COMMAND ----------


