# Databricks notebook source
# MAGIC %run ../common/utils

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

config = Config()

# COMMAND ----------


class SilverIngestion:
    def __init__(self,db_name:str = 'silver' ):
        self.db_name = db_name

    def load_to_silver_customers(self, bronze_table_name:str = 'bronze.customers_bz', silver_table_name:str='customers_si'):
        from pyspark.sql.functions import current_timestamp, col, when, current_date, datediff, lit, trim, lower

        full_silver_table_name = f'`{self.db_name}`.`{silver_table_name}`'

        # Impliment Incremental loading
        last_processed_timestamp = get_last_processed_timestamp(table_name=full_silver_table_name)

        raw_customers_df = spark.table(bronze_table_name) \
                            .filter(col('inserted_time') > lit(last_processed_timestamp).cast('timestamp')) \
                            .dropDuplicates(['customer_id']) 

        
        # Data Validation and Cleaning
        # 1. Validate email
        # 2. Validate age between 18 and 100
        # 3. Create customer segment
        # 4. Days since user registerd in the system
        # 5. Remove any junk records where total purchase is negative
        cleaned_customers_df = raw_customers_df \
                .filter((lower(trim(col('email'))) != 'null') & (col('email').isNotNull())) \
                .filter((col('age') >= 18) & (col('age') <= 100)) \
                .filter(col('total_purchases') >= 0 ) \
                .withColumn('days_since_registration', datediff(current_date(), col('registration_date'))) \
                .withColumn('customer_segment', when(col('total_purchases') > 10000, 'High Value')
                                                .when(col('total_purchases') > 5000, 'Medium Value')
                                                .otherwise('Low Value')) \
                .withColumn('last_updated', col('inserted_time'))

        # Create temp view from the cleaned dataframe
        cleaned_customers_df.createOrReplaceTempView("incremental_view")

        spark.sql(f""" MERGE INTO {full_silver_table_name} AS silver_customers
                        USING incremental_view 
                        ON silver_customers.customer_id = incremental_view.customer_id 
                        WHEN MATCHED THEN UPDATE 
                        SET *
                        WHEN NOT  MATCHED THEN 
                        INSERT *
                   """)
        
    def load_to_silver_products(self, bronze_table_name:str = 'bronze.products_bz', silver_table_name:str='products_si'):
        from pyspark.sql.functions import current_timestamp, col, when, current_date, datediff, lit, trim, lower

        full_silver_table_name = f'`{self.db_name}`.`{silver_table_name}`'

        # Impliment Incremental loading
        last_processed_timestamp = get_last_processed_timestamp(table_name='silver.products_si')
        
        raw_products_df = spark.table(bronze_table_name) \
                .filter(col('inserted_time') > lit(last_processed_timestamp).cast('timestamp')) \
                .dropDuplicates(['product_id']) 

        # Data Validation and Cleaning
        # 1. Price Normalization(Setting negative price to zero)
        # 2. Stock Quantity normalization(Setting negative stock quantity to zero)
        # 3. Rating Normalization(Clamping between 0 and 5)
        # 4. Price Categorization (Premium, Standard, Budget)
        # 5. Stock Status Calculation(Out of stock, Low stock, Moderate stock, Sufficient stock)

        cleaned_products_df = raw_products_df.withColumn('price', when(col('price') < 0 , 0).otherwise(col('price'))) \
            .withColumn('stock_quantity', when(col('stock_quantity') < 0 , 0).otherwise(col('stock_quantity'))) \
            .withColumn('rating', when(col('rating') < 0, 0).when(col('rating') > 5, 5).otherwise(col('rating'))) \
            .withColumn('price_category', when(col('price') > 1000, 'Premium') 
                        .when(col('price') > 100, 'Standard').otherwise('Budget')) \
            .withColumn('stock_status', when(col('stock_quantity') == 0 , 'Out of Stock')
                                        .when(col('stock_quantity') < 10 , 'Low Stock')
                                        .when(col('stock_quantity') < 50  , 'Moderate Stock')
                                        .otherwise('Sufficient Stock'))

        # Implimentation of SCD2

        # 1. Remove duplicate ros from source dataframe if it's already present in target table.
            # Below function uses hash value to compare two dataframes then remove duplicate rows using anti join.

        meta_colummns = ['is_active', 'start_date', 'end_date', 'last_updated']
        target_df = spark.table('silver.products_si')
        deduplicated_products_df = remove_duplicate_rows(source_df=cleaned_products_df, target_df=target_df, meta_columns=meta_colummns)

        deduplicated_products_df.createOrReplaceTempView('source_products_view')

        spark.sql(f""" 
                    MERGE INTO silver.products_si
                    USING (
                        SELECT product_id AS merge_key, * FROM source_products_view
                        UNION ALL
                        SELECT null AS merge_key, source_products_view.* FROM source_products_view 
                        INNER JOIN silver.products_si 
                        ON source_products_view.product_id = silver.products_si.product_id
                    ) AS source_df 
                    ON silver.products_si.product_id = source_df.merge_key
                    WHEN MATCHED AND silver.products_si.is_active = true AND silver.products_si.price != source_df.price
                    THEN UPDATE SET 
                        silver.products_si.is_active = false,
                        silver.products_si.end_date = source_df.inserted_time
                    WHEN NOT MATCHED THEN
                    INSERT(product_id, name, brand, category, price, rating, stock_quantity,  price_category, stock_status, last_updated, is_active, start_date, end_date)
                    VALUES(source_df.product_id, source_df.name, source_df.brand, source_df.category, source_df.price, source_df.rating, source_df.stock_quantity, source_df.price_category, source_df.stock_status, source_df.inserted_time, true, current_timestamp(), null)
                  """)
        
    def load_to_silver_transactions(self, bronze_table_name:str = 'bronze.transactions_bz', silver_table_name:str='transactions_si'):
        from pyspark.sql.functions import current_timestamp, col, when, to_date, lit

        full_silver_table_name =  f'`{self.db_name}`.`{silver_table_name}`'

        # Impliment Incremental loading
        last_processed_timestamp = get_last_processed_timestamp(table_name='silver.transactions_si')

        raw_transactions_df = spark.table(bronze_table_name) \
            .filter(col('inserted_time') > lit(last_processed_timestamp).cast('timestamp')) \
            .dropDuplicates(['transaction_id']) 

        # Data Transformations 
        # 1. Filter out records with null trnsation dates, customer_ids, or products_ids
        # 2. Quantity and total_amount normalization(Setting negative values to 0)
        # 3. Order status derivation based on quantity and total_amount.
        # 4. Date casting to ensure consistent date format.
        # 5. Add column last_updated for incremental loading.

        order_cancel_condition = ((col('quantity') == 0 ) | (col('total_amount') == 0))

        cleaned_transactions_df = raw_transactions_df \
                                    .filter((col('transaction_date') != 'null') & (col('transaction_date').isNotNull())  ) \
                                    .filter(col('customer_id').isNotNull()) \
                                    .filter(col('product_id').isNotNull()) \
                                    .withColumn('quantity', when(col('quantity') < 0, 0)
                                                .otherwise(col('quantity'))) \
                                    .withColumn('total_amount', when(col('total_amount') < 0 , 0)
                                                .otherwise(col('total_amount'))) \
                                    .withColumn('order_status', when(order_cancel_condition, 'Cancelled')
                                                .otherwise('Completed') ) \
                                    .withColumn('transaction_date', to_date(col('transaction_date'), 'yyyy-MM-dd')) \
                                    .withColumn('last_updated', col('inserted_time'))
                                    
        cleaned_transactions_df.createOrReplaceTempView('transactions_view')
  
        spark.sql(f""" MERGE INTO {full_silver_table_name} AS target
                  USING transactions_view AS source
                  ON target.transaction_id = source.transaction_id and target.transaction_date = source.transaction_date
                  WHEN NOT MATCHED THEN
                    INSERT * """)



# COMMAND ----------

silver_ingestion = SilverIngestion()

# COMMAND ----------

silver_ingestion.load_to_silver_customers()

# COMMAND ----------

silver_ingestion.load_to_silver_products()


# COMMAND ----------

silver_ingestion.load_to_silver_transactions()

