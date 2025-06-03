# Databricks notebook source
def archive_files(source_dir:str, archive_dir:str):
    """
    Archives files from a source directory to an archive directory with a timestamped filename.

    This function lists all files in the specified `source_dir`, prepends each filename with the current 
    UTC timestamp, and moves them to the `archive_dir`.

    Parameters
    ----------
    source_dir : str
        The path to the source directory containing the files to archive (e.g., a DBFS path like 'dbfs:/mnt/source').
    
    archive_dir : str
        The path to the archive directory where the files will be moved (e.g., 'dbfs:/mnt/archive').

    Notes
    -----
    - Filenames are prefixed with the current UTC timestamp in the format 'YYYY-MM-DD_HH:MM:SS'.
    - Uses `dbutils.fs.mv` to move files, which deletes them from the source after the move.
    - Intended for use in a Databricks environment.
    """
    from datetime import datetime, timezone

    current_utc_time = datetime.now(timezone.utc).strftime( '%Y-%m-%d_%H:%M:%S')


    # List files in the source folder
    files = dbutils.fs.ls(source_dir)

    # Move each file to the archive directory
    for file_info in files:
        source_path = file_info.path
        filename = current_utc_time + '_' + source_path.split('/')[-1]
        destination_path = f"{archive_dir}/{filename}"
        dbutils.fs.mv(source_path, destination_path)      


# COMMAND ----------

def get_last_processed_timestamp(table_name:str) -> str:
    from pyspark.sql.functions import col 

    last_processed_df = spark.sql(f'select max(last_updated) as last_processed from {table_name}')
    last_processed_timestamp = last_processed_df.collect()[0]['last_processed']

    if last_processed_timestamp is None:
        last_processed_timestamp = '1900-01-01T00:00:00.000+00:00'

    return last_processed_timestamp


# COMMAND ----------

from pyspark.sql import DataFrame

def remove_duplicate_rows(source_df:DataFrame, target_df:DataFrame, meta_columns:list["str"]) -> DataFrame:

    """
        Compares the source and target DataFrames and removes duplicate rows from the source DataFrame.

        This function generates a hash of the rows (excluding specified metadata columns) from both the source 
        and target DataFrames using the MD5 hash function. It performs a **left anti join** between the source 
        and target DataFrames to remove rows in the source that already exist in the target (based on hash).

        Parameters:
        ----------
        source_df : DataFrame
            The source DataFrame containing the new or updated data that needs to be compared against the target DataFrame.

        target_df : DataFrame
            The target DataFrame that contains the existing data to which the source DataFrame will be compared.

        meta_columns : list[str]
                These columns typically contain metadata (e.g., timestamps, unique identifiers, or flags like `start_date`, `end_date`, and `is_active`) 
            that shouldn't be used to detect duplicates, particularly in Slowly Changing Dimension (SCD2) tables.

        Returns:
        -------
        DataFrame
            A DataFrame containing the rows from the source DataFrame that do not have corresponding matches 
            in the target DataFrame (i.e., rows that are considered unique based on the non-metadata columns).

        Notes:
        ------
        - The function creates a hash by concatenating the values of all columns (except the metadata columns) 
            and applying the MD5 hash function to detect duplicates.
        - The final result DataFrame contains only the rows from the source DataFrame that do not have an exact match 
            in the target DataFrame, based on the non-metadata columns.
        - This function uses the **left anti join** to filter out the duplicate rows from the source DataFrame.

        Example Usage:
        -------------
        ```python
        unique_rows = remove_duplicate_rows(source_df=source_data, target_df=target_data, meta_columns=["start_date", "end_date", "is_active"])
        ```

        This will return a DataFrame with only those rows from `source_data` that do not exist in `target_data`,
        ignoring the `start_date`, `end_date`, and `is_active` columns in target  datframe during comparison.

    """

    from pyspark.sql.functions import md5, lit, concat_ws

    target_df_columns = [x for x in target_df.columns if x not in meta_columns]
    target_df_without_meta_cols = target_df.select(target_df_columns)

    final_target_df = target_df_without_meta_cols.withColumn("hash", md5(concat_ws("|", *[c for c in target_df_columns])))
    final_source_df = source_df.withColumn("hash", md5(concat_ws("|", *[c for c in target_df_columns])))

    result = final_source_df.join(final_target_df, final_source_df.hash==final_target_df.hash, "left_anti") \
                .drop("hash")

    return result


# COMMAND ----------

from datetime import datetime

def get_last_commit_time(table_name:str)-> datetime:
    last_commit_time = spark.sql(f"""select last_inserted_time 
                                        from silver.meta_si where table_name ='{table_name}' """).collect()[0][0]
    
    return last_commit_time
