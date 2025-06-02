# Databricks notebook source
class DatabaseNotCreatedError(Exception):
    def __init__(self, db_name):
        message = f"Database '{db_name}' was not created successfully."
        super().__init__(message)
        self.db_name = db_name

# COMMAND ----------


