# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## README
# MAGIC 
# MAGIC This script goes through the `prod.workload_insights` table, filters the AutoML related entries,
# MAGIC and writes them into a separate `ml.automl_workflow_insights` table.
# MAGIC The result table is much smaller (26,874 entries as of 4/18/2022) than the original dataset
# MAGIC and more suitable for interactive analysis and dashboard (e.g. go/automl/usage).
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

schema = StructType([
   StructField('CustomerID', IntegerType(), False),
   StructField('FirstName',  StringType(),  False),
   StructField('LastName',   StringType(),  False)
])

data = [
   [ 1000, 'Mathijs', 'Oosterhout-Rijntjes' ],
   [ 1001, 'Joost',   'van Brunswijk' ],
   [ 1002, 'Stan',    'Bokenkamp' ]
]

customers = spark.createDataFrame(data, schema)
customers.show()
