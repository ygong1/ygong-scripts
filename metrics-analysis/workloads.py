# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## README
# MAGIC
# MAGIC This script goes through the `prod.workload_insights` table, filters the AutoML related entries,
# MAGIC and writes them into a separate `ml.automl_workflow_insights` table.
# MAGIC The result table is much smaller (26,874 entries as of 4/18/2022) than the original dataset
# MAGIC and more suitable for interactive analysis and dashboard (e.g. go/automl/usage).
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE users.yu_gong.workload_insights_10days SELECT * from prod.workload_insights where customerType IN ("Customer", "MicrosoftPaid") AND canonicalCustomerName NOT IN ("Databricks", "Microsoft") AND date < date_trunc('week', current_date()) AND date >= DATEADD(day, -10, current_date())

# COMMAND ----------

from pyspark.sql.functions import col
df = spark.sql(f''' SELECT * from  users.yu_gong.workload_insights_10days''').select ('date', 'cloudType', 'canonicalCustomerName', 'workloadType', 'workloadName', 'workloadFeatureFlags', 'clusterTags', 'clusterFeatureFlags', 'attributedInfo', 'attributedRevenue', 'packages').filter(col("workloadFeatureFlags").isNotNull())
df.cache()

# COMMAND ----------

from pyspark.sql.functions import udf
from typing import Dict, List
from pyspark.sql.types import ArrayType, StringType

# ML, horovod, horovodrunner, petastorm, pytorch, sklearn
# sparkdl, tensorflow, xgboost
@udf(returnType=ArrayType(StringType()))
def extractWorkload(workloadFeatureFlags: Dict[str, str])-> List[str]:
  workloadTypes = []
  if workloadFeatureFlags != None:
    for k in workloadFeatureFlags.keys():
      if workloadFeatureFlags[k]:
        workloadTypes.append(k)
  return workloadTypes

# COMMAND ----------

Find out all the different 

# COMMAND ----------

from pyspark.sql.functions import explode
from pyspark.sql.functions import lit

allWorkloads = df \
  .withColumn("MLworkloads", extractWorkload(col("workloadFeatureFlags"))) \
  .select(explode("MLworkloads").alias("ygong")).distinct().collect()
  
# values = tmp.collect()

# for r in values:
#   print(r)
  

# COMMAND ----------

tmp.select("ygong").distinct().show()

