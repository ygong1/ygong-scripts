# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from typing import Dict, List
from pyspark.sql.types import ArrayType, StringType


# COMMAND ----------


df = spark.sql(f''' SELECT
               date, cloudType, canonicalCustomerName, workloadType, workloadName, 
               clusterTags.clusterDriverNodeType as driverNodeType, clusterTags.clusterWorkerNodeType as workerNodeType,
               attributedRevenue * attributedInfo.attributedDriverDbus / attributedInfo.attributedDbus as driverDBU,
               attributedRevenue * attributedInfo.attributedWorkerDbus / attributedInfo.attributedDbus as workderDBU,
               attributedRevenue as DBU,
               clusterFeatureFlags.GPU as useGPU,
               clusterFeatureFlags.MLR as MLR, 
               clusterFeatureFlags,
               workloadFeatureFlags,
               packages from users.yu_gong.workload_insights_1year
               ''')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from users.yu_gong.workload_insights_1year
# MAGIC

# COMMAND ----------

# ML workload based on packages
trainingFramework = "TrainingFramework"
sparkML = "sparkML"
pytorch = "pytorch"
genAI = "genAI"
autoML = "autoML"
sklearn = "sklearn"
xgboost = "xgboost"
other = "misc"
tensorflow = "tensorflow"  

# COMMAND ----------

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

@udf(returnType=ArrayType(StringType()))
def spot(loads: List[str])-> List[str]:
  mapping = {
    "sparkML": sparkML,
    "sklearn": sklearn,
    "xgboost": xgboost,
    "tensorflow": tensorflow,
    "pytorch": pytorch, 
    "huggingface": genAI,
    "langchain": genAI,
    "ML": "ML"
  }
  ret = set(["all"])
  for i in range(len(loads)):
    if loads[i] in mapping:
      ret.add(mapping[loads[i]])
    else:
      ret.add(other)
  return list(ret)

# COMMAND ----------

from pyspark.sql.functions import udf
from typing import Dict, List
from pyspark.sql.types import ArrayType, StringType

@udf(returnType=ArrayType(StringType()))
def parsePackages(packages: Dict[str, List[str]])-> List[str]:  
  
  
  pythonPackageMapping = {
    "databricks.automl": autoML,
    "statsmodels": trainingFramework, 
    "pyspark.ml": sparkML,
    "sklearn": "sklearn",
    "xgboost": "xgboost",
    "torch": pytorch,
    "transformers": genAI,
    "sentence_transformers": genAI,
    "sentencepiece": genAI,
    "tensorflow": tensorflow,
    "sparknlp": sparkML,
    "sparkdl": sparkML,
    "spark_sklearn": sparkML
  }

  if packages == None:
    return []
  packageTypes = set()
  
  pythonPackages = packages['pythonPackages'] if packages != None and 'pythonPackages' in packages else []
  for i in range(len(pythonPackages)):
    for packageSegment in pythonPackageMapping:
      if packageSegment in pythonPackages[i]:
        packageTypes.add(pythonPackageMapping[packageSegment])
      else:
        packageTypes.add(other)

  if "scalaPackages" in packages and len(packages['scalaPackages']) > 0:
    packageTypes.add("scala")

  if "rPackages" in packages and len(packages['rPackages']) > 0:
    packageTypes.add("R")
  return list(packageTypes)

# COMMAND ----------


df = df \
  .withColumn("packagesTypes", parsePackages(col("packages"))) \
  .withColumn("MLworkloads", extractWorkload(col("workloadFeatureFlags")))\
  .withColumn('spottedPackageTypes',spot(col("MLworkloads")))

# COMMAND ----------

df.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("users.yu_gong.insights_1year_process1")

# COMMAND ----------

@udf()
def computeType(mlr: bool, gpu: bool)-> str:
  hardware = "gpu" if gpu else "cpu"
  runtime = "mlr" if mlr else "dbr"
  return f"{runtime}-{hardware}"

df = spark.sql(f'''SELECT date_trunc('week', date) AS week, MLR, useGPU, DBU from users.yu_gong.insights_1year_process1 ''')\
  .withColumn("computetype", computeType(col("MLR"), col("useGPU")))
display(df)

# COMMAND ----------

@udf()
def computeType(single: bool, gpu: bool)-> str:
  hardware = "gpu" if gpu else "cpu"
  single = "single-node" if single else "distribued"
  return f"{single}-{hardware}"

df = spark.sql(f'''SELECT date_trunc('week', date) AS week, driverDBU == DBU as isSingleNode, useGPU, DBU from users.yu_gong.insights_1year_process1 where MLR == TRUE''')\
  .withColumn("computetype", computeType(col("isSingleNode"), col("useGPU")))
display(df)

# COMMAND ----------

from pyspark.sql.functions import explode

@udf(returnType=ArrayType(StringType()))
def spottedLoads(loads: List[str])-> List[str]:
  spottedLoads = set(["sparkML", "sklearn", "xgboost", "ML", "tensorflow",  "pytorch", "huggingface", "langchain"])
  ret = set(["all"])
  for i in range(len(loads)):
    if loads[i] in spottedLoads:
      ret.add(loads[i])
  return list(ret)

df = spark.sql(f'''SELECT date, MLworkloads, DBU from users.yu_gong.insights_10days_process1 where MLR == TRUE''')\
      .withColumn("spottedLoads", spottedLoads(col("MLworkloads")))\
      .select("date", "DBU", explode("spottedLoads"))
display(df)

# COMMAND ----------

df = spark.sql(f'''SELECT date, packagesTypes, DBU from users.yu_gong.insights_10days_process1 where MLR == TRUE''')\
      .select("date", "DBU", explode("packagesTypes").alias("mlType"))
display(df)
