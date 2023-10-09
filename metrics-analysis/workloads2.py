# Databricks notebook source
from pyspark.sql.functions import col

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
               packages from users.yu_gong.workload_insights_10days where workloadFeatureFlags is not null
               ''')

# COMMAND ----------

display(df)

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

display(df)

# COMMAND ----------

df.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("users.yu_gong.insights_10days_process1")

# COMMAND ----------

@udf()
def computeType(mlr: bool, gpu: bool)-> str:
  hardware = "gpu" if gpu else "cpu"
  runtime = "mlr" if mlr else "dbr"
  return f"{runtime}-{hardware}"

df = spark.sql(f'''SELECT date, MLR, useGPU, DBU from users.yu_gong.insights_10days_process1 ''')\
  .withColumn("computetype", computeType(col("MLR"), col("useGPU")))
display(df)

# COMMAND ----------

@udf()
def computeType(single: bool, gpu: bool)-> str:
  hardware = "gpu" if gpu else "cpu"
  single = "single-node" if single else "distribued"
  return f"{single}-{hardware}"

df = spark.sql(f'''SELECT date, driverDBU == DBU as isSingleNode, useGPU, DBU from users.yu_gong.insights_10days_process1 where MLR == TRUE''')\
  .withColumn("computetype", computeType(col("isSingleNode"), col("useGPU")))
display(df)

# COMMAND ----------

from pyspark.sql.functions import explode
from pyspark.sql.functions import col

df = spark.sql(f'''SELECT date, spottedPackageTypes, DBU from users.yu_gong.insights_10days_process1 where MLR == TRUE''')\
  .select("date", explode("spottedPackageTypes").alias("ygong"), "DBU")
display(df)

# COMMAND ----------

df = spark.sql(f'''SELECT date, spottedPackageTypes, DBU from users.yu_gong.insights_10days_process1 where MLR == TRUE''')
df.printSchema()

# COMMAND ----------



# COMMAND ----------

df = spark.sql(f'''SELECT date, packagesTypes, DBU from users.yu_gong.insights_10days_process1 where MLR == TRUE''')\
      .select("date", "DBU", explode("packagesTypes").alias("mlType"))
display(df)

# COMMAND ----------

from pyspark.sql.functions import col, split, struct
from pyspark.sql.types import DoubleType, StructType, StructField

workloads = [sparkML, pytorch, genAI, autoML, sklearn, xgboost, other, tensorflow]
types = [StructField(w, DoubleType(), False) for w in workloads]
return_types = [StructField("ML", DoubleType(), False)]  + types
other_index = workloads.index(other)

@udf (returnType=StructType(return_types))
def split_name(MLLoads: List[str], dbu: float)-> List[float]:
  x = set(MLLoads)
  x_dbs = [dbu if w in x else 0 for w in workloads]

  if "ML" in MLLoads:
    x_dbs[other_index] = 0
    v = [dbu] + x_dbs
    
  else:
    v = [0] + x_dbs
  return v


# COMMAND ----------

from pyspark.sql.functions import col, sum, avg

df = spark.sql(f'''SELECT date, spottedPackageTypes, DBU from users.yu_gong.insights_10days_process1 where MLR == TRUE''')\
  .withColumn("nn", split_name(col("spottedPackageTypes"), col("DBU")))\
    .select("date", "DBU", "nn.*")\
    .groupBy("date").agg(
    sum("DBU").alias("DBU"),
    sum("ML").alias("ML"),
    sum("misc").alias("misc"),
    sum("sparkML").alias("sparkML"),
    sum("pytorch").alias("pytorch"),
    sum("genAI").alias("genAI"),
    sum("sklearn").alias("sklearn"),
    sum("xgboost").alias("xgboost"),
    sum("tensorflow").alias("tensorflow"),
      sum("autoML").alias("autoML")
    
    
)

df = df.withColumn("rate", (col("ML") + col("misc")) / col("DBU"))\
   .withColumn("MLrate", col("ML") / col("DBU"))\
  .withColumn("sparkMLrate", col("sparkML") / col("DBU"))\
  .withColumn("pytorchrate", col("pytorch") / col("DBU"))\
  .withColumn("genAIrate", col("genAI") / col("DBU"))\
  .withColumn("autoMLrate", col("autoML") / col("DBU"))\
  .withColumn("sklearnrate", col("sklearn") / col("DBU"))\
  .withColumn("xgboostrate", col("xgboost") / col("DBU"))\
  .withColumn("tfboostrate", col("tensorflow") / col("DBU"))

display(df)

# COMMAND ----------


