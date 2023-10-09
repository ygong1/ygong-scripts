# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from typing import Dict, List
from pyspark.sql.types import ArrayType, StringType


# COMMAND ----------

# validate the clusterFeatureFlags.GPU is correct 
import re

def get_instance_family(cloud_type: str, instance_type: str) -> str:
  """
  Get the instance family from instance type.

  See go/gpu/instances for reference.
  """
  if cloud_type == "aws":
    # "g4dn.xlarge" --> "g4dn" --> "g4"
    return instance_type.split('.')[0][:2]
  elif cloud_type == "azure":
    if re.match("Standard_NC\d+$", instance_type):
      # Standard_NC[12,24]
      return "NC"
    if re.match("Standard_NC\d+s_v2", instance_type):
      # Standard_NC[12,24]s_v2
      return "NCv2"
    elif re.match("Standard_NC\d+s_v3", instance_type):
      # Standard_NC[6,12,24]s_v3
      return "NCv3"
    elif re.match("Standard_NC\d+as_T4_v3", instance_type):
      # Standard_NC[6,12,24]s_v3
      return "NCT4v3"
    elif re.match("Standard_NC\d+ads_A100_v4", instance_type):
      # Standard_NC[24,48,96]ads_A100_v4
      return "NCA100v4"
    elif re.match("Standard_ND\d+asr_v4", instance_type):
      # Standard_ND96asr_v4
      return "NDv4"
    elif re.match("Standard_NV\d+adm?s_A10_v5", instance_type):
      # Standard_NV[36,72]ad[m]s_v5
      return "NVA10v4"
    else:
      return instance_type
  elif cloud_type == "gcp":
    if re.match("a2-highgpu-\d+g", instance_type):
      return "a2"
    elif re.match("g2-standard-\d+", instance_type):
      return 'g2'
    else:
      return instance_type
  else:
    return "unknown-cloud-type"

INSTANCE_FAMILY_TO_GPU_TYPE = {
  # AWS
  "p2": "K80",
  "p3": "V100",  
  "p4": "A100",
  "g4": "T4",
  "g5": "A10",
  # AZURE
  "NC": "K80",
  "NCv2": "P100",
  "NCv3": "V100",
  "NCT4v3": "T4",
  "NCA100v4": "A100",
  "NDv4": "A100",
  "NVA10v4": "A10",
  # GCP
  "a2": "A100",
  "g2": "L4"
}

def get_gpu_type(instance_family: str) -> str:
  """Get the GPU type from instance family."""
  return INSTANCE_FAMILY_TO_GPU_TYPE.get(instance_family, "unknown") 


@udf
def is_instance_gpu(cloud_type: str, instance_type: str) -> str:
  family = get_instance_family(cloud_type, instance_type)
  gpu_type = get_gpu_type(family)
  if gpu_type != 'unknown':
    return "gpu"
  else:
    return instance_type


# COMMAND ----------

# clusterFeatureFlags.GPU is trustable. Only 0.4% DBU are mis-categorized
df = spark.sql(f'''SELECT date, cloudType, driverNodeType, workerNodeType, useGPU, DBU from users.yu_gong.insights_10days_process1 where MLR == TRUE''')\
  .withColumn("dgpu", is_instance_gpu(col("cloudType"), col("driverNodeType")) == 'gpu')\
  .withColumn("wgpu", is_instance_gpu(col("cloudType"), col("workerNodeType")) =='gpu')\
  .withColumn("isGPUByInstanceTypes", (col("dgpu")  | col("wgpu") ))\
  .withColumn("consistent", col("isGPUByInstanceTypes") == col('useGPU'))\
  .select("date", "DBU", "consistent")
display(df)
