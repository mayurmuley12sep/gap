# Databricks notebook source
# MAGIC %run "./reusable_functions"

# COMMAND ----------

df = extract_data("/mnt/storageaccgaodev/bronze/circuits.csv")

# COMMAND ----------

df.show(5)

# COMMAND ----------

pip install pydeequ


# COMMAND ----------

pip install sagemaker_pyspark

# COMMAND ----------

import sagemaker_pyspark
import pydeequ

# COMMAND ----------

classpath = ":".join(sagemaker_pyspark.classpath_jars())

# COMMAND ----------

from pydeequ.analyzers import *

# COMMAND ----------


df = spark.read.parquet("s3a://amazon-reviews-pds/parquet/product_category=Electronics/")
df.show()

# COMMAND ----------

analysisResult = AnalysisRunner(spark) \
                    .onData(df) \
                    .addAnalyzer(Size()) \
                    .addAnalyzer(Completeness("circuitId")) \
                    .addAnalyzer(ApproxCountDistinct("location")) \
                    .addAnalyzer(Correlation("location", "country")) \
                    .run()

analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
analysisResult_df.show()

# COMMAND ----------

from pydeequ.checks import *
from pydeequ.verification import *

check = Check(spark, CheckLevel.Warning, "Review Check")

checkResult = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(
        check.hasSize(lambda x: x >= 3000000) \
        .hasMin("star_rating", lambda x: x == 1.0) \
        .hasMax("star_rating", lambda x: x == 5.0)  \
        .isComplete("circuitId")  \
        .isUnique("circuitId")  \
        .isComplete("marketplace")  \
        .isContainedIn("marketplace", ["US", "UK", "DE", "JP", "FR"]) \
        .isNonNegative("year")) \
    .run()
    
checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.show()