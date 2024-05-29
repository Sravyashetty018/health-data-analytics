# Databricks notebook source
# DBTITLE 1,Importing the data and explorind the dataset

import pandas as pd
zipped_data = pd.read_csv("https://www.datafiles.samhsa.gov/sites/default/files/field-uploads-protected/studies/MH-CLD-2021/MH-CLD-2021-datasets/MH-CLD-2021-DS0001/MH-CLD-2021-DS0001-bundles-with-study-info/MH-CLD-2021-DS0001-bndl-data-csv_v1.zip")
df = spark.createDataFrame(zipped_data)

df.show()
df.printSchema

# COMMAND ----------

# DBTITLE 1,Checking what is the good candidate for parition
df.select("year","region","division").distinct().show()
df.select("year","region","division").groupBy("year","region","division").count().show()
df.select("year","region").groupBy("year","region").count().show()

# COMMAND ----------

# DBTITLE 1,Doing the initial transformation for storing in bronze layer
from pyspark.sql.functions import when,lit,col
transformed = df.withColumn(
    "GENDER",when(df.GENDER == 1, 'Male').when(df.GENDER == 2, 'Female').otherwise('Missing/unknown/not collected/invalid')).withColumn(
        "RACE",when(df.RACE == 1, 'American Indian/Alaska Native').when(df.RACE == 2, 'Asian').when(df.RACE == 3, 'Black or African American').when(df.RACE == 4, 'Native Hawaiian or Other Pacific Islander').when(df.RACE == 5, 'White').when(df.RACE == 6, 'Some other race alone/two or more races').otherwise('Missing/unknown/not collected/invalid')).withColumn(
            "ETHNIC",when(df.ETHNIC == 1, 'Mexican').when(df.ETHNIC == 2, 'Puerto Rican').when(df.ETHNIC == 3, 'Other Hispanic or Latino origin').when(df.ETHNIC == 4, 'Not of Hispanic or Latino origin').otherwise('Missing/unknown/not collected/invalid')).withColumn(
                "MARSTAT",when(df.MARSTAT == 1, 'Never married').when(df.MARSTAT == 2, 'Now married').when(df.MARSTAT == 3, 'Separated').when(df.MARSTAT == 4, 'Divorced, widowed').otherwise('Missing/unknown/not collected/invalid')).withColumn(
                    "EMPLOY",when(df.EMPLOY == 1, 'Full-time').when(df.EMPLOY == 2, 'Part-time').when(df.EMPLOY == 3, 'Employed full-time/part-time not differentiated').when(df.EMPLOY == 4, 'Unemployed').when(df.EMPLOY == 5, 'Not in labor force').otherwise('Missing/unknown/not collected/invalid')).withColumn(
                        "CASEID",col("CASEID").cast("Integer")).withColumn(
                            "MH1",col("MH1").cast("Float")).withColumn(
                                "MH2",col("MH2").cast("Float")).withColumn(
                                    "MH3",col("MH3").cast("Float"))

# COMMAND ----------

# DBTITLE 1,Writing to bronze layer
transformed.write.format("delta").partitionBy("year","region").mode("overwrite").saveAsTable("mh_bronze_data")

# COMMAND ----------

# DBTITLE 1,Normalizing the data with min and max scaling
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.sql.functions import udf,stddev_samp
from pyspark.sql.types import DoubleType, ArrayType

#UDF
sparse_values = udf(lambda v: v.values.tolist(), ArrayType(DoubleType()))

assemblerv1 = VectorAssembler(inputCols=["MH1"], outputCol="MHV1")
assemblerv2 = VectorAssembler(inputCols=["MH2"], outputCol="MHV2")
assemblerv3 = VectorAssembler(inputCols=["MH3"], outputCol="MHV3")

transformed_data = transformed.where("GENDER !='Missing/unknown/not collected/invalid' and RACE != 'Missing/unknown/not collected/invalid' and ETHNIC != 'Missing/unknown/not collected/invalid' and MARSTAT != 'Missing/unknown/not collected/invalid' and EMPLOY !='Missing/unknown/not collected/invalid'")

# Min-max scaler for MH1
transformed1 = assemblerv1.transform(transformed_data)
scaler = MinMaxScaler(inputCol="MHV1", outputCol="vScaled", min=0, max=1)
scaled_df = scaler.fit(transformed1).transform(transformed1)
normalized = scaled_df.withColumn("MH1N", sparse_values("vScaled").getItem(0))


# Min-max scaler for MH2
transformed2 = assemblerv2.transform(normalized)
scaler2 = MinMaxScaler(inputCol="MHV2", outputCol="vScaled2", min=0, max=1)
scaled_df2 = scaler2.fit(transformed2).transform(transformed2)
normalized2 = scaled_df2.withColumn("MH2N", sparse_values("vScaled2").getItem(0))


# Min-max scaler for MH3
transformed3 = assemblerv3.transform(normalized2)
scaler = MinMaxScaler(inputCol="MHV3", outputCol="vScaled3", min=0, max=1)
scaled_df = scaler.fit(transformed3).transform(transformed3)
normalized3 = scaled_df.withColumn("MH3N", sparse_values("vScaled3").getItem(0)).drop("MHV1","MHV2","MHV3","vScaled","vScaled2","vScaled3")

display(normalized3)

# COMMAND ----------

# DBTITLE 1,Generating the z order nomralization - formulae : z =(x - mean)/std_dev
from pyspark.sql.functions import mean as _mean, stddev as _stddev, col, lit

df_stats_mh1 = normalized3.select(
    _mean(col('MH1')).alias('mean'),
    _stddev(col('MH1')).alias('std')
).collect()

df_stats_mh2 = normalized3.select(
    _mean(col('MH2')).alias('mean'),
    _stddev(col('MH2')).alias('std')
).collect()

df_stats_mh3 = normalized3.select(
    _mean(col('MH3')).alias('mean'),
    _stddev(col('MH3')).alias('std')
).collect()

mean_mh1 = df_stats_mh1[0]['mean']
std_mh1 = df_stats_mh1[0]['std']

mean_mh2 = df_stats_mh2[0]['mean']
std_mh2 = df_stats_mh2[0]['std']

mean_mh3 = df_stats_mh3[0]['mean']
std_mh3 = df_stats_mh3[0]['std']

standardized  = normalized3.withColumn(
    "mh1_std", (col("MH1")-lit(mean_mh1))/lit(std_mh1)).withColumn(
        "mh2_std", (col("MH1")-lit(mean_mh2))/lit(std_mh2)).withColumn(
            "mh3_std", (col("MH1")-lit(mean_mh3))/lit(std_mh3))


# COMMAND ----------

# DBTITLE 1,Writing the silver data
standardized.write.format("delta").partitionBy("year","region").mode("overwrite").saveAsTable("mh_silver_data")

# COMMAND ----------

# DBTITLE 1,Doing further calculations and computing gold data
spark.table("mh_silver_data").where("year='2021' and gender='Male' and NUMMHS=0").write.format("delta").partitionBy("year","region").mode("overwrite").saveAsTable("mh_gold_data_num_mhs_zero")
spark.table("mh_silver_data").where("year='2021' and gender='Male' and NUMMHS!=0").write.format("delta").partitionBy("year","region").mode("overwrite").saveAsTable("mh_gold_datan_um_mhs_non_zero")


# COMMAND ----------

# DBTITLE 1,Creating sample testing and trainind data and also validation data
# Pyspark program to sampleBy using multiple columns 

# Import the libraries SparkSession library
from pyspark.sql import SparkSession

df_data = standardized.limit(100)

# Apply transformation on every element by defining the columns
# as well as sampling percentage as an argument in the map function
fractions1 = df_data.rdd.map(lambda x:
(x[5],x[4],x[3],x[15],x[18])).distinct().map(lambda x:
					(x,0.1)).collectAsMap()

fractions2 = df_data.rdd.map(lambda x:
(x[5],x[4],x[3],x[15],x[18])).distinct().map(lambda x:
					(x,0.1)).collectAsMap()

fractions3 = df_data.rdd.map(lambda x:
(x[5],x[4],x[3],x[15],x[18])).distinct().map(lambda x:
					(x,0.6)).collectAsMap()

# Create tuple of elements using keyBy function 
key_df = df_data.rdd.keyBy(lambda x: (x[5],x[4],x[3],x[15],x[18]))


# Extract random sample through sampleByKey function 
# using boolean, columns and fraction as arguments 
training_sample = key_df.sampleByKey(False,
fractions1).map(lambda x:
				x[1]).toDF(normalized.columns)

# Again extract random sample through sampleByKey function 
# using boolean, columns and fraction as arguments 
testing_sample = key_df.sampleByKey(False,
fractions2).map(lambda x:
				x[1]).toDF(normalized.columns)

validation_sample = key_df.sampleByKey(False,
fractions3).map(lambda x:
				x[1]).toDF(normalized.columns)

# COMMAND ----------

training_sample.repartition(1).write.format("csv").option("header","true").save("/tmp/training_sample/")
testing_sample.repartition(1).write.format("csv").option("header","true").save("/tmp/testing_sample/")
validation_sample.repartition(1).write.format("csv").option("header","true").save("/tmp/validation_sample/")
