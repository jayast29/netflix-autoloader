# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Transformation

# COMMAND ----------

df = spark.read.format("delta")\
        .option("header",True)\
        .option("inferSchema",True)\
        .load("abfss://bronze@netflixpipelineadls.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

df = df.fillna({"duration_minutes" : 0, "duration_seasons":1})

# COMMAND ----------

df = df.withColumn("duration_minutes",col('duration_minutes').cast(IntegerType()))\
            .withColumn("duration_seasons",col('duration_seasons').cast(IntegerType()))

# COMMAND ----------

df.printSchema()
display(df.limit(10))

# COMMAND ----------

df = df.withColumn("title_short",split(col('title'),':')[0])

# COMMAND ----------

df = df.withColumn("rating",split(col('rating'),'-')[0])

# COMMAND ----------

df = df.withColumn("type_flag",when(col('type')=='TV Show',0)\
                        .when(col('type')=='Movie',1)\
                        .otherwise(2))

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df = df.withColumn("duration_ranking",dense_rank().over(Window.orderBy(col('duration_minutes').desc())))

# COMMAND ----------

df.createOrReplaceGlobalTempView("global_view")

# COMMAND ----------

df = spark.sql(""" 
                select * from global_temp.global_view
               
               """)

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

df_count = df.groupBy("type").agg(count("*").alias("total_count"))
display(df_count)

# COMMAND ----------

df.write.format("delta")\
        .mode("overwrite")\
        .option("path","abfss://silver@netflixpipelineadls.dfs.core.windows.net/netflix_titles")\
        .save()