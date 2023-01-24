# Databricks notebook source
covid = (spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv('/databricks-datasets/COVID/covid-19-data/us-counties.csv'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show the schema for covid DataFrame

# COMMAND ----------

covid.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select cases for the counties with each state greater than 100 million cases from covid DataFrame

# COMMAND ----------

from pyspark.sql.functions import *
county_count_df = (covid.groupBy(col('state'), col('county'))
                   .agg(sum('cases').alias('total_cases'))
                   .where(col('total_cases') > 10**8))
display(county_count_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: select state with most cases from covid DataFrame

# COMMAND ----------

most_state_df = (covid.groupby(col('state'))
                .agg(sum('cases').alias('total_cases'))
                .orderBy(col('total_cases'), ascending=False)
                .limit(1))
display(most_state_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: select the records for the latest day from covid DataFrame

# COMMAND ----------

from pyspark.sql.window import Window
window_func = Window.orderBy(col('date').desc())
latest_df = (covid.withColumn('rank', dense_rank().over(window_func))
             .filter(col('rank') == 1))
display(latest_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: merge the state and county column to "State-County" from covid DataFrame. <br>
# MAGIC ### The records should also be concatenated with "-", for example "New York | New York City" now is "New York-New York City".

# COMMAND ----------

state_county_merged = (covid.withColumn('state-county',
    concat(col('state'), lit("-"), col('county'))))
display(state_county_merged)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the Container in your Storage Account (the one you used before for logging on Azure Storage) to your DBFS

# COMMAND ----------

dbutils.fs.mount(
  source = "PUT_YOUR_SOURCE_CONTAINER",
  mount_point = "/mnt/adblogging", extra_configs = {"fs.azure.account.key.adblogging.blob.core.windows.net":"PUT_YOUR_ACCOUNT_KEY"})


# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: save the last dataframe to the Container in your Storage Account in .parquet files

# COMMAND ----------

state_county_merged.write.mode("overwrite").parquet("dbfs:/mnt/adblogging")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: read the .parquet files from the mount and read them as DataFrame

# COMMAND ----------

df = spark.read.parquet("dbfs:/mnt/adblogging")
display(df)
