# Databricks notebook source
# MAGIC %md
# MAGIC ### Read the csv file genereated before to read as DataFrame

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/results.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files.
df = (spark.read.format(file_type)
      .option("inferSchema", infer_schema)
      .option("header", first_row_is_header)
      .option("sep", delimiter)
      .load(file_location)
     )

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: Create a new DataFrame from df to calculate the count for people who have the same Last Name

# COMMAND ----------

from pyspark.sql.functions import *
LastNameCounts = df.groupBy(col('lastname')).count()
display(LastNameCounts)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: Save DataFrame LastNameCounts as a table in Database Tables (i.e. shown in the Data tab)

# COMMAND ----------

LastNameCounts.write.saveAsTable('LastNameCounts')

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: Configure the parameters below to have the correct format (e.g., correct schema) for DataFrame

# COMMAND ----------

# TODO
covid = (spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv('/databricks-datasets/COVID/covid-19-data/us-counties.csv'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: Create a new DataFrame from covid to count the people in California who lives in the same county. 

# COMMAND ----------

CACovid = (covid
           .where(col('state') == 'California')
           .groupBy(col('county'), col('state')).count()
          )

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: Using display() configure a Pie Chart to show portions of people who live in the same county.

# COMMAND ----------

display(CACovid)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: Read the 8 json files from people as DataFrame with corresponding schema.

# COMMAND ----------

from pyspark.sql.types import *
schema = StructType(
    [StructField('firstname', StringType(), True),
     StructField('lastname', StringType(), True),
     StructField('Phone', StringType(), True),
     StructField('email', StringType(), True),
     StructField('address', StringType(), True)
    ])
jDF = spark.read.schema(schema).json('dbfs:/FileStore/tables/*.json', multiLine=True)
display(jDF)
