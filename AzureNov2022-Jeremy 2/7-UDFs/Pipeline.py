# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %run ./Utilities

# COMMAND ----------

""" Create DataFrame """
columns = ["Seqno", "Name"]
data = [("1", "john jones"), ("2", "tracey smith"), ("3", "amy sanders")]

df = spark.createDataFrame(data=data, schema=columns)

# COMMAND ----------

""" Apply UDFs """
df_upper = df.select(col("Seqno"), convertUDF(col("Name")).alias("Name"))

# COMMAND ----------

""" Using UDF on SQL """
df.createOrReplaceTempView("Names")
display(spark.sql("select Seqno, convertUDF(Name) as Name from Names"))

# COMMAND ----------

""" null check """

columns = ["Seqno", "Name", "Department", "Salary"]
data = [
    ("1", "john jones", "Data", "2000"),
    ("2", "tracey smith", None, "-3000"),
    ("3", "amy sanders", "#$@#@", None),
    ("4", None, "Backend", "0"),
]

df2 = spark.createDataFrame(data=data, schema=columns)

# COMMAND ----------

# This will fail...
# Update convertUDF in Utilities notebook, and replace the the null with a string 'NULL'
df2_upper = df2.select(col("Seqno"), convertUDF(col("Name")).alias("Name"))

# COMMAND ----------

# Create a new UDF in Utilities notebook
# Change field to 'Unknown' for Department that has None or broken value e.g., #$@#@.
df2_department = df2.select(
    col("Seqno"),
    convertUDF(col("Name")).alias("Name"),
    convertDepartmentUDF(col("Department")).alias("Department"),
)

# COMMAND ----------

# Create a new UDF in Utilities notebook
# Change field to 0 for Salary that has None or negative value
# Update the Salary with $ to indicate it is in US Dollar
df2_salary = df2.select(
    col("Seqno"),
    convertUDF(col("Name")).alias("Name"),
    convertDepartmentUDF(col("Department")).alias("Department"),
    convertSalaryUDF(col("Salary")).alias("Salary"),
)

# COMMAND ----------

# Check you final dataframe result
display(df2_salary)
