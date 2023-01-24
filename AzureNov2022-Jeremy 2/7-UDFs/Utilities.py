# Databricks notebook source
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# COMMAND ----------

""" 1. python functions """
def convertCase(str):
    if str == None:
        return "NULL"
    resStr=""
    arr = str.split(" ")
    for x in arr:
        resStr= resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr 

def convertDepartment(d):
    if d == None or not d.isalpha():
        return "Unknown"
    return d

def convertSalary(s):
    if s == None:
        return "$0"
    if float(s) <= 0:
        return "$0"
    return "$" + s
# Add python functions below

# COMMAND ----------

""" 2. Converting functions to UDFs """
convertUDF = udf(lambda z: convertCase(z))
# Add UDFs registration below
convertDepartmentUDF = udf(lambda z: convertDepartment(z))
convertSalaryUDF = udf(lambda z: convertSalary(z))

# COMMAND ----------

""" 3. [Optional] Using UDF on SQL """
spark.udf.register("convertUDF", convertCase, StringType())
