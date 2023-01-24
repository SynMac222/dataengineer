# Databricks notebook source
# MAGIC %md
# MAGIC ### If you have some suspicious code that may raise an exception, you can defend your program by placing the suspicious code in a try: block. 
# MAGIC ### After the try: block, include an except: statement, followed by a block of code which handles the problem as elegantly as possible.

# COMMAND ----------

xi = iter([1, 2, 3])

# COMMAND ----------

# Will give an error because xi only has 3 elements. 
# TODO: Have a try-except error handling so that we can skip this error and run the print statement.
try:
    while True:
        print(next(xi))
except:
    print("iterator looping complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assert takes a condition/expression and check if it’s true
# MAGIC ### When assertion fails, AssertionError exception is thrown.

# COMMAND ----------

# returns the sum only when a and b are two-digit numbers; otherwise return None
def addTwoDigitNumbers(a, b):
    return a+b if len(str(a)) == len(str(b)) == 2 else None

# COMMAND ----------

# Unit-testing addTwoDigitNumbers function
# assert only gives error when two items are NOT matched
assert(addTwoDigitNumbers(10, 20) == 30)
assert(addTwoDigitNumbers(0, 20) == None)
assert(addTwoDigitNumbers(0, 100) == None)
assert(addTwoDigitNumbers(20, 0) == None)
assert(addTwoDigitNumbers(100, 0) == None)

# COMMAND ----------

# MAGIC %md
# MAGIC Python comes with a logging module in the standard library. <br>
# MAGIC To emit a log message, a caller first requests a named logger. <br>
# MAGIC The name can be used by the application to configure different rules for different loggers. <br>
# MAGIC This logger then can be used to emit simply-formatted messages at different log levels (DEBUG, INFO, ERROR, etc.), which again can be used by the application to handle messages of higher priority different than those of a lower priority.

# COMMAND ----------

# The message is turned into a LogRecord object and routed to a Handler object registered for this logger. 
# The handler will then use a Formatter to turn the LogRecord into a string and emit that string.
import logging
log = logging.getLogger('my-logger')
log.info('Hello, world!')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assignment: For the above iteration and assertion, have a logging handling to record job completes or fails (with error message) using Azure Storage Logging.

# COMMAND ----------

import logging
from azure_storage_logging.handlers import TableStorageHandler
# below code snipet came from documentation
# fetch your account_key from Access Key under storage account on Azure Portal

# configure the handler and add it to the logger
logger = logging.getLogger('example')
handler = TableStorageHandler(account_name='mystorageaccountname',
                              account_key='mystorageaccountkey',
                              extra_properties=('%(hostname)s',
                                                '%(levelname)s'))
logger.addHandler(handler)

# output log messages
logger.info('info message')
logger.warning('warning message')
logger.error('error message')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Assignment: Python Coding Practices

# COMMAND ----------

# MAGIC %md
# MAGIC Given an array of letters, e.g. ["a", "b", "a", "c", "b", "b"], 
# MAGIC return the ocrrencies of the elementes within that array, e.g. {"a": 2, "b": 3, "c": 1}.

# COMMAND ----------

def p1(letters):
    dic = {}
    for l in letters:
        if l not in dic:
            dic[l] = 1
        dic[l] += 1
    return dic
p1(["a", "b", "a", "c", "b", "b"])

# COMMAND ----------

# MAGIC %md
# MAGIC Given two arrays, subjects e.g. ["English", "Math"] and corresponding scores e.g. [88, 92], 
# MAGIC return the combined result in one array in descending order of scores e.g. [["Math", 92], ["English", 88]].

# COMMAND ----------

def p2(subjs, scores):
    r = []
    for sub, s in zip(subjs, scores):
        r.append([sub, s])
    r.sort(key=lambda x: -x[1])
    return r
p2(["English", "Math"], [88, 92])

# COMMAND ----------

# MAGIC %md
# MAGIC Given an array of URLs e.g. ["a.com/u=test&s=true","b.com/u=dev&s=false"], 
# MAGIC return an array of dictionaries of data parsed from URL e.g. 
# MAGIC [{url:"a.com", u:"test", s:"true"}, {url:"b.com", u:"dev", s:"false"}].

# COMMAND ----------

def p3(urls):
    r = []
    for u in urls:
        dic = {'url':u.split('/')[0]}
        for p in u.split('/')[1].split('&'):
            k, v = p.split('=')
            dic[k] = v
        r.append(dic)
    return r
p3(["a.com/u=test&s=true","b.com/u=dev&s=false"])

# COMMAND ----------

# MAGIC %md
# MAGIC Given an array of integers e.g. [2, 5, 10] and a power e.g. 3,
# MAGIC return the powered result of a dictionary in descending order based on the result 
# MAGIC e.g. {10: 100, 5: 125, 2: 8}

# COMMAND ----------

def p4(ints, p):
    dic = {a:a**p for a in ints}
    return {k:v for k, v in sorted(dic.items(), key=lambda x: -x[1])}
p4([2,5,10], 3)
