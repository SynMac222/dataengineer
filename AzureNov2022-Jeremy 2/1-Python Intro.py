# Databricks notebook source
# MAGIC %md
# MAGIC ## Native Datatypes in Python
# MAGIC ### 1. Python is Dynamically and Strongly typed
# MAGIC ### 2. You don’t need to explicitly declare the data type of variables in python
# MAGIC ### 3. Python will “guess” the datatype and keeps track of it internally.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bytes and byte arrays, e.g. a jpeg image file.

# COMMAND ----------

x = b'hello'
x

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lists are ordered sequences of objects.

# COMMAND ----------

arr = ['Apple', 'Banana']
arr

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: Add 'Cherry' in the array arr

# COMMAND ----------

arr.append('Cherry')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tuples are ordered, immutable sequences of values.

# COMMAND ----------

tup = ('Apple', 'Banana', 'Cherry')
tup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sets are unordered group of values

# COMMAND ----------

s = {'Apple', 'Banana'}
s

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: Add 'Cherry' in Set s

# COMMAND ----------

s.add('Cherry')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dicts are unordered key-value pairs (hash tables)

# COMMAND ----------

d = {'a': 'Apple', 'b':'Banana'}
d

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: Add 'Cherry' with key 'c' in dict d

# COMMAND ----------

d['c'] = 'Cherry'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Operators and Functions

# COMMAND ----------

# TODO: Implement this function.
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
# MAGIC ### Loop

# COMMAND ----------

# MAGIC %md
# MAGIC ### Complete the loop below

# COMMAND ----------

# TODO: For each item in tup (defined above), print the FIRST two letters using for loop
for i in tup:
    print(i[:2])

# COMMAND ----------

# TODO: For each item in tup (defined above), print the SECOND LAST letter using while loop
l = len(tup) -1
while l >= 0:
    print(tup[l][-2])
    l -= 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Iterator and Generator

# COMMAND ----------

# MAGIC %md
# MAGIC ### An object which allows a programmer to traverse through all the elements of a collection, regardless of its specific implementation.
# MAGIC ### An iterator is an object which implements the iterator protocol
# MAGIC ### The iterator protocol consists of two methods: __iter__() and __next__()

# COMMAND ----------

xi = iter([1, 2, 3])

# COMMAND ----------

print(xi)
print(xi.__next__())
print(next(xi))
print(xi.__next__())
# Will give an error because xi only has 3 elements. 
# print(next(xi))

# COMMAND ----------

def count(start=0):
    num = start
    while True:
        yield num
        num += 1

# COMMAND ----------

c = count()
c

# COMMAND ----------

# TODO: Write a for loop that prints out count from 0 to 9
for i in range(10):
    print(next(c))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assignment
# MAGIC Create a script that will read and parse the given files and remove duplicates using python, then write back into a single CSV and 8 smaller JSON files respectively. <br>
# MAGIC When two rows are duplicates, they have the same information but might have different separators/casing. <br>
# MAGIC For example: <br>
# MAGIC <ul>
# MAGIC   <li>“1234567890” instead of “123-456-7890” </li>
# MAGIC   <li>“JANE” instead of “Jane”</li>
# MAGIC   <li>“     Tom” instead of “Tom”</li>
# MAGIC </ul>
# MAGIC 
# MAGIC ### For this Assignment, DO NOT USE PANDAS

# COMMAND ----------

# TODO: Read people_1 and people_2
file_people1 = "/dbfs/FileStore/tables/people_1.txt"
file_people2 = "/dbfs/FileStore/tables/people_2.txt"
with open(file_people1, 'r') as f:
    people1 = [line.strip() for line in f]
with open(file_people1, 'r') as f:
    people2 = [line.strip() for line in f]
peoples = people1 + people2
peoples

# COMMAND ----------

# TODO: Clean the data
cleaned = []
for c in peoples:
    info = c.split('\t')
    fn = info[0].lower().strip(' ')
    ln = info[1].lower().strip(' ')
    em = info[2].lower().strip(' ')
    ph = info[3].strip(' ').replace('-', '')
    ad = info[4].lower().replace('no.', '').replace('#', '')
    total = [fn] + [ln] + [em] + [ph] + [ad]
    cleaned.append(total)
cleaned

# COMMAND ----------

# TODO: Remove duplicates
dedup = set()
results = []
for c in cleaned:
    st = ' '.join([i for i in c])
    if st not in dedup:
        results.append(c)
        dedup.add(st)
print(f'found: {len(peoples) - len(dedup)} duplicates')
results

# COMMAND ----------

# TODO: Output data as a csv file
import csv
with open('/dbfs/FileStore/tables/results.csv', 'w+') as c:
        cw = csv.writer(c, delimiter=',')
        cw.writerows(results)

# COMMAND ----------

# TODO: Output data as 8 smaller JSON files
import json
l = len(results)
count = 0
slices = 8
for i in range(0, l, (l//slices)+1):
    end = i + (l//slices)+1
    count += 1
    with open('/dbfs/FileStore/tables/'+str(count)+'.json', 'w+') as d:
        output = []
        for r in results[i:end]:
            output.append({a:b for (a,b) in zip(results[0], r)})
        json.dump(output, d, indent=4)

# COMMAND ----------

df = (spark.read
.option("multiline","true")
.json('dbfs:/FileStore/tables/*.json'))

display(df)
