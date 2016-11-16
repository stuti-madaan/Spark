# Databricks notebook source exported at Mon, 14 Nov 2016 06:09:10 UTC
# MAGIC %md
# MAGIC 
# MAGIC # **Importing Data from S3 from Python Notebooks**
# MAGIC S3 is Amazon's cloud storage service, which we recommend using for storing your large data files.

# COMMAND ----------

# MAGIC %md ### Use Notebook Permissions to Protect Your Keys
# MAGIC You can protect the keys you will enter in this notebook by clicking "Permissions" in the context bar of a notebook to control who can see the contents of this notebook. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### **Preferred Method:** Mount your S3 bucket to the **Databricks File System**
# MAGIC * Access the data on your mounted S3 bucket as if your files were stored locally.
# MAGIC * The mount is a pointer to an S3 location, so the data is never synced locally.
# MAGIC * Never need to enter your AWS keys again with each use.
# MAGIC 
# MAGIC We support s3a protocol and recommend it over the native S3 block-based file system. See [AmazonS3 Wiki](https://wiki.apache.org/hadoop/AmazonS3) for more details on the differences between the two.

# COMMAND ----------

#AWS Link#
ACCESS_KEY = "*****"
SECRET_KEY = "*****"
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "dbmshadoop"
MOUNT_NAME = "S3"

# COMMAND ----------

#link to AWS#
dbutils.fs.unmount("/mnt/S3")
dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

#display contents
display(dbutils.fs.ls("/mnt/S3/data/Spark2"))

# COMMAND ----------

myRDD = sc.textFile("/mnt/S3/data/Spark2/dataSet10.txt")
myRDD.take(5)

# COMMAND ----------

# MAGIC %md Once the S3 bucket is mounted, access your data as if your files were stored locally.

# COMMAND ----------

# Removing blank lines
nonempty_RDD = myRDD.filter(lambda line:len(line.strip())>0) 
nonempty_RDD.take(5)

# COMMAND ----------

# splitting verseID and verse
strip_splitRDD = nonempty_RDD.map(lambda X:X.split(' ',1))
strip_splitRDD.first()

# COMMAND ----------

## function for to replace punctuation with blank space, lowercase and split
def replace_punc(x):
  punc = [',', '.', ':', ';', '!', '(', ')', '?']
  for p in punc:
    x= x.lower().replace(p,' ')
  return x.split()


# COMMAND ----------

#make tuples for every line
loweredRDD = strip_splitRDD.map(lambda line: (line[0],replace_punc(line[1])))
loweredRDD.take(2)

# COMMAND ----------

# removes duplicates and re-arrange
def split_func(k):
  l=[]
  for i in range(0,len(k[1])):
    l.append((k[1][i],k[0]))
  for i in l:
    l = set(l) 
  return l

# Making (word, verseID) tuples
word_verseRDD = loweredRDD.flatMap(lambda X: split_func(X)).sortByKey()
word_verseRDD.take(5)

# COMMAND ----------

# Sorted RDD
outputRDD = word_verseRDD.groupByKey().mapValues(list)
sortedRDD = outputRDD.sortByKey()
sortedRDD.take(5)

# COMMAND ----------

def count_cal(x):
  count=0
  for i in x[1]:
    count+=1
  return (count,(x[0],sorted(x[1])))

# Adding count of VerseIds corresponding to every word #
countRDD = sortedRDD.map(lambda X:count_cal(X))
countRDD.take(5)

# COMMAND ----------

finalRDD = countRDD.sortByKey(False)
# Sorting from Highest to lowest number of VerseIds

# COMMAND ----------

finalRDD2 = finalRDD.map(lambda X: (X[1][0],X[1][1],X[0]))
finalRDD2.take(5)
# Reordering (count, word, list of vrseIds) to (word, VerseIDs, count)

# COMMAND ----------

#Send output to S3#
sortedRDD.coalesce(1).saveAsTextFile("/mnt/S3/output/Spark2Basic")
finalRDD2.coalesce(1).saveAsTextFile("/mnt/S3/output/Spark2Bonus")
