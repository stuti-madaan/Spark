# Databricks notebook source exported at Wed, 16 Nov 2016 03:52:03 UTC
#Linking to AWS# 
ACCESS_KEY = "******"
SECRET_KEY = "******"
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "*****"
MOUNT_NAME = "S3"
dbutils.fs.unmount("/mnt/S3")
dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)
display(dbutils.fs.ls("/mnt/S3/data/Spark3"))

# COMMAND ----------

# Load a dataframe from a CSV file (with header line).  Change the filename to one that matches your S3 bucket.
eventDF = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferSchema='true').load('/mnt/S3/data/Spark3/dataSet3Events.csv')

# COMMAND ----------

# Examine the data, and field/column names
display(eventDF)


# COMMAND ----------

#create SQL table#
eventDF.registerTempTable("table1")

# COMMAND ----------

#Question 1 : price statistics in SQL
queryDF = sqlContext.sql("SELECT make , model , min(price) as min_price, max(price) as max_price, avg(price) as avg_price FROM (Select distinct make, model, vin , price, mileage from table1 WHERE price >0) GROUP BY make, model ORDER BY make,model")

# COMMAND ----------

display(queryDF)

# COMMAND ----------

#Q 2: Mileage statistics in SQL
queryDF_2 = sqlContext.sql("SELECT year , min(mileage) as min_mileage, max(mileage) as max_mileage, avg(mileage) as avg_mileage FROM (Select distinct make, model, vin , price, mileage, year from table1 WHERE mileage >0) GROUP BY year ORDER BY year")

# COMMAND ----------

display(queryDF_2)

# COMMAND ----------

display(eventDF)

# COMMAND ----------

#Convert dataFrame to RDD and split event into event_type and event_detail
makeModelRDD = eventDF.rdd.map(lambda row: (row['userId'], row['event'].split(' ',1)[0],row['event'].split(' ',1)[1],row['timestamp'],row['vin'],row['condition'],row['year'],row['make'],row['model'],row['price'],row['mileage']))
makeModelRDD.take(5)

# COMMAND ----------

#convert RDD back to Dataframe
eventDF_new = makeModelRDD.toDF(['userId','event_type','event_detail','timestamp','vin','condition','year','make','model','price','mileage'])
display(eventDF_new)

# COMMAND ----------

#Dataframe to table
eventDF_new.registerTempTable("table2")

# COMMAND ----------

#Q3:Event counts for VIN using SQL 
queryDF_3 = sqlContext.sql("SELECT vin, event_type, count(event_detail) as sum_event_type FROM table2 GROUP BY vin, event_type ORDER BY vin,event_type ")
display(queryDF_3)

# COMMAND ----------

#convert output tables to RDDs
outputRDD1 = queryDF.rdd.map(lambda x:(x.asDict()))
outputRDD2 = queryDF_2.rdd.map(lambda x:x.asDict())
outputRDD1.take(5)

# COMMAND ----------

#Combine different events and event counts for all VINs
outputRDD3 = queryDF_3.rdd.map(lambda x:(tuple(x.asDict().values())[0], [(tuple(x.asDict().values())[1], tuple(x.asDict().values())[2])]))
outputRDD3_final = outputRDD3.reduceByKey(lambda a,b : a + b)
outputRDD3_final.take(5)

# COMMAND ----------

#Export to AWS as textfile
outputRDD1.coalesce(1).saveAsTextFile("/mnt/S3/output/SparkAssignment2OutputPrice")
outputRDD2.coalesce(1).saveAsTextFile("/mnt/S3/output/SparkAssignment2OutputMileage")
outputRDD3_final.coalesce(1).saveAsTextFile("/mnt/S3/output/SparkAssignment2OutputEvent_final")
