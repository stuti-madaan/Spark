
# Load a dataframe from a CSV file (with header line).  Change the filename to one that matches your S3 bucket.
eventDF = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferSchema='true').load('/mnt/S3/data/Spark3/dataSet3Events.csv')

# COMMAND ----------

# Examine the data, and field/column names
display(eventDF)

# COMMAND ----------

#Question 1 : price statistics by make, model
from pyspark.sql import functions as F
aggprice_DF=eventDF.filter(eventDF.price>0).select( 'make', 'model', 'vin' , 'price','mileage').distinct().groupBy("make","model").agg(F.mean(eventDF.price).alias("avg_price"), F.min(eventDF.price).alias("min_price"),  F.max(eventDF.price).alias("max_price")).orderBy("make","model")
display(aggprice_DF)

# COMMAND ----------

#Q 2: Mileage statistics by year
aggmileage_DF=eventDF.filter(eventDF.mileage>0).select( 'year','make', 'model', 'vin' , 'price','mileage').distinct().groupBy("year").agg(F.mean(eventDF.mileage).alias("avg_mileage"), F.min(eventDF.mileage).alias("min_mileage"),  F.max(eventDF.mileage).alias("max_mileage")).orderBy("year")
display(aggmileage_DF)

# COMMAND ----------

#Q 3: Event statistics By Vin
aggevent_DF=eventDF.distinct().groupBy("vin","event").count().orderBy("vin","event")
display(aggevent_DF)

# COMMAND ----------

aggprice_DF.rdd.coalesce(1).saveAsTextFile("/mnt/S3/output/SparkAssignment2OutputPrice")
aggmileage_DF.rdd.coalesce(1).saveAsTextFile("/mnt/S3/output/SparkAssignment2OutputMileage")
aggevent_DF.rdd.coalesce(1).saveAsTextFile("/mnt/S3/output/SparkAssignment2OutputEvent")
