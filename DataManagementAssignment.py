# Databricks notebook source exported at Sat, 3 Dec 2016 16:38:38 UTC
#Import file
InputRDD = sc.textFile("/FileStore/tables/p29as72z1479866575318/dataSetEvents.csv")

# COMMAND ----------

header = InputRDD.first() #extract header
InputRDD2 = InputRDD.filter(lambda row:  row != header)

# COMMAND ----------

#split input columns
FieldsRDD= InputRDD2.map(lambda line:line.split(','))

# COMMAND ----------

#function to separate event type and subtype
def split_date_event(x):
  event_type,subtype = x[1].split(' ',1)
  date_var, time = x[2].split(' ',1)
  return [x[0],event_type,subtype,date_var,time,x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11],x[12],x[13],x[14],x[15],x[16],x[17]]

# COMMAND ----------

#apply function on all rows
splitRDD = FieldsRDD.map(lambda x: split_date_event(x))

# COMMAND ----------

#separate time and date
YMMRDD=splitRDD.map(lambda x: ((x[9][:8] + '_' + x[9][9]), x[11],x[12],x[13])).distinct()
YMM= YMMRDD.toDF(['VIN_prefix','year','make','model'])
display(YMM)

# COMMAND ----------

#create VIN_prefix
VINRDD =splitRDD.map(lambda x: (x[9], (x[9][:8] + '_' + x[9][9]), x[14],x[15],x[16])).distinct()
VIN = VINRDD.toDF(['VIN','VIN_prefix','trim','body_style','cab_style'])
display(VIN)

# COMMAND ----------

#location Dataframe
LocationRDD = splitRDD.map(lambda x: (x[6],x[7],x[8])).distinct()
Location = LocationRDD.toDF(['ZIP','city','state'])
display(Location)

# COMMAND ----------

#Listing DataFrame
ListingRDD= splitRDD.map(lambda x:(x[9],x[3],x[6],x[10],x[18],x[19],x[17])).distinct()
Listing = ListingRDD.toDF(['VIN','date','location','condition','price','mileage','image_count'])
display(Listing)

# COMMAND ----------

#Event RDD
EventRDD = splitRDD.map(lambda x:(x[0],x[9],x[3],x[5],x[1],x[2],x[4]))

# COMMAND ----------

#Rdd to dataframe
eventDF = EventRDD.toDF(['user','VIN','date','site','event_type','event_subtype','time_of_day'])

# COMMAND ----------

#creating counts for Event DataFrame
Event = eventDF.groupBy('user','VIN','date','site','event_type','event_subtype','time_of_day').count().orderBy('user','VIN','date','site','event_type','event_subtype','time_of_day').distinct()
display(Event)

# COMMAND ----------

# From Event, extractig User and VIN. FInding the counts of events using count()
# inner join with Listing on VIN and extracting condition , price and mileage
# inner join with VIN to extract VIN_prefix
# inner join with YMM on VIN_prefix and extract year
# remove VIN_prefix
from pyspark.sql import functions as F
Ad_content = Event.select('user','VIN').groupBy('user','VIN').count().orderBy('user','VIN').distinct().join(Listing.select('VIN','condition','price','mileage'),'VIN').join(VIN.select('VIN','VIN_prefix'),'VIN').join(YMM.select('VIN_prefix','year'),'VIN_prefix').drop('VIN_prefix').distinct()
display(Ad_content)
