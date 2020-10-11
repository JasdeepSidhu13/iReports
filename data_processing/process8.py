from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from sql_q import *
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import pandas as pd

 
region = 'us-east-2'
bucket = 'jsbucket311'

key8='Phoenix.csv'



 
sc = SparkContext()
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')
spark = SparkSession(sc)
 

s3file8 = f"s3a://{bucket}/{key8}"


df8 = spark.read.load(s3file8, sep=",", inferSchema="true", header="true", format="csv")


newColumns8=['NOPD_Item','CREATION DATE','CLOSED DATE', 'STATUS', 'Type', 'STREET ADDRESS','ADDRESS WITH GEOCODE', 'MapX', 'MapY','CLOSED MONTH', 'CLOSED YEAR']
df8n=df8.select(newColumns5)
df8n=df8n.na.drop()


df8n=df8n.withColumnRenamed('CASE ID', 'CaseId')
df8n=df8n.withColumnRenamed('CREATION DATE', 'Opened_date')
df8n=df8n.withColumnRenamed('CLOSED DATE', 'Closed_date')
df8n=df8n.withColumnRenamed('STATUS', 'Status')
df8n=df8n.withColumnRenamed('TYPE', 'Street_address')
df8n=df8n.withColumnRenamed('STREET ADDRESS', 'Service_type')
df8n=df8n.withColumnRenamed('ADDRESS WITH GEOCODE', 'Location')
df8n=df8n.withColumnRenamed('XCOORDINATE', 'Latitude')
df8n=df8n.withColumnRenamed('YCOORDINATE', 'Longitude')
df8n=df8n.withColumnRenamed('CLOSED MONTH', 'Month')
df8n=df8n.withColumnRenamed('CLOSED YEAR', 'Year')
df8n=df8n.withColumn('City', lit('Kansas'))



schema=['CaseId','Opened_date','Closed_date', 'Status', 'Service_type', 'Location', 'Street_address', 'Latitude', 'Longitude', 'City', 'Month', 'Year']
df8n=df8n.select(schema) 


df8n.printSchema()
print(df8n.show())
print(df8n.count())




spark.stop()

