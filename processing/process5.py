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

key5='kansas.csv'

 
sc = SparkContext()
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')
spark = SparkSession(sc)
 

s3file5 = f"s3a://{bucket}/{key5}"



df5 = spark.read.load(s3file5, sep=",", inferSchema="true", header="true", format="csv")


newColumns5=['CASE ID','CREATION DATE','CLOSED DATE', 'STATUS', 'TYPE', 'STREET ADDRESS','ADDRESS WITH GEOCODE', 'XCOORDINATE', 'YCOORDINATE','CLOSED MONTH', 'CLOSED YEAR']
df5n=df5.select(newColumns5)
df5n=df5n.na.drop()


df5n=df5n.withColumnRenamed('CASE ID', 'CaseId')
df5n=df5n.withColumnRenamed('CREATION DATE', 'Opened_date')
df5n=df5n.withColumnRenamed('CLOSED DATE', 'Closed_date')
df5n=df5n.withColumnRenamed('STATUS', 'Status')
df5n=df5n.withColumnRenamed('TYPE', 'Street_address')
df5n=df5n.withColumnRenamed('STREET ADDRESS', 'Service_type')
df5n=df5n.withColumnRenamed('ADDRESS WITH GEOCODE', 'Location')
df5n=df5n.withColumnRenamed('XCOORDINATE', 'Latitude')
df5n=df5n.withColumnRenamed('YCOORDINATE', 'Longitude')
df5n=df5n.withColumnRenamed('CLOSED MONTH', 'Month')
df5n=df5n.withColumnRenamed('CLOSED YEAR', 'Year')
df5n=df5n.withColumn('City', lit('Kansas'))



schema=['CaseId','Opened_date','Closed_date', 'Status', 'Service_type', 'Location', 'Street_address', 'Latitude', 'Longitude', 'City', 'Month', 'Year']
df5n=df5n.select(schema) 


df5n.printSchema()
print(df5n.show())
print(df5n.count())




spark.stop()

