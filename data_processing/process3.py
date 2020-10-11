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

key3 = 'Chicago.csv'


 
sc = SparkContext()
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')
spark = SparkSession(sc)
 

s3file3 = f"s3a://{bucket}/{key3}"


df3 = spark.read.load(s3file3, sep=",", inferSchema="true", header="true", format="csv")


newColumns3=['LEGACY_SR_NUMBER','CREATED_DATE','CLOSED_DATE', 'STATUS', 'SR_TYPE', 'LOCATION', 'STREET_NAME', 'LATITUDE', 'LONGITUDE', 'CITY']
df3n=df3.select(newColumns3)
df3n=df3n.na.drop()


df3n = df3n.withColumn('CREATED_DATE', to_timestamp('CREATED_DATE',
                                                format='MM/dd/yyyy HH:mm'))
df3n = df3n.withColumn('Month', F.month('CREATED_DATE'))
df3n = df3n.withColumn('Year', F.year('CREATED_DATE'))

df3n=df3n.withColumnRenamed('LEGACY_SR_NUMBER', 'CaseId')
df3n=df3n.withColumnRenamed('CREATED_DATE', 'Opened_date')
df3n=df3n.withColumnRenamed('CLOSED_DATE', 'Closed_date')
df3n=df3n.withColumnRenamed('STATUS', 'Status')
df3n=df3n.withColumnRenamed('SR_TYPE', 'Service_type')
df3n=df3n.withColumnRenamed('LOCATION', 'Location')
df3n=df3n.withColumnRenamed('STREET_NAME', 'Street_address')
df3n=df3n.withColumnRenamed('LATITUDE,', 'Latitude')
df3n=df3n.withColumnRenamed('LONGITUDE', 'Longitude')
df3n=df3n.withColumnRenamed('CITY', 'City')


schema=['CaseId','Opened_date','Closed_date', 'Status', 'Service_type', 'Location', 'Street_address', 'Latitude', 'Longitude', 'City', 'Month', 'Year']
df3n=df3n.select(schema) 


df3n.printSchema()
print(df3n.show())
print(df3n.count())


spark.stop()

