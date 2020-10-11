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

key4=  'Dallas.csv'

sc = SparkContext()
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')
spark = SparkSession(sc)
 

s3file4 = f"s3a://{bucket}/{key4}"

df4 = spark.read.load(s3file4, sep=",", inferSchema="true", header="true", format="csv")



newColumns4=['Service Request Number','Created Date','Closed Date', 'Status', 'Service Request Type', 'Address','City Council District', 'X Coordinate', 'Y Coordinate' ]
df4n=df4.select(newColumns4)
df4n=df4n.na.drop()


df4n = df4n.withColumn('Created Date', to_timestamp('Created Date',
                                                format='MM/dd/yyyy HH:mm'))
df4n = df4n.withColumn('Month', F.month('Created Date'))
df4n = df4n.withColumn('Year', F.year('Created Date'))


df4n=df4n.withColumnRenamed('Service Request Number', 'CaseId')
df4n=df4n.withColumnRenamed('Created Date', 'Opened_date')
df4n=df4n.withColumnRenamed('Closed Date', 'Closed_date')
df4n=df4n.withColumnRenamed('Status', 'Status')
df4n=df4n.withColumnRenamed('Address', 'Street_address')
df4n=df4n.withColumnRenamed('Service Request Type', 'Service_type')
df4n=df4n.withColumnRenamed('City Council District', 'Location')
df4n=df4n.withColumnRenamed('X Coordinate', 'Latitude')
df4n=df4n.withColumnRenamed('Y Coordinate', 'Longitude')
df4n=df4n.withColumn('City', lit('Dallas'))



schema=['CaseId','Opened_date','Closed_date', 'Status', 'Service_type', 'Location', 'Street_address', 'Latitude', 'Longitude', 'City', 'Month', 'Year']
df4n=df4n.select(schema) 


df4n.printSchema()
print(df4n.show())
print(df4n.count())




spark.stop()

