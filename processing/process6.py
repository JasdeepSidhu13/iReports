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


key6='NY.csv'


sc = SparkContext()
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')
spark = SparkSession(sc)
 

s3file6 = f"s3a://{bucket}/{key6}"

df6 = spark.read.load(s3file6, sep=",", inferSchema="true", header="true", format="csv")


newColumns6=['Unique Key','Created Date','Closed Date', 'Status', 'Complaint Type', 'Incident Address','Street Name', 'Latitude', 'Longitude', 'Location' ]
df6n=df6.select(newColumns6)
df6n=df6n.na.drop()


df6n = df6n.withColumn('Created Date', to_timestamp('Created Date',
                                                format='MM/dd/yyyy HH:mm'))
df6n = df6n.withColumn('Month', F.month('Created Date'))
df6n = df6n.withColumn('Year', F.year('Created Date'))


df6n=df6n.withColumnRenamed('Unique Key', 'CaseId')
df6n=df6n.withColumnRenamed('Created Date', 'Opened_date')
df6n=df6n.withColumnRenamed('Closed Date', 'Closed_date')
df6n=df6n.withColumnRenamed('Status', 'Status')
df6n=df6n.withColumnRenamed('Incident Address', 'Street_address')
df6n=df6n.withColumnRenamed('Complaint Type', 'Service_type')
df6n=df6n.withColumn('City', lit('NY'))



schema=['CaseId','Opened_date','Closed_date', 'Status', 'Service_type', 'Location', 'Street_address', 'Latitude', 'Longitude', 'City', 'Month', 'Year']
df6n=df6n.select(schema) 


df6n.printSchema()
print(df6n.show())
print(df6n.count())




spark.stop()

