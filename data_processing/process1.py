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
key1 = 'Berkeley.csv'


sc = SparkContext()
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')
spark = SparkSession(sc)
 
s3file1 = f"s3a://{bucket}/{key1}"


df1 = spark.read.load(s3file1, sep=",", inferSchema="true", header="true", format="csv")



newColumns1=['Case_ID','Date_Opened','Date_Closed', 'Case_Status', 'Request_Category', 'Location', 'Street_Address', 'Latitude', 'Longitude', 'City']
df1n=df1.select(newColumns1)
df1n=df1n.na.drop()


df1n = df1n.withColumn('Date_Opened', to_timestamp('Date_Opened',
                                                 format='MM/dd/yyyy HH:mm'))
df1n = df1n.withColumn('Month', F.month('Date_Opened'))
df1n = df1n.withColumn('Year', F.year('Date_Opened'))

df1n=df1n.withColumnRenamed('Case_Id', 'CaseId')
df1n=df1n.withColumnRenamed('Date_Opened', 'Opened_date')
df1n=df1n.withColumnRenamed('Date_Closed', 'Closed_date')
df1n=df1n.withColumnRenamed('Case_Status', 'Status')
df1n=df1n.withColumnRenamed('Request_Category', 'Service_type')


df1n.printSchema()
print(df1n.show())
print(df1n.count())

spark.stop()

