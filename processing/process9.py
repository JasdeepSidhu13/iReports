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

key9='SF.csv'



 
sc = SparkContext()
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')
spark = SparkSession(sc)
 
s3file9 = f"s3a://{bucket}/{key9}"


df9 = spark.read.load(s3file9, sep=",", inferSchema="true", header="true", format="csv")



newColumns9=['CaseID','Opened','Closed', 'Status', 'Request Type', 'Street','Address', 'Latitude', 'Longitude']
df9n=df9.select(newColumns9)
df9n=df9n.na.drop()

df9n = df9n.withColumn('Opened', to_timestamp('Opened',
                                                 format='MM/dd/yyyy HH:mm'))
df9n = df9n.withColumn('Month', F.month('Opened'))
df9n = df9n.withColumn('Year', F.year('Opened'))

df9n=df9n.withColumnRenamed('CASEID', 'CaseId')
df9n=df9n.withColumnRenamed('Opened', 'Opened_date')
df9n=df9n.withColumnRenamed('Closed', 'Closed_date')
df9n=df9n.withColumnRenamed('Status', 'Status')
df9n=df9n.withColumnRenamed('Street', 'Street_address')
df9n=df9n.withColumnRenamed('Request Type', 'Service_type')
df9n=df9n.withColumnRenamed('Address', 'Location')


df9n=df9n.withColumn('City', lit('SanFrancisco'))



schema=['CaseId','Opened_date','Closed_date', 'Status', 'Service_type', 'Location', 'Street_address', 'Latitude', 'Longitude', 'City', 'Month', 'Year']
df9n=df9n.select(schema) 


df9n.printSchema()
print(df9n.show())
print(df9n.count())


spark.stop()

