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
key2 = 'Boston2020.csv'



 
sc = SparkContext()
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')
spark = SparkSession(sc)
 
s3file2 = f"s3a://{bucket}/{key2}"


df2 = spark.read.load(s3file2, sep=",", inferSchema="true", header="true", format="csv")


newColumns2=['case_enquiry_id','open_dt','closed_dt', 'case_status', 'case_title', 'location', 'location_street_name', 'latitude', 'longitude']
df2n=df2.select(newColumns2)
df2n=df2n.na.drop()


df2n = df2n.withColumn('open_dt', to_timestamp('open_dt',
                                                 format='MM/dd/yyyy HH:mm'))
df2n = df2n.withColumn('Month', F.month('open_dt'))
df2n = df2n.withColumn('Year', F.year('open_dt'))

df2n=df2n.withColumnRenamed('case_enquiry_id', 'CaseId')
df2n=df2n.withColumnRenamed('open_dt', 'Opened_date')
df2n=df2n.withColumnRenamed('closed_dt', 'Closed_date')
df2n=df2n.withColumnRenamed('case_status', 'Status')
df2n=df2n.withColumnRenamed('case_title', 'Service_type')
df2n=df2n.withColumnRenamed('location', 'Location')
df2n=df2n.withColumnRenamed('location_street_name', 'Street_address')
df2n=df2n.withColumnRenamed('latitude', 'Latitude')
df2n=df2n.withColumnRenamed('longitude', 'Longitude')
df2n=df2n.withColumn('City', lit('Boston'))

schema=['CaseId','Opened_date','Closed_date', 'Status', 'Service_type', 'Location', 'Street_address', 'Latitude', 'Longitude', 'City', 'Month', 'Year']
df2n=df2n.select(schema) 


df2n.printSchema()
print(df2n.show())
print(df2n.count())


spark.stop()

