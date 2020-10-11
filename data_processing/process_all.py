from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from sql_q import *
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import pandas as pd

# Get region, bucket name and data stored in AWS s3.
 
region = 'us-east-2'
bucket = 'jsbucket311'
key1 = 'Berkeley.csv'
key2 = 'Boston2020.csv'
key3 = 'Chicago.csv'
key4=  'Dallas.csv'
key5='kansas.csv'
key7='NewOrleans.csv'
key6='NY.csv'
key8='Phoenix2020.csv'
key9='SF.csv'
key10='SanJose.csv'

# Create a fixed schema to be extracted from each data source

schema=['CaseId','Opened_date','Closed_date', 'Status', 'Service_type', 'Location', 'Street_address', 'Latitude', 'Longitude', 'City', 'Month', 'Year']
 
# Create a spark context object and connect to spark session 
sc = SparkContext()
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')
spark = SparkSession(sc)

# Get the path of the s3 files

s3file1 = f"s3a://{bucket}/{key1}"
s3file2 = f"s3a://{bucket}/{key2}"
s3file3 = f"s3a://{bucket}/{key3}"
s3file4 = f"s3a://{bucket}/{key4}"
s3file5 = f"s3a://{bucket}/{key5}"
s3file6 = f"s3a://{bucket}/{key6}"
s3file7 = f"s3a://{bucket}/{key7}"
s3file8 = f"s3a://{bucket}/{key8}"
s3file9 = f"s3a://{bucket}/{key9}"
s3file10 = f"s3a://{bucket}/{key10}"

# Read the data files from s3

df1 = spark.read.load(s3file1, sep=",", inferSchema="true", header="true", format="csv")
df2 = spark.read.load(s3file2, sep=",", inferSchema="true", header="true", format="csv")
df3 = spark.read.load(s3file3, sep=",", inferSchema="true", header="true", format="csv")
df4 = spark.read.load(s3file4, sep=",", inferSchema="true", header="true", format="csv")
df5 = spark.read.load(s3file5, sep=",", inferSchema="true", header="true", format="csv")
df6 = spark.read.load(s3file6, sep=",", inferSchema="true", header="true", format="csv")
df7 = spark.read.load(s3file7, sep=",", inferSchema="true", header="true", format="csv")
df8 = spark.read.load(s3file8, sep=",", inferSchema="true", header="true", format="csv")
df9 = spark.read.load(s3file9, sep=",", inferSchema="true", header="true", format="csv")
df10 = spark.read.load(s3file10, sep=",", inferSchema="true", header="true", format="csv")

###-------------------------------------------------------------------------------------
# City1- Extract relevant columns and drop all rows with missing data

newColumns1=['Case_ID','Date_Opened','Date_Closed', 'Case_Status', 'Request_Category', 'Location', 'Street_Address', 'Latitude', 'Longitude', 'City']
df1n=df1.select(newColumns1)
df1n=df1n.na.drop()

# Get year and month from Date
df1n = df1n.withColumn('Date_Opened', to_timestamp('Date_Opened',
                                                 format='MM/dd/yyyy HH:mm'))
df1n = df1n.withColumn('Month', F.month('Date_Opened'))
df1n = df1n.withColumn('Year', F.year('Date_Opened'))

# Rename columns to match the column names in schema
df1n=df1n.withColumnRenamed('Case_Id', 'CaseId')
df1n=df1n.withColumnRenamed('Date_Opened', 'Opened_date')
df1n=df1n.withColumnRenamed('Date_Closed', 'Closed_date')
df1n=df1n.withColumnRenamed('Case_Status', 'Status')
df1n=df1n.withColumnRenamed('Request_Category', 'Service_type')


#df1n.printSchema()
#print(df1n.show())
#print(df1n.count())

###-------------------------------------------------------------------------------------
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


df2n=df2n.select(schema) 


#df2n.printSchema()
#print(df2n.show())
#print(df2n.count())

###-------------------------------------------------------------------------------------
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

df3n=df3n.select(schema) 


#df3n.printSchema()
#print(df3n.show())
#print(df3n.count())

###-------------------------------------------------------------------------------------
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

df4n=df4n.select(schema) 

#df4n.printSchema()
#print(df4n.show())
#print(df4n.count())

###-------------------------------------------------------------------------------------
newColumns5=['CASE ID','CREATION DATE','CLOSED DATE', 'STATUS', 'TYPE', 'STREET ADDRESS','ADDRESS WITH GEOCODE', 'XCOORDINATE', 'YCOORDINATE','CLOSED MONTH', 'CLOSED YEAR']
df5n=df5.select(newColumns5)
#df5n=df5n.na.drop()


df5n=df5n.withColumnRenamed('CASE ID', 'CaseId')
df5n=df5n.withColumnRenamed('CREATION DATE', 'Opened_date')
df5n=df5n.withColumnRenamed('CLOSED DATE', 'Closed_date')
df5n=df5n.withColumnRenamed('STATUS', 'Status')
df5n=df5n.withColumnRenamed('TYPE', 'Service_type')
df5n=df5n.withColumnRenamed('STREET ADDRESS', 'Street_Address')
df5n=df5n.withColumnRenamed('ADDRESS WITH GEOCODE', 'Location')
df5n=df5n.withColumnRenamed('XCOORDINATE', 'Latitude')
df5n=df5n.withColumnRenamed('YCOORDINATE', 'Longitude')
df5n=df5n.withColumnRenamed('CLOSED MONTH', 'Month')
df5n=df5n.withColumnRenamed('CLOSED YEAR', 'Year')
df5n=df5n.withColumn('City', lit('Kansas'))


df5n=df5n.select(schema) 


#df5n.printSchema()
#print(df5n.show())
#print(df5n.count())

###-------------------------------------------------------------------------------------
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

df6n=df6n.select(schema) 


#df6n.printSchema()
#print(df6n.show())
#print(df6n.count())


###-------------------------------------------------------------------------------------
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

df9n=df9n.select(schema) 


#df9n.printSchema()
#print(df9n.show())
#print(df9n.count())

###-------------------------------------------------------------------------------------

dfs = [df1n,df2n,df3n, df4n, df5n, df6n, df9n]
df = reduce(DataFrame.union, dfs)
print(df.show())
print("dfcount" +str(df.count()))
###-------------------------------------------------------------------------------------

df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://10.0.0.4:5431/my_db") \
    .option("dbtable", "your_table_name") \
    .option("user", "your_user_name") \
    .option("password", "your_pwd") \
    .option("driver", "org.postgresql.Driver")\
    .save()

         
###-------------------------------------------------------------------------------------  


spark.stop()

