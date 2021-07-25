from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DecimalType


sfOptions = {
  "sfURL" : "tl53533.ap-south-1.aws.snowflakecomputing.com",
  "sfUser" : "shanky8232",
  "sfPassword" : "200M%lk072wit",
  "sfDatabase" : "USER_SHANKY",
  "sfSchema" : "HUB",
  "sfWarehouse" : "INTERVIEW_WH"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"


schema = StructType([
         StructField('IATA_CODESSSS', StringType(), True),
         StructField('AIRLINE', StringType(), True),
         ])
df1 = spark.read.format("csv").load("dbfs:/FileStore/tmp/airlines/airlines.csv",header='true',schema=schema)
#df1.filter("IATA_CODESSSS == 'UA'").show()
##df1.show()




df1.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "AIRLINES").mode("overwrite").save()

schema2 = StructType([
         StructField('IATA_CODE', StringType(), True),
         StructField('AIRPORT', StringType(), True),
         StructField('CITY', StringType(), True),
         StructField('STATE', StringType(), True),
         StructField('COUNTRY', StringType(), True),
         StructField('LATITUDE', DecimalType(38,10), True),   
         StructField('LONGITUDE', DecimalType(38,10), True),  
         ])
df2 = spark.read.format("csv").load("dbfs:/FileStore/tmp/airports/airports.csv",header='true',schema=schema2)
df2.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "AIRPORTS").mode("overwrite").save()

df1 = spark.read.format("csv").load("dbfs:/FileStore/data/flight/*",header='true')
dff1=df1.drop_duplicates()
#dff1=df1.count()
#print ("Count is",dff1)
#Count is 429191
# 933503
dff1.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "flights").mode("overwrite").save()






