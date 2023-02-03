from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, array, col, expr
from pyspark.sql.types import *

spark = SparkSession.builder.appName("SqlAssignment2").getOrCreate()

data = [("Banana", 1000, "USA"),
         ("Carrots", 1500, "INDIA"),
         ("Beans", 1600, "SWEDEN"),
         ("Orange", 2000, "UK"),
         ("Orange", 2000, "UAE"),
         ("Banana", 400, "CHINA"),
         ("Carrots", 1200, "CHINA")]

schema = StructType([StructField("Product", StringType()),
                     StructField("Amount", IntegerType()),
                     StructField("Country", StringType())])

# Creating DataFrame
def CreateDataFrame():
    df = spark.createDataFrame(data, schema)
    return df

a = CreateDataFrame()
a.createTempView("Assignment2")

# Total amount exported to each country of each product
def TotalAmount():
    df = spark.sql("SELECT * FROM Assignment2").groupBy("Country").pivot("Product").sum("Amount")
    return df

# Unpivot
def Unpivot():
    df = TotalAmount()
    df2 = df.selectExpr("Country", "stack(4, 'Banana', Banana, 'Beans', Beans, 'Carrots', Carrots, 'Orange', Orange) as (Product, Amount)").where("Amount is not null")
    return df2


