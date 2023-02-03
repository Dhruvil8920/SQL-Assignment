from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import lit
from pyspark.sql.types import *

spark = SparkSession.builder.appName("SqlAssignment1").getOrCreate()

data = [({"firstname": "James", "middlename": "", "lastname": "Smith"}, "1998-01-03", "M", 3000),
         ({"firstname": "Michael", "middlename": "Rose", "lastname": ""}, "1998-11-10", "M", 20000),
        ({"firstname": "Robert", "middlename": "", "lastname": "Williams"}, "2000-01-02", "M", 3000),
        ({"firstname": "Maria", "middlename": "Anne", "lastname": "Jones"}, "1998-01-03", "F", 11000),
        ({"firstname": "Jen", "middlename": "Mary", "lastname": "Brown"}, "1998-10-04", "F", 10000)]

schema = StructType([StructField("name", MapType(StringType(), StringType())),
                           StructField("dob", StringType()),
                           StructField("gender", StringType()),
                           StructField("salary", IntegerType())])

# Creating DataFrame
def CreateDataFrame():
    df = spark.createDataFrame(data, schema)
    return df

# Selecting firstname, lastmane and salary from dataframe
def SelectColumn():
    df = CreateDataFrame()
    df2 = df.select(df.name["firstname"], df.name["lastname"], df.salary)
    return df2

# Adding Country, department and age column
def AddingColumn():
    df = CreateDataFrame()
    df = df.withColumn("Country", lit("")).withColumn("department", lit("")).withColumn("age", lit(""))
    return df

# Changing value of salary
def ChangingSalary():
    df = AddingColumn()
    df = df.withColumn("salary", df["salary"]+1000)
    return df

# Changing datatype of dob and salary to string
def ChangeDataType():
    df = ChangingSalary()
    df3 = df.withColumn("dob", df["dob"].cast(StringType()))
    df3 = df3.withColumn("salary", df3["salary"].cast(StringType()))
    return df3

# Deriving new column from salary column
def DerivingNewColumn():
    df = ChangingSalary()
    df = df.withColumn("newsalary", df["salary"]+5000)
    return df

# Renaming nested column( Firstname -> firstposition, middlename -> secondposition, lastname -> lastposition)
def Rename():
    df = DerivingNewColumn()
    df4 = df.withColumn("name", df["name"].cast(StringType()))
    df4 = df4.withColumn("name", F.regexp_replace("name", "firstname", "firstposition"))\
    .withColumn("name", F.regexp_replace("name", "middlename", "secondposition"))\
    .withColumn("name", F.regexp_replace("name", "lastname", "lastposition"))
    return df4

# Filtering the name column whose salary in maximum.
def MaxSalary():
    df = ChangingSalary()
    return df.filter(df['salary'] == df.agg(F.max(df['salary'])).collect()[0][0]).select('name')

# Dropping the department and age column.
def DropColumn():
    df = DerivingNewColumn()
    df = df.drop('department', 'age')
    return df

# List of distinct value of dob and salary
def Distinct1():
    df = ChangingSalary()
    df5 = df.select('dob').distinct()
    return df5

def Distinct2():
    df = ChangingSalary()
    df6 = df.select('salary').distinct()
    return df6

