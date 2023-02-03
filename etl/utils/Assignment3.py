from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Assignment3").getOrCreate()

data = [("James", "Sales", 3000),
        ("Michael", "Sales", 4600),
        ("Robert", "Sales", 4100),
        ("Maria", "Finance", 3000),
        ("Raman", "Finance", 3000),
        ("Scott", "Finance", 3300),
        ("Jen", "Finance", 3900),
        ("Jeff", "Marketing", 3000),
        ("Kumar", "Marketing", 2000)]

schema = StructType([StructField("employee_name", StringType()),
                     StructField("department", StringType()),
                     StructField("salary", IntegerType())])

# Creating DataFrame
def CreateDataFrame():
    df = spark.createDataFrame(data, schema)
    return df

# Creating a temp table
a = CreateDataFrame()
a.createTempView("Assignment3")

# Selecting first row from each department group.
def SelectingRow():
    df2 = spark.sql("select * from Assignment3 limit 1")
    return df2

# Retrieving Employees who earns the highest salary.
def MaxSalary():
    df3 = spark.sql("SELECT employee_name FROM Assignment3 WHERE salary = (SELECT MAX(salary) FROM Assignment3)")
    return df3

# Selecting the highest, lowest, average, and total salary for each department group.
def Aggregate():
    df4 = spark.sql("SELECT department, MAX(salary), MIN(salary), AVG(salary), SUM(salary) FROM Assignment3 group by department")
    return df4

