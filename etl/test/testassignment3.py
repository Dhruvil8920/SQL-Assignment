from etl.utils.Assignment3 import *
import unittest

class TestMyFunc(unittest.TestCase):

    # Testing DataFrame
    def testDataFrame(self):

        def checkDf(spark):

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
            df = spark.createDataFrame(data, schema)
            return df

        self.assertEqual(CreateDataFrame().collect(), checkDf(spark).collect())

    # Testing SelectedRow
    def testRow(self):

        def chechrow(spark):
            data = [Row(employee_name="James", department="Sales", salary=3000)]
            df = spark.createDataFrame(data)
            return df

        self.assertEqual(SelectingRow().collect(), chechrow(spark).collect())

    # Testing MaxSalary employee
    def testMaxSalary(self):

        def checkmaxsalary(spark):
            data = [Row(employee_name="Michael")]
            df = spark.createDataFrame(data)
            return df

        self.assertEqual(MaxSalary().collect(), checkmaxsalary(spark).collect())

    # Testing highest, lowest, average, and total salary for each department group.
    def testAggregate(self):

        def checkaggregate(spark):
            data = [("Sales", 4600, 3000,3900.0, 11700),
                    ("Finance", 3900, 3000, 3300.0, 13200),
                    ("Marketing", 3000, 2000, 2500.0, 5000)]
            schema = StructType([StructField("department", StringType()),
                                 StructField("max(salary)", IntegerType()),
                                 StructField("min(salary)", IntegerType()),
                                 StructField("avg(salary)", FloatType()),
                                 StructField("sum(salary)", IntegerType())])
            df = spark.createDataFrame(data, schema)
            return df

        self.assertEqual(Aggregate().collect(), checkaggregate(spark).collect())