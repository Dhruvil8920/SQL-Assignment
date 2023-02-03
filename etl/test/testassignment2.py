from etl.utils.Assignment2 import *
import unittest

class TestMyFunc(unittest.TestCase):

    def testDataFrame(self):

        def checkdf(spark):
            data = [("Banana", 1000, "USA"),
                    ("Carrots", 1500, "INDIA"),
                    ("Beans", 1600, "SWEDEN"),
                    ("Orange", 2000, "UK"),
                    ("Orange", 2000, "UAE"),
                    ("Banana", 400, "CHINA"),
                    ("Carrot", 1200, "CHINA")]

            schema = StructType([StructField("Product", StringType()),
                                 StructField("Amount", IntegerType()),
                                 StructField("Country", StringType())])
            df = spark.createDataFrame(data, schema)
            return df

        self.assertEqual(CreateDataFrame().collect(), checkdf(spark).collect())

    def testTotalAmount(self):

        def chechtotalamount(spark):
            data = [("CHINA", 400, None, 1200, None),
                            ("USA", 1000, None, None, None),
                            ("SWEDEN", None, 1600, None, None),
                            ("UK", None, None, None, 2000),
                            ("UAE", None, None, None, 2000),
                            ("INDIA", None, None, 1500, None)]
            df = spark.createDataFrame(data, ["Country", "Banana", "Beans", "Carrots", "Orange"])
            return df

        self.assertEqual(TotalAmount().collect(), chechtotalamount(spark).collect())

    def testunpivot(self):

        def chechunpivot(spark):
            data = [("CHINA", "Banana", 400),
                    ("CHINA", "Carrots", 1200),
                    ("USA", "Banana", 1000),
                    ("SWEDEN", "Beans", 1600),
                    ("UK", "Orange", 2000),
                    ("UAE", "Orange", 2000),
                    ("INDIA", "Carrots", 1500)]
            schema = StructType([StructField("Country", StringType()),
                                 StructField("Producr", StringType()),
                                 StructField("Amount", IntegerType())])
            df = spark.createDataFrame(data, schema)
            return df

        self.assertEqual(Unpivot().collect(), chechunpivot(spark).collect())