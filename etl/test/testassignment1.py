from etl.utils.Assignment1 import *
import unittest


class TestMyFunc(unittest.TestCase):

    # Testing DataFrame
    def testdataframe(self):

        def chackdataframe(saprk):
            data = [({"firstname": "James", "middlename": "", "lastname": "Smith"}, "1998-01-03", "M", 3000),
                    ({"firstname": "Michael", "middlename": "Rose", "lastname": ""}, "1998-11-10", "M", 20000),
                    ({"firstname": "Robert", "middlename": "", "lastname": "Williams"}, "2000-01-02", "M", 3000),
                    ({"firstname": "Maria", "middlename": "Anne", "lastname": "Jones"}, "1998-01-03", "F", 11000),
                    ({"firstname": "Jen", "middlename": "Mary", "lastname": "Brown"}, "1998-10-04", "F", 10000)]

            schema = StructType([StructField("name", MapType(StringType(), StringType())),
                                 StructField("dob", StringType()),
                                 StructField("gender", StringType()),
                                 StructField("salary", IntegerType())])
            df = spark.createDataFrame(data)
            return df

        self.assertEqual(CreateDataFrame().collect(), chackdataframe(spark).collect())

    # Testing Select Column
    def testSelectColumn(self):

        def chackselectcolumn(spark):
            data = [("James", "Smith", 3000),
                    ("Michael", "", 20000),
                    ("Robert", "Williams", 3000),
                    ("Maria", "Jones", 11000),
                    ("Jen", "Brown", 10000)]
            schema = StructType([StructField("name[firstname]", StringType()),
                                 StructField("name[lastname]", StringType()),
                                 StructField("salary", IntegerType())])
            df = spark.createDataFrame(data, schema)
            return df

        self.assertEqual(SelectColumn().collect(), chackselectcolumn(spark).collect())

    # Testing adding column
    def testAddColumn(self):

        def chackaddcolumn(spark):
            data = [({"firstname": "James", "middlename": "", "lastname": "Smith"}, "1998-01-03", "M", 3000, "", "", ""),
                    ({"firstname": "Michael", "middlename": "Rose", "lastname": ""}, "1998-11-10", "M", 20000, "", "", ""),
                    ({"firstname": "Robert", "middlename": "", "lastname": "Williams"}, "2000-01-02", "M", 3000,  "", "", ""),
                    ({"firstname": "Maria", "middlename": "Anne", "lastname": "Jones"}, "1998-01-03", "F", 11000,  "", "", ""),
                    ({"firstname": "Jen", "middlename": "Mary", "lastname": "Brown"}, "1998-10-04", "F", 10000,  "", "", "")]

            schema = StructType([StructField("name", MapType(StringType(), StringType())),
                                 StructField("dob", StringType()),
                                 StructField("gender", StringType()),
                                 StructField("salary", IntegerType()),
                                 StructField("Country", StringType()),
                                 StructField("department", StringType()),
                                 StructField("age", StringType())])
            df = spark.createDataFrame(data, schema)
            return df

        self.assertEqual(AddingColumn().collect(), chackaddcolumn(spark).collect())

    # Testing change of salary value
    def testChangeSalary(self):

        def chackchangedsalary(spark):
            data = [
                ({"firstname": "James", "middlename": "", "lastname": "Smith"}, "1998-01-03", "M", 4000, "", "", ""),
                ({"firstname": "Michael", "middlename": "Rose", "lastname": ""}, "1998-11-10", "M", 21000, "", "", ""),
                (
                {"firstname": "Robert", "middlename": "", "lastname": "Williams"}, "2000-01-02", "M", 4000, "", "", ""),
                ({"firstname": "Maria", "middlename": "Anne", "lastname": "Jones"}, "1998-01-03", "F", 12000, "", "",
                 ""),
                ({"firstname": "Jen", "middlename": "Mary", "lastname": "Brown"}, "1998-10-04", "F", 11000, "", "", "")]

            schema = StructType([StructField("name", MapType(StringType(), StringType())),
                                 StructField("dob", StringType()),
                                 StructField("gender", StringType()),
                                 StructField("salary", IntegerType()),
                                 StructField("Country", StringType()),
                                 StructField("department", StringType()),
                                 StructField("age", StringType())])
            df = spark.createDataFrame(data, schema)
            return df

        self.assertEqual(ChangingSalary().collect(), chackchangedsalary(spark).collect())

    # Testing change datatypes
    def testDataType(self):

        def chackDataType(spark):
            data = [({"firstname": "James", "middlename": "", "lastname": "Smith"}, "1998-01-03", "M", 4000, "", "", ""),
                ({"firstname": "Michael", "middlename": "Rose", "lastname": ""}, "1998-11-10", "M", 21000, "", "", ""),
                ({"firstname": "Robert", "middlename": "", "lastname": "Williams"}, "2000-01-02", "M", 4000, "", "",
                    ""),
                ({"firstname": "Maria", "middlename": "Anne", "lastname": "Jones"}, "1998-01-03", "F", 12000, "", "",
                 ""),
                ({"firstname": "Jen", "middlename": "Mary", "lastname": "Brown"}, "1998-10-04", "F", 11000, "", "", "")]

            schema = StructType([StructField("name", MapType(StringType(), StringType())),
                                 StructField("dob", StringType()),
                                 StructField("gender", StringType()),
                                 StructField("salary", StringType()),
                                 StructField("Country", StringType()),
                                 StructField("department", StringType()),
                                 StructField("age", StringType())])
            df = spark.createDataFrame(data, schema)
            return df

        self.assertEqual(ChangeDataType().collect(), chackDataType(spark).collect())

    # Testing new created column from salary
    def testnewcolumn(self):

        def chacknewcolumn(spark):
            data = [
                ({"firstname": "James", "middlename": "", "lastname": "Smith"}, "1998-01-03", "M", 4000, "", "", "", 9000),
                ({"firstname": "Michael", "middlename": "Rose", "lastname": ""}, "1998-11-10", "M", 21000, "", "", "", 26000),
                ({"firstname": "Robert", "middlename": "", "lastname": "Williams"}, "2000-01-02", "M", 4000, "", "", "", 9000),
                ({"firstname": "Maria", "middlename": "Anne", "lastname": "Jones"}, "1998-01-03", "F", 12000, "", "", "", 17000),
                ({"firstname": "Jen", "middlename": "Mary", "lastname": "Brown"}, "1998-10-04", "F", 11000, "", "", "", 16000)]

            schema = StructType([StructField("name", MapType(StringType(), StringType())),
                                 StructField("dob", StringType()),
                                 StructField("gender", StringType()),
                                 StructField("salary", IntegerType()),
                                 StructField("Country", StringType()),
                                 StructField("department", StringType()),
                                 StructField("age", StringType()),
                                 StructField("newsalary", IntegerType())])
            df = spark.createDataFrame(data, schema)
            return df

        self.assertEqual(DerivingNewColumn().collect(), chacknewcolumn(spark).collect())

    # Testing renamed column
    def testrenamed(self):

        def chackRenameColumn(spark):
            data = [
                ({"firstposition": "James", "secondposition": "", "lastposition": "Smith"}, "1998-01-03", "M", 4000, "", "", "", 9000),
                ({"firstposition": "Michael", "secondposition": "Rose", "lastposition": ""}, "1998-11-10", "M", 21000, "", "", "", 26000),
                ({"firstposition": "Robert", "secondposition": "", "lastposition": "Williams"}, "2000-01-02", "M", 4000, "", "", "", 9000),
                ({"firstposition": "Maria", "secondposition": "Anne", "lastposition": "Jones"}, "1998-01-03", "F", 12000, "", "", "", 17000),
                ({"firstposition": "Jen", "secondposition": "Mary", "lastposition": "Brown"}, "1998-10-04", "F", 11000, "", "", "", 16000)]

            schema = StructType([StructField("name", MapType(StringType(), StringType())),
                                 StructField("dob", StringType()),
                                 StructField("gender", StringType()),
                                 StructField("salary", IntegerType()),
                                 StructField("Country", StringType()),
                                 StructField("department", StringType()),
                                 StructField("age", StringType()),
                                 StructField("newsalary", IntegerType())])
            df = spark.createDataFrame(data, schema)
            df = df.withColumn("name", df["name"].cast(StringType()))
            return df

        self.assertEqual(Rename().collect(), chackRenameColumn(spark).collect())

    # Testing MaxSalary name
    def testMaxSalary(self):

        def chackmaxsalary(spark):
            data = [
                ({"firstname": "James", "middlename": "", "lastname": "Smith"}, "1998-01-03", "M", 4000, "", "", "",
                 9000),
                ({"firstname": "Michael", "middlename": "Rose", "lastname": ""}, "1998-11-10", "M", 21000, "", "", "",
                 26000),
                ({"firstname": "Robert", "middlename": "", "lastname": "Williams"}, "2000-01-02", "M", 4000, "", "", "",
                 9000),
                (
                {"firstname": "Maria", "middlename": "Anne", "lastname": "Jones"}, "1998-01-03", "F", 12000, "", "", "",
                17000),
                ({"firstname": "Jen", "middlename": "Mary", "lastname": "Brown"}, "1998-10-04", "F", 11000, "", "", "",
                 16000)]

            schema = StructType([StructField("name", MapType(StringType(), StringType())),
                                 StructField("dob", StringType()),
                                 StructField("gender", StringType()),
                                 StructField("salary", IntegerType()),
                                 StructField("Country", StringType()),
                                 StructField("department", StringType()),
                                 StructField("age", StringType()),
                                 StructField("newsalary", IntegerType())])
            df = spark.createDataFrame(data, schema)
            return df.filter(df['salary'] == df.agg(F.max(df['salary'])).collect()[0][0]).select('name')

        self.assertEqual(MaxSalary().collect(), chackmaxsalary(spark).collect())

    # Testing Dropped column
    def testDropColumn(self):

        def checkDropColumn(spark):
            data = [
                ({"firstname": "James", "middlename": "", "lastname": "Smith"}, "1998-01-03", "M", 4000, "", 9000),
                ({"firstname": "Michael", "middlename": "Rose", "lastname": ""}, "1998-11-10", "M", 21000, "", 26000),
                ({"firstname": "Robert", "middlename": "", "lastname": "Williams"}, "2000-01-02", "M", 4000, "", 9000),
                ({"firstname": "Maria", "middlename": "Anne", "lastname": "Jones"}, "1998-01-03", "F", 12000,  "", 17000),
                ({"firstname": "Jen", "middlename": "Mary", "lastname": "Brown"}, "1998-10-04", "F", 11000, "",  16000)]

            schema = StructType([StructField("name", MapType(StringType(), StringType())),
                                 StructField("dob", StringType()),
                                 StructField("gender", StringType()),
                                 StructField("salary", IntegerType()),
                                 StructField("Country", StringType()),
                                 StructField("newsalary", IntegerType())])
            df = spark.createDataFrame(data, schema)
            return df

        self.assertEqual(DropColumn().collect(), checkDropColumn(spark).collect())

    # Testing unique values of dob
    def testDistinct(self):

        def checkdistinct(spark):
            data = [Row(dob="1998-01-03"), Row(dob="1998-11-10"), Row(dob="2000-01-02"), Row(dob="1998-10-04")]
            df = spark.createDataFrame(data)
            return df

        self.assertEqual(Distinct1().collect(), checkdistinct(spark).collect())

    # Testing unique values of salary
    def testDistinct2(self):

        def checkDistinct2(spark):
            data = [Row(salary=4000), Row(salary=21000), Row(salary=12000), Row(salary=11000)]
            df = spark.createDataFrame(data)
            return df
        self.assertEqual(Distinct2().collect(), checkDistinct2(spark).collect())
