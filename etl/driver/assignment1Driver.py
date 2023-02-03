from etl.test.testassignment1 import *

# Created DataFrame
CreateDataFrame()

# Selecting firstname, lastmane and salary from dataframe
SelectColumn().show()

# Adding Country, department and age column
AddingColumn().show()

# Changing value of salary
ChangingSalary().show()

# Changing datatype of dob and salary to string
print(ChangeDataType().schema)

# Deriving new column from salary column
DerivingNewColumn().show()

# Renaming nested column( Firstname -> firstposition, middlename -> secondposition, lastname -> lastposition)
Rename().show()

# Dropping the department and age column.
DropColumn().show()

# List of distinct value of dob and salary
Distinct1().show()
Distinct2().show()


