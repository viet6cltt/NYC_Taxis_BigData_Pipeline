import os
os.environ["PYSPARK_PYTHON"] = "python3"
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
s1 = StructType([StructField("a", StringType(), True)])
s2 = s1.add(StructField("b", DoubleType(), True))
print(f"s1 length: {len(s1.fields)}, s2 length: {len(s2.fields)}")
