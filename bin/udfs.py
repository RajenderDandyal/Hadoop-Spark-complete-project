from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

@udf(returnType=IntegerType())
def column_split_cnt(city_zips):
    return len(city_zips.split(" "))