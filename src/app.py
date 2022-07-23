from pyspark.sql import SparkSession
from pyspark.functions import (
    from_unixtime,
    expr,
    sha2,
    concat,
    col,
    lit,
    regexp_extract
)

def load_data(format, input):
    ()
