import sys
import argparse
import logging, traceback
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_unixtime,
    expr,
    sha2,
    concat,
    col,
    lit,
    regexp_extract
)

def load_data(format, input):
    """load data"""
    if format == 'parquet':
        return spark.read.parquet(input)
    else:
        return spark.read.text(input)

def main(spark):

    #load data
    raw_df = load_data(flags.format,flags.input)
    raw_df.show(truncate = False)

    df = raw_df.select( 
        regexp_extract(col('value'),r'''pid\s(\d+)''',1).alias('PID'),
        #regexp_extract(col('value'),r'''''')
    )

    df.show(truncate = False)

    df.write.parquet(flags.output,mode = "overwrite",compression="gzip")


if __name__ == "__main__":
    try:
        ap = argparse.ArgumentParser()
        ap.add_argument("-i","--input",help="input path", required=True)
        ap.add_argument("-o","--output", help="output path", required=True)
        ap.add_argument(
            "--format",
            help="input file format",
            default="text",
            choices=["parquet","text"]
        )
        
        flags = ap.parse_args()
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s:%(levelname)s:%(name)s:%(message)s"
        )

        spark = SparkSession.builder.getOrCreate()
        main(spark)

    except (Exception):
        traceback.print_exc()
        sys.exit(2)