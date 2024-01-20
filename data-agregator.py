import os
import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def transform_data(data_source, output_uri):

    with SparkSession.builder.appName("People-DataAgregator").getOrCreate() as spark:
        spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

        #Load file
        df = spark.read.option("header","true").parquet(data_source)

        #Create in-memory DataFrame
        df.createOrReplaceTempView("people")

        #Creating SQL Query
        SQL_QUERY = """
            SELECT country, count(*) as population
            FROM people
            GROUP BY country
            ORDER BY population DESC
        """

        #Executing query
        df = spark.sql(SQL_QUERY)

        #Creating new file
        df.write.option("header", "true").mode("overwrite").csv(output_uri)



if __name__ == "__main__":

    bucket = sys.argv[1]

    data_source = "s3://" + bucket + "/data/people-parquet"
    output_uri = "s3://" + bucket + "/data/people-agregation"

    print(f"Data Source: {data_source}")
    print(f"Output Location: {output_uri}")

    transform_data(data_source, output_uri)


