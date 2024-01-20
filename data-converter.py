import os
import sys
from pyspark.sql import SparkSession


if __name__ == "__main__":

    bucket = sys.argv[1]
    key = sys.argv[2]
    
    file_name = key[key.rfind("/") + 1:]
    data_source = "s3://" + bucket + "/" + key
    output_uri = "s3://" + bucket + "/data/people-parquet"

    print(f"Data Source: {data_source}")
    print(f"File Name: {file_name}")
    print(f"Output Location: {output_uri}")

    #Conveting data from CSV to PARQUET
    print("Conveting data from CSV to PARQUET")
    spark = SparkSession.builder.appName("People-DataConverter").getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    df = spark.read.option("header","true").csv(data_source)
    df.write.mode("append").parquet(output_uri)
    spark.stop()
