from pyspark.sql import SparkSession

# Function to get the Spark session
def get_spark_session(app_name="MySparkApp"):

    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return (spark)
