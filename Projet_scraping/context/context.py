from pyspark.sql import SparkSession

# Function to get the Spark session
def get_spark_session(app_name="MySparkApp"):

    spark = SparkSession.builder.appName(app_name).config("spark.jars", "C:\\mysql-connector-j-8.0.33.jar").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return (spark)
