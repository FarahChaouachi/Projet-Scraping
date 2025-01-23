# Ce fichier contient la connexion avec Spark

from pyspark.sql import SparkSession

def get_spark_session(app_name="MySparkApp"):

    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return (spark)
