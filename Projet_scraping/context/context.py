# Importation de la classe SparkSession depuis le module pyspark.sql
from pyspark.sql import SparkSession

# Fonction pour obtenir une session Spark
def get_spark_session(app_name="MySparkApp"):

    # Création de la session Spark avec un nom d'application et la configuration du connecteur MySQL
    spark = SparkSession.builder.appName(app_name).config("spark.jars", "C:\\mysql-connector-j-8.0.33.jar").getOrCreate()

    # Définition du niveau de log à "ERROR" pour réduire la verbosité des logs
    spark.sparkContext.setLogLevel("ERROR")

    # Retour de l'objet SparkSession
    return (spark)
