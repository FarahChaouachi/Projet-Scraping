# Importation de la fonction get_spark_session depuis le module context.context
from context.context import get_spark_session

# Importation des types de données nécessaires pour définir la structure d'un DataFrame
from pyspark.sql.types import StructType, StructField, StringType

# Fonction pour lire les données depuis Kafka
def read_from_kafka(data):
    # Obtention de la session Spark via la fonction 'get_spark_session'
    spark = get_spark_session()

    # Diviser le texte (représenté par 'data[0]') par des retours à la ligne pour obtenir une liste de parties
    lines = data[0].split('\n')

    # Création d'un DataFrame Spark où chaque ligne du texte devient une entrée distincte
    df = spark.createDataFrame([(line,) for line in lines], ["Donnée de kafka"])

    # Retourner le DataFrame contenant les données lues depuis Kafka
    return(df)
