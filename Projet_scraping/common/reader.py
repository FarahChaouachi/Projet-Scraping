from context.context import get_spark_session
from pyspark.sql.types import StructType, StructField, StringType


def read_from_kafka(data):
    spark = get_spark_session()
    # Diviser le texte par des points pour obtenir une liste de parties
    lines = data[0].split('\n')
    # Créer un DataFrame où chaque élément du tableau devient une ligne distincte
    df = spark.createDataFrame([(line,) for line in lines], ["Donnée de kafka"])
    return(df) 