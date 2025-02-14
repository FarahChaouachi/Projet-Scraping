# Importation de la classe DataFrame depuis le module pyspark.sql
from pyspark.sql import DataFrame

# Importation de la fonction read_from_kafka depuis le module common.reader
from common.reader import read_from_kafka

# Classe pour lire les données
class Data_Frame_Reader:

    # Initialisation de la classe avec un DataFrame optionnel
    def __init__(self, df=None):
        self.df = df

    # Fonction pour lire les données depuis Kafka
    def read_from_kafka(self, data):
        # Appel de la fonction 'read_from_kafka' pour récupérer les données de Kafka et les assigner à l'attribut 'df'
        self.df = read_from_kafka(data)
