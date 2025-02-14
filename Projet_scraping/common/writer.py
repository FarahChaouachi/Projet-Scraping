# Importation de la classe DataFrame depuis le module pyspark.sql
from pyspark.sql import DataFrame

# Fonction pour écrire dans une base de données
def write_to_DataBase(df: DataFrame, output) -> str:
    # URL JDBC de la base de données, passée en paramètre 'output'
    jdbc_url = output
    
    # Nom de la table dans laquelle les données seront insérées
    table_name = "top_10_films"
    
    # Propriétés de connexion à la base de données, incluant l'utilisateur, le mot de passe et le driver JDBC
    properties = {
        "user" : "root",  # Utilisateur de la base de données
        "password" : "Anas.osman@1998",  # Mot de passe de l'utilisateur
        "driver": "com.mysql.cj.jdbc.Driver"  # Driver JDBC pour MySQL
    }
    
    # Écriture du DataFrame dans la base de données en mode "overwrite" (écrase la table si elle existe déjà)
    df.write.mode("overwrite").jdbc(jdbc_url, table_name, properties=properties)
