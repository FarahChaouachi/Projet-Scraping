import pytest
from pyspark.sql import DataFrame, SparkSession
from process.Traitement import kafka, Traitement

# Fixture pour configurer une session Spark avant l'exécution des tests
@pytest.fixture(scope="session")
def spark():
    # Création d'une session Spark avec une configuration locale
    spark_session = SparkSession.builder \
        .appName("pytest-pyspark") \
        .master("local[*]") \
        .getOrCreate()
    
    # Fournit la session Spark aux tests et s'assure qu'elle est arrêtée après
    yield spark_session
    spark_session.stop()

# Test pour la lecture de données depuis Kafka
def test_read_from_kafka():
    # Initialisation de l'objet Kafka
    kaf = kafka()
    # Consommation des messages depuis Kafka
    cons = kaf.consume_from_kafka()
    
    # Création d'un objet Traitement avec l'URL source et la base MySQL
    traitement = Traitement("https://www.imdb.com/", "jdbc:mysql://localhost:3306/Scrapping")
    
    # Transformation des données consommées en DataFrame
    df = traitement._get_data_from_kafka(cons)
    
    # Vérification que le DataFrame n'est pas vide
    assert df is not None
    assert isinstance(df, DataFrame)  # Vérifie que l'objet est bien un DataFrame
    assert len(df.columns) > 0  # Vérifie qu'il y a des colonnes
    assert df.count() > 0  # Vérifie qu'il y a des données dans le DataFrame

# Test pour extraire les noms de colonnes du DataFrame
def test_get_nom_colonne(spark):
    # Données simulées de Kafka sous forme de liste de chaînes de caractères
    cons = ['Top 10 sur IMDb cette semaine\n8,7\n1. Severance\nListe de favoris\nBande-annonce\n7,9\n2. Paradise\nListe de favoris\nBande-annonce\n7,5\n3. The Night Agent\nListe de favoris\nBande-annonce']
    
    # Transformation des données en lignes pour un DataFrame Spark
    lines = cons[0].split('\n')
    df_data = spark.createDataFrame([(line,) for line in lines], ["Donnée de kafka"])
    
    # Initialisation de l'objet Traitement
    traitement = Traitement("https://www.imdb.com/", "jdbc:mysql://localhost:3306/Scrapping")
    traitement.df = df_data  # Attribution des données au traitement
    
    # Extraction du nom de la colonne
    df = traitement._get_nom_colonne()
    
    # Vérifications sur le DataFrame obtenu
    assert df is not None
    assert isinstance(df, DataFrame)  # Vérifie que le résultat est bien un DataFrame
    assert len(df.columns) == 1, f"Le DataFrame contient plusieurs colonnes : {df.columns.tolist()}"
    assert df.columns[0] == "Top 10 sur IMDb cette semaine", f"Nom de colonne incorrect : {df.columns[0]}"

# Test pour la transformation et structuration des données
def test_get_data_traitement(spark):
    # Données brutes simulées issues de Kafka
    cons = ['Top 10 sur IMDb cette semaine\n8,7\n1. Severance\nListe de favoris\nBande-annonce\n7,9\n2. Paradise\nListe de favoris\nBande-annonce\n7,5\n3. The Night Agent\nListe de favoris\nBande-annonce']
    
    # Transformation des données en lignes pour un DataFrame Spark
    lines = cons[0].split('\n')
    df_data = spark.createDataFrame([(line,) for line in lines], ["Donnée de kafka"])
    
    # Initialisation de l'objet Traitement
    traitement = Traitement("https://www.imdb.com/", "jdbc:mysql://localhost:3306/Scrapping")
    traitement.df = df_data  # Attribution des données
    
    # Extraction du nom de la colonne
    traitement._get_nom_colonne()
    
    # Transformation des données en un format structuré
    df = traitement._get_data_traitement()
    
    # Vérifications sur le DataFrame structuré
    assert df is not None
    assert isinstance(df, DataFrame)  # Vérifie que le résultat est bien un DataFrame
    assert df.count() == 3  # Vérifie que le nombre de lignes correspond à 3
    
    # Récupération des lignes sous forme de liste
    rows = df.collect()
    
    # Vérification des classements des films
    assert rows[0]["Classement"] == 1, "Le premier classement doit être 1"
    assert rows[1]["Classement"] == 2, "Le deuxième classement doit être 2"
    assert rows[2]["Classement"] == 3, "Le troisième classement doit être 3"
    
    # Vérification des noms des films
    assert rows[0]["Top 10 sur IMDb cette semaine"] == "Severance", "Le premier film doit être 'Severance'"
    assert rows[1]["Top 10 sur IMDb cette semaine"] == "Paradise", "Le deuxième film doit être 'Paradise'"
    assert rows[2]["Top 10 sur IMDb cette semaine"] == "The Night Agent", "Le troisième film doit être 'The Night Agent'"
