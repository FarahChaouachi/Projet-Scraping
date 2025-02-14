import time
import subprocess
import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from data.Data_Frame_Reader import Data_Frame_Reader
from pyspark.sql.window import Window
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from kafka import KafkaConsumer, KafkaProducer
from common.writer import write_to_DataBase

# Classe pour effectuer le web scraping sur une page web spécifique
class WebScraping: 
    def __init__(self, path_driver, path_site, element=None):
        self.element = element  # Stocke l'élément scrappé
        self.path_driver = path_driver  # Chemin du driver Chrome
        self.path_site = path_site  # URL du site à scrapper

    def webscrap(self):
        # Configuration des options pour le navigateur Chrome
        options = Options()
        options.add_experimental_option("detach", True)  # Maintient la fenêtre ouverte après exécution
        
        # Initialisation du driver Chrome
        service = Service(self.path_driver)
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(self.path_site)  # Ouvre la page IMDb
        
        # Défilement vers le bas pour charger la page entièrement
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)  # Pause pour permettre le chargement de la page
        
        # Extraction du texte de l'élément contenant le top 10 des films
        self.element = driver.find_element(By.XPATH, "//*[@class='top-ten']").text        

# Classe pour gérer Kafka : production et consommation de messages
class kafka: 
    def __init__(self):
        pass

    # Lancement de Zookeeper (nécessaire pour Kafka)
    def start_zookeeper(self):
        subprocess.Popen(["C:/kafka/bin/windows/zookeeper-server-start.bat", "C:/kafka/config/zookeeper.properties"])
    
    # Lancement du serveur Kafka
    def start_kafka(self):
        subprocess.Popen(["C:/kafka/bin/windows/kafka-server-start.bat", "C:/kafka/config/server.properties"])

    # Création d'un topic Kafka nommé "ScrabingToPySpark"
    def create_topic(self):
        subprocess.run([
            "C:\\kafka\\bin\\windows\\kafka-topics.bat",
            "--create",
            "--topic", "ScrabingToPySpark",
            "--bootstrap-server", "localhost:9092",
            "--partitions", "1",
            "--replication-factor", "1"
        ])
    
    # Consommation des messages du topic Kafka "ScrabingToPySpark"
    def consume_from_kafka(self):
        consumer = KafkaConsumer(
            'ScrabingToPySpark',  # Nom du topic
            bootstrap_servers=['localhost:9092'],  # Adresse du serveur Kafka
            group_id='my-group',  # Identifiant du groupe de consommateurs
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Désérialisation JSON
        )
        data = []
        for message in consumer:
            data.append(message.value)  # Ajout des données consommées dans une liste
            return data

    # Envoi des données scrappées vers Kafka
    def send_to_kafka(self, data):
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',  # Adresse du serveur Kafka
            value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Sérialisation des données en JSON
        )
        producer.send('ScrabingToPySpark', value=data)  # Envoi des données dans le topic Kafka
        producer.flush()  # Forcer l'envoi des messages

# Classe pour le traitement des données extraites
class Traitement: 
    def __init__(self, inputweb, output_path_database, df=None):
        self.df = df  # Stocke le DataFrame contenant les données traitées
        self.inputweb = inputweb  # URL du site à scrapper
        self.output_path_database = output_path_database  # Chemin de la base de données

    # Exécute le pipeline de scraping, Kafka et traitement
    def run(self) -> None:
        # Étape 1 : Web Scraping des 10 meilleurs films sur IMDb
        Scrap = WebScraping("C:/Program Files (x86)/chromedriver.exe", self.inputweb)
        Scrap.webscrap()

        # Étape 2 : Envoi des données vers Kafka
        kaf = kafka()
        kaf.start_zookeeper()
        # kaf.start_kafka()
        # kaf.create_topic()
        kaf.send_to_kafka(Scrap.element)
        cons = kaf.consume_from_kafka()

        # Étape 3 : Traitement des données récupérées
        self._get_data_from_kafka(cons)
        self._get_nom_colonne()
        self._get_data_traitement()
        self._put_data_to_csv()

    # Lecture des données depuis Kafka
    def _get_data_from_kafka(self, data):
        df_read = Data_Frame_Reader()  # Création d'un lecteur de données
        df_read.read_from_kafka(data)  # Lecture des données Kafka
        self.df = df_read.df  # Stockage des données dans l'attribut df
        return df_read.df
    
    # Extraction du nom de la colonne principale
    def _get_nom_colonne(self): 
        column_name = self.df.select("Donnée de kafka").first()[0]  # Récupération du premier élément comme nom de colonne       
        self.df = self.df.withColumnRenamed("Donnée de kafka", column_name)  # Renommage de la colonne
        self.df = self.df.filter(self.df[column_name] != column_name)  # Suppression de la ligne contenant le titre
        return self.df
    
    # Traitement des données : extraction du classement et des films
    def _get_data_traitement(self):
        self.df = self.df.filter(col("Top 10 sur IMDb cette semaine").rlike(r"\."))  # Filtrer les lignes contenant un point (numérotation)
        self.df = self.df.withColumn("Top 10 sur IMDb cette semaine", F.regexp_replace(F.col("Top 10 sur IMDb cette semaine"), r"^\d+\.\s*", ""))  # Suppression des numéros
        self.df = self.df.withColumn("Classement", F.row_number().over(Window.orderBy(F.lit(1))))  # Ajout d'une colonne "Classement"
        self.df = self.df.select("Classement", "Top 10 sur IMDb cette semaine")  # Sélection des colonnes pertinentes
        return self.df
    
    # Sauvegarde des données traitées dans la base de données
    def _put_data_to_csv(self):
        write_to_DataBase(self.df, self.output_path_database)  # Écriture des données dans la base de données
