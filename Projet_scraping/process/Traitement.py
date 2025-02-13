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
from kafka import KafkaConsumer,KafkaProducer
from common.writer import write_to_DataBase


# Class pour la partie Web Scrapping de la page web 
class WebScraping: 
    def __init__(self,path_driver,path_site,element=None):
        self.element=element
        self.path_driver=path_driver
        self.path_site=path_site

    def webscrap(self):
        options = Options()
        options.add_experimental_option("detach", True)  # Pour garder la fenêtre ouverte après l'exécution
        
        # Chemin vers ton driver Chrome
        path = self.path_driver
        service = Service(path)
        
        # Lancer Chrome avec Selenium
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(self.path_site)  # Page d'accueil d'IMDb
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)  # Attendre que la page charge après le scroll
        self.element = driver.find_element(By.XPATH, "//*[@class='top-ten']").text        


# Class pour Activer et mettre les données dans un topic kafka
class kafka : 
    def __init__(self):
        pass

    # Lancer Zookeeper
    def start_zookeeper(self):
        subprocess.Popen(["C:/kafka/bin/windows/zookeeper-server-start.bat", "C:/kafka/config/zookeeper.properties"])
    
    # Lancer Kafka
    def start_kafka(self):
        subprocess.Popen(["C:/kafka/bin/windows/kafka-server-start.bat", "C:/kafka/config/server.properties"])

    # Creation d'un topic Kafka 
    def create_topic(self):
        subprocess.run([
        "C:\\kafka\\bin\\windows\\kafka-topics.bat",
        "--create",
        "--topic", "ScrabingToPySpark",
        "--bootstrap-server", "localhost:9092",
        "--partitions", "1",
        "--replication-factor", "1"])
    
    # Consommer des messages du topic ScrabingToPySpark
    def consume_from_kafka(self):
        consumer = KafkaConsumer(
            'ScrabingToPySpark',  # Nom du topic
            bootstrap_servers=['localhost:9092'],  # Serveur Kafka
            group_id='my-group',  # ID du groupe de consommateurs
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Désérialisation JSON
        )
        data=[]
        for message in consumer:
            data.append(message.value)
            return(data)

    # Envoi des données scrappées vers Kafka dans le topic ScrabingToPySpark
    def send_to_kafka(self, data):
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',  # Serveur Kafka
            value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Sérialisation des données en JSON
        )

        # Envoi des données scrappées dans le topic Kafka 'ScrabingToPySpark'
        producer.send('ScrabingToPySpark', value=data)
        producer.flush()  # Forcer l'envoi de tous les messages


# Class pour la partie de traitement 
class Traitement : 
    def __init__(self,inputweb,output_path_database,df=None):
        self.df=df
        self.inputweb=inputweb
        self.output_path_database=output_path_database

    def run(self) -> None:
        # Faire Le web Scrapping pour avoir les 10 meilleurs films 
        Scrap=WebScraping("C:/Program Files (x86)/chromedriver.exe",self.inputweb)
        Scrap.webscrap()

        # Faire la partie de Kafka 
        kaf=kafka()
        kaf.start_zookeeper()
        # kaf.start_kafka()
        # kaf.create_topic()
        kaf.send_to_kafka(Scrap.element)
        cons=kaf.consume_from_kafka()

        # Faire la partie traitement des données 
        self._get_data_from_kafka(cons)
        self._get_nom_colonne()
        self._get_data_traitement()
        self._put_data_to_csv()

    def _get_data_from_kafka(self,data):
       df_read=Data_Frame_Reader() 
       df_read.read_from_kafka(data)
       self.df=df_read.df
       return(df_read.df)
    
    def _get_nom_colonne(self): 
        column_name = self.df.select("Donnée de kafka").first()[0]        
        self.df = self.df.withColumnRenamed("Donnée de kafka", column_name)
        self.df = self.df.filter(self.df[column_name] != column_name)
        return(self.df)
    
    def _get_data_traitement(self):
        self.df = self.df.filter(col("Top 10 sur IMDb cette semaine").rlike(r"\."))
        self.df = self.df.withColumn("Top 10 sur IMDb cette semaine",F.regexp_replace(F.col("Top 10 sur IMDb cette semaine"), r"^\d+\.\s*", ""))
        self.df = self.df.withColumn("Classement", F.row_number().over(Window.orderBy(F.lit(1))))
        self.df = self.df.select("Classement","Top 10 sur IMDb cette semaine")
        return (self.df)
    
    def _put_data_to_csv(self):
        write_to_DataBase(self.df,self.output_path_database)