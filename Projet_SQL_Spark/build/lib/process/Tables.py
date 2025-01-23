# Ce fichier contient la class qui permet de faire notre ETL 
from pyspark.sql import DataFrame
from data.Data_Frame_Reader import *
from process.Traitement import get_data_from_csv


class Tables:

    def __init__(self,input_path: str) -> None:
        
        #initialisation par les paths des données 
        self.input_path: str = input_path
        self.df=None
        self.output_path=None
        


    def run(self) -> None:

        #Transformation des csv en data frame
        self.df: DataFrame = get_data_from_csv(self.input_path)
        print('test')
        # Transformation à faire : La derniere sous requete 

