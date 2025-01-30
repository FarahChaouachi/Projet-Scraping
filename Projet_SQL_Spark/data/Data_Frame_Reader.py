# Ce fichier contient la classe qui va permettre la lecture des données 

from common.reader import read_from_csv
from pyspark.sql import DataFrame

# Class to read the data
class Data_Frame_Reader:

    def __init__(self,path: str, df: DataFrame = None):
        self.path = path
        self.df=df
        
    # Function to read the data    
    def read(self, path):
        self.df=read_from_csv(path)
        return (self.df)