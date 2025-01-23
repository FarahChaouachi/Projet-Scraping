# Ce fichier contient la classe qui va permettre la lecture des données 

import os
from common.reader import read_from_csv
from pyspark.sql import DataFrame


class Data_Frame_Reader:

    def __init__(self,path: str, df: DataFrame = None):
        self.path = path
        self.df=df
        

    def read(self, path):
        self.df=read_from_csv(path)
        return (self.df)