from pyspark.sql import DataFrame
from common.reader import read_from_kafka

# Class to read the data
class Data_Frame_Reader:

    def __init__(self,df=None):
        self.df=df

    # Function to read the data    
    def read_from_kafka(self,data):
        self.df=read_from_kafka(data)
