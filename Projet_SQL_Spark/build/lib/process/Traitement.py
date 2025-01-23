from pyspark.sql import DataFrame
from data.Data_Frame_Reader import *


def get_data_from_csv(path)-> DataFrame:
    # converir le parquet en DF
        df_read=Data_Frame_Reader(path)
        df_read.read(path)
        df_read.df.show()
        df_read.df.printSchema()
        return (df_read.df)


def derniere_jointure():
        return