from pyspark.sql import DataFrame

# Function to write to a CSV file
def write_to_DataBase(df: DataFrame,output) -> str:   
    jdbc_url = output
    table_name = "top_10_films"
    properties = {
    "user" : "root",
    "password" : "Anas.osman@1998",
    "driver": "com.mysql.cj.jdbc.Driver"
    }
    df.write.mode("overwrite").jdbc(jdbc_url, table_name, properties=properties)
