from pyspark.sql import DataFrame

# Function to write to a CSV file
def write_to_csv(df: DataFrame, output_file_path: str, header: bool = True, delimiter: str = ",") -> str:
    df.write.mode("overwrite").option("header", str(header).lower()).option("sep", delimiter).csv(output_file_path)
    return output_file_path