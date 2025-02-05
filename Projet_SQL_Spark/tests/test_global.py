import pytest
from pyspark.sql import SparkSession
from tests.schemadatatest import schema_finale
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim
from process.Resultat import remove_accents

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("unit-tests").getOrCreate()
 
def test_compare_csv_files(spark):
    
    #################################Test Global #############################
    # Paths to the CSV files
    output_csv_path = "C:/Users/fchaouachi/Desktop/Projet SQL to Spark/Projet_SQL_Spark/output/part-00000-83bd2de4-3694-4fe5-b782-b6d6d61adbf9-c000.csv"
    expected_csv_path = "C:/Users/fchaouachi/Desktop/Projet SQL to Spark/Projet_SQL_Spark/tests/data/Final_nextsise_MONO_projet_20240722 (1).csv"
   
    # Read the CSV files into DataFrames
    df_output=spark.read.csv(output_csv_path, header=True, schema=schema_finale)
    df_expected = spark.read.csv(expected_csv_path, header=True, schema=schema_finale)
    remove_accents_udf = udf(remove_accents, StringType())
    df_expected = df_expected.withColumn("V_RESUME_PRJ", trim(remove_accents_udf(df_expected["V_RESUME_PRJ"])))
    df_expected = df_expected.withColumn("V_NOM_PRJ", trim(remove_accents_udf(df_expected["V_NOM_PRJ"])))
    df_expected = df_expected.withColumn("V_IDENT_PRJ", trim(remove_accents_udf(df_expected["V_IDENT_PRJ"])))
    df_expected = df_expected.withColumn("CONCAT_ACT_PROC", trim(remove_accents_udf(df_expected["CONCAT_ACT_PROC"])))
    df_expected = df_expected.withColumn("V_PROC_PRJ_2", trim(remove_accents_udf(df_expected["V_PROC_PRJ_2"])))

    # Verify the column names
    assert set(df_output.columns) == set(df_expected.columns), "Column names do not match"
 
    # Verify the number of columns
    assert len(df_output.columns) == len(df_expected.columns), "Number of columns do not match"
 
    # Verify the number of rows
    assert df_output.count() == df_expected.count(), "Number of rows do not match"

    ################################# Row Compatibility #############################
    
    ## Verify if columns are compatible
    for column in df_output.columns : 
        assert verification_val_colonne(df_output, df_expected, column) == True
    
    ################################# Row Equality ###################################

    # Verify if all rows in df_output exist in df_expected
    missing_in_df_expected = df_output.exceptAll(df_expected) 
    assert (missing_in_df_expected.count() == 0) 

    # Verify if all rows in df_expected exist in df_output
    missing_in_df_output = df_expected.exceptAll(df_output)  
    assert (missing_in_df_output.count() == 0)

# Function to verify the values of the column in both data frames
def verification_val_colonne(df_output, df_expected, colonne): 
    # Select the column
    output = df_output.select(colonne).rdd.flatMap(lambda x: x).distinct().collect()
    expected = df_expected.select(colonne).rdd.flatMap(lambda x: x).distinct().collect()
    
    # Find differing values in output not in expected
    in_output = [value for value in output if value not in expected]

    # Find differing values in expected not in output
    in_expected = [value for value in expected if value not in output]
    
    if (len(in_output) == len(in_expected)):
        return True
    else:
        return False