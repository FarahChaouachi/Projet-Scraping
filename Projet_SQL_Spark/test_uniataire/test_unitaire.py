import os
import pytest
from pyspark.sql import DataFrame, SparkSession
import datetime
from common.reader import read_from_csv
from common.writer import write_to_csv
from process.Resultat import TransormationJointure
from test_uniataire.schemadatatest import (
    schema_data,

    schema_initial_transformation,
    data_initial,

    schema_intermediate_transformation,
    data_intermediate,

    schema_v2_transformation,
    data_v2,

    schema_v3_transformation,
    data_v3,
)

# Fonction pour la configuration de Spark
@pytest.fixture(scope="session")
def spark():
   
    spark_session = SparkSession.builder \
        .appName("pytest-pyspark") \
        .master("local[*]") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()


# Test pour la lecture d'un fichier CSV
def test_read_from_csv(spark):
   
    test_data_path = "C:/Users/fchaouachi/Downloads/export_DEPR_v20240708.csv"
    df = read_from_csv(test_data_path)

    assert df is not None
    assert isinstance(df, DataFrame)
    assert df.count() > 0
    assert len(df.columns) > 1


# Test pour l'écriture dans un fichier CSV
def test_write_to_csv(spark):
    
    test_data_path = "C:/Users/fchaouachi/Downloads/export_DEPR_v20240708.csv"
    df = read_from_csv(test_data_path)
    output_file_path = "C:/Users/fchaouachi/Desktop/Projet SQL to Spark/Projet_SQL_Spark/output"
    result_path = write_to_csv(df, output_file_path)

    assert os.path.exists(result_path)
    assert result_path == output_file_path

# Test pour get_initial_transformation
def test_get_initial_transformation(spark):

    # Données d'entrée avec la correction des dates
    data = [
    (6, 2021, "DC14044W0006", "360261701", "PSPC LOwCO2MOTION+", "PSPC", "LOwCO2MOTION+", "PSPC-AAP-3.0", None, "E13PS310203", "ENC14044W06", "PSPC-AAP-3.0", None, "",datetime.date(2013, 6, 28), datetime.date(2014, 5, 28), "3602617", None, None, None, "", "62000", "39000976900071", "", "AR", 245145.00, 1152498, 460999, 245145.00, 0.00, datetime.date(2014, 12, 19), 229989.00, "005800000", "SAS", "SOFICOR MADER", "PRIVE", "COLLABORATIF", "EN GESTION", "EC", "", "")
    ]
    df_data = spark.createDataFrame(data, schema_data)

    # Data attendue
    expected = [
    (6, 2021, 'PSPC LOwCO2MOTION+', 'LOwCO2MOTION+', 'PSPC-AAP-3.0', '', datetime.date(2013,6,28), None, None, None, None, None, 'COLLABORATIF', '', 'PSPC', '', '005800000')
    ]
    df_expected = spark.createDataFrame(expected, schema_initial_transformation)

    # Transformation
    transfo=TransormationJointure()
    transformed_df = transfo._get_initial_transformation(df_data)

    assert transformed_df is not None
    assert transformed_df.count() > 0
    assert transformed_df.collect()== df_expected.collect()
    assert isinstance(transformed_df, DataFrame)
    assert set(transformed_df.columns) == set(data_initial)


# Test pour get_intermediate_transformation
def test_get_intermediate_transformation(spark):
    
    # Data d'entrée 
    data = [
    (6, 2021, "DC14044W0006", "360261701", "PSPC LOwCO2MOTION+", "PSPC", "LOwCO2MOTION+", "PSPC-AAP-3.0", None, "E13PS310203", "ENC14044W06", "PSPC-AAP-3.0", None, "",datetime.date(2023, 6, 28), datetime.date(2014, 5, 28), "3602617", None, None, None, "", "62000", "39000976900071", "", "AR", 245145.00, 1152498, 460999, 245145.00, 0.00, datetime.date(2014, 12, 19), 229989.00, "005800000", "SAS", "SOFICOR MADER", "PRIVE", "COLLABORATIF", "EN GESTION", "EC", "", "")
    ]
    df_data = spark.createDataFrame(data, schema_data)

    # Transformation
    Transfo = TransormationJointure()
    initial_df = Transfo._get_initial_transformation(df_data)
    intermediate_df = Transfo._get_intermediate_transformation(initial_df)

    # Data attendue
    expected = [
    (
       6, 2021, 'PSPC LOwCO2MOTION+', 'LOwCO2MOTION+', 'PSPC-AAP-3.0', '', 'COLLABORATIF', '', 'PSPC', datetime.date(2023, 6, 28), None, None, None, None, None, 'PSPCPSPC-AAP-3.0', 'PSPC-AAP-3.0'
    )]
    df_expected = spark.createDataFrame(expected, schema_intermediate_transformation)

    assert intermediate_df is not None
    assert isinstance(intermediate_df, DataFrame)
    assert intermediate_df.count() > 0
    assert intermediate_df.columns == data_intermediate
    assert intermediate_df.collect()== df_expected.collect()


# Test pour _get_v2_data
def test_get_v2_data(spark):
    
    # Data d'entrée 
    data = [
        (6, 2021, "DC14044W0006", "360261701", "PSPC LOwCO2MOTION+", "PSPC", "LOwCO2MOTION+", "PSPC-AAP-3.0", None, "E13PS310203", "ENC14044W06", "PSPC-AAP-3.0", None, "", datetime.date(2013, 6, 28), datetime.date(2014, 5, 28), "3602617", None, None, None, "", "62000", "39000976900071", "", "AR", 245145.00, 1152498, 460999, 245145.00, 0.00, datetime.date(2014, 12, 19), 229989.00, "005800000", "SAS", "SOFICOR MADER", "PRIVE", "COLLABORATIF", "EN GESTION", "EC", "", "")
    ]
    df_data = spark.createDataFrame(data, schema_data)

    # Transformation
    Transfo = TransormationJointure()
    v2_df = Transfo._get_v2_data(df_data)

    # Data attendue
    expected=[
        (6, 2021, 'PSPC LOwCO2MOTION+', '', '62000', '', '', '')
    ]
    df_expected = spark.createDataFrame(expected,schema_v2_transformation)

    assert v2_df is not None
    assert v2_df.count() > 0
    assert v2_df.collect()== df_expected.collect()
    assert isinstance(v2_df, DataFrame)
    assert set(v2_df.columns) == set(data_v2)


# Test pour get_v3_data
def test_get_v3_data(spark):
    # Data d'entrée 
    data = [
        (6, 2021, "DC14044W0006", "360261701", "PSPC LOwCO2MOTION+", "PSPC", "LOwCO2MOTION+", "PSPC-AAP-3.0", None, "E13PS310203", "ENC14044W06", "PSPC-AAP-3.0", None, None, datetime.date(2013, 6, 28), datetime.date(2014, 5, 28), "3602617", None, None, None, "", "62000", "39000976900071", "", "AR", 245145.00, 1152498, 460999, 245145.00, 0.00, datetime.date(2014, 12, 19), 229989.00, "005800000", "SAS", "SOFICOR MADER", "PRIVE", "COLLABORATIF", "EN GESTION", "EC", "", ""),
        (7, 2021, "DC14044W0007", "360261702", "PSPC LOwCO2MOTION+ 2", "PSPC", "LOwCO2MOTION+ 2", "PSPC-AAP-3.1", None, "E13PS310204", "ENC14044W07", "PSPC-AAP-3.1", None, None, datetime.date(2013,7,28), datetime.date(2014,6,28), "3602618", None, None, None, "", "62001", "39000976900072", "", "AR", 245146.00, 1152499, 461000, 245146.00, 0.00, datetime.date(2014,12,20), 229990.00, "005800001", "SAS", "SOFICOR MADER", "PRIVE", "COLLABORATIF", "EN GESTION", "EC", "", "")
    ]
    df_data = spark.createDataFrame(data, schema_data)

    # Transformation
    Transfo = TransormationJointure()
    v3_df = Transfo._get_v3_data(df_data)

    expected = [
        (6, 2021, "PSPC LOwCO2MOTION+"),
        (7, 2021, "PSPC LOwCO2MOTION+ 2")
    ]
    df_expected = spark.createDataFrame(expected,schema_v3_transformation)

    assert v3_df is not None
    assert isinstance(v3_df, DataFrame)
    assert v3_df.count() > 0
    assert v3_df.collect()== df_expected.collect()
    assert set(v3_df.columns) == set(data_v3)