import pytest
from pyspark.sql import SparkSession
from test_uniataire.schemadatatest import schema_finale
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
    output_csv_path = "C:/Users/fchaouachi/Desktop/Projet SQL to Spark/Projet_SQL_Spark/output/part-00000-cdd99a46-be61-465b-a360-0948680ef789-c000.csv"
    expected_csv_path = "C:/Users/fchaouachi/Downloads/Final_nextsise_MONO_projet_20240722 (1).csv"
   
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

    #################################N_MOIS_DONN columns#############################
    # Select of the column N_MOIS_DONN
    output_N_MOIS_DONN = df_output.select("N_MOIS_DONN").rdd.flatMap(lambda x: x).distinct().collect()
    expected_N_MOIS_DONN = df_expected.select("N_MOIS_DONN").rdd.flatMap(lambda x: x).distinct().collect()
    
    # Find differing values in output_N_MOIS_DONN not in expected_N_MOIS_DONN
    in_output_N_MOIS_DONN = [value for value in output_N_MOIS_DONN if value not in expected_N_MOIS_DONN]

    # Find differing values in expected_N_MOIS_DONN not in output_N_MOIS_DONN
    in_expected_N_MOIS_DONN = [value for value in expected_N_MOIS_DONN if value not in output_N_MOIS_DONN]

    #################################N_AN_DONNE columns#############################
    # Select of the column N_AN_DONNE
    output_N_AN_DONNE = df_output.select("N_AN_DONNE").rdd.flatMap(lambda x: x).distinct().collect()
    expected_N_AN_DONNE = df_expected.select("N_AN_DONNE").rdd.flatMap(lambda x: x).distinct().collect()
    
    # Find differing values in output_N_AN_DONNE not in expected_N_AN_DONNE
    in_output_N_AN_DONNE = [value for value in output_N_AN_DONNE if value not in expected_N_AN_DONNE]

    # Find differing values in expected_N_AN_DONNE not in output_N_AN_DONNE
    in_expected_N_AN_DONNE = [value for value in expected_N_AN_DONNE if value not in output_N_AN_DONNE]

    #################################V_IDENT_PRJ columns#############################

    # Select of the column V_IDENT_PRJ
    output_V_IDENT_PRJ = df_output.select("V_IDENT_PRJ").rdd.flatMap(lambda x: x).distinct().collect()
    expected_V_IDENT_PRJ = df_expected.select("V_IDENT_PRJ").rdd.flatMap(lambda x: x).distinct().collect()
    
    # Find differing values in output_V_IDENT_PRJ not in expected_V_IDENT_PRJ
    in_output_V_IDENT_PRJ = [value for value in output_V_IDENT_PRJ if value not in expected_V_IDENT_PRJ]

    # Find differing values in expected_V_IDENT_PRJ not in output_V_IDENT_PRJ
    in_expected_V_IDENT_PRJ = [value for value in expected_V_IDENT_PRJ if value not in output_V_IDENT_PRJ]

    #################################V_NOM_PRJ columns#############################
    # Select of the column V_NOM_PRJ
    output_V_NOM_PRJ = df_output.select("V_NOM_PRJ").rdd.flatMap(lambda x: x).distinct().collect()
    expected_V_NOM_PRJ = df_expected.select("V_NOM_PRJ").rdd.flatMap(lambda x: x).distinct().collect()
    
    # Find differing values in output_V_NOM_PRJ not in expected_V_NOM_PRJ
    in_output_V_NOM_PRJ = [value for value in output_V_NOM_PRJ if value not in expected_V_NOM_PRJ]

    # Find differing values in expected_V_NOM_PRJ not in output_V_NOM_PRJ
    in_expected_V_NOM_PRJ = [value for value in expected_V_NOM_PRJ if value not in output_V_NOM_PRJ]

    #################################CONCAT_ACT_PROC columns#############################
    # Select of the column V_NOM_PRJ
    output_CONCAT_ACT_PROC = df_output.select("CONCAT_ACT_PROC").rdd.flatMap(lambda x: x).distinct().collect()
    expected_CONCAT_ACT_PROC = df_expected.select("CONCAT_ACT_PROC").rdd.flatMap(lambda x: x).distinct().collect()
    
    # Find differing values in output_CONCAT_ACT_PROC not in expected_CONCAT_ACT_PROC
    in_output_CONCAT_ACT_PROC = [value for value in output_CONCAT_ACT_PROC if value not in expected_CONCAT_ACT_PROC]

    # Find differing values in expected_CONCAT_ACT_PROC not in output_CONCAT_ACT_PROC
    in_expected_CONCAT_ACT_PROC = [value for value in expected_CONCAT_ACT_PROC if value not in output_CONCAT_ACT_PROC]

    #################################V_PROC_PRJ_2 columns#############################
    # Select of the column V_PROC_PRJ_2
    output_V_PROC_PRJ_2 = df_output.select("V_PROC_PRJ_2").rdd.flatMap(lambda x: x).distinct().collect()
    expected_V_PROC_PRJ_2 = df_expected.select("V_PROC_PRJ_2").rdd.flatMap(lambda x: x).distinct().collect()
    
    # Find differing values in output_V_PROC_PRJ_2 not in expected_V_PROC_PRJ_2
    in_output_V_PROC_PRJ_2 = [value for value in output_V_PROC_PRJ_2 if value not in expected_V_PROC_PRJ_2]

    # Find differing values in expected_V_PROC_PRJ_2 not in output_V_PROC_PRJ_2
    in_expected_V_PROC_PRJ_2 = [value for value in expected_V_PROC_PRJ_2 if value not in output_V_PROC_PRJ_2]

    #################################V_ABD_REJ columns#############################
    # Select of the column V_ABD_REJ
    output_V_ABD_REJ = df_output.select("V_ABD_REJ").rdd.flatMap(lambda x: x).distinct().collect()
    expected_V_ABD_REJ = df_expected.select("V_ABD_REJ").rdd.flatMap(lambda x: x).distinct().collect()
    
    # Find differing values in output_V_ABD_REJ not in expected_V_ABD_REJ
    in_output_V_ABD_REJ = [value for value in output_V_ABD_REJ if value not in expected_V_ABD_REJ]

    # Find differing values in expected_V_ABD_REJ not in output_V_ABD_REJ
    in_expected_V_ABD_REJ = [value for value in expected_V_ABD_REJ if value not in output_V_ABD_REJ]

    #################################C_TYPE_PROG columns#############################
    # Select of the column C_TYPE_PROG
    output_C_TYPE_PROG = df_output.select("C_TYPE_PROG").rdd.flatMap(lambda x: x).distinct().collect()
    expected_C_TYPE_PROG = df_expected.select("C_TYPE_PROG").rdd.flatMap(lambda x: x).distinct().collect()
    
    # Find differing values in output_C_TYPE_PROG not in expected_C_TYPE_PROG
    in_output_C_TYPE_PROG = [value for value in output_C_TYPE_PROG if value not in expected_C_TYPE_PROG]

    # Find differing values in expected_C_TYPE_PROG not in output_C_TYPE_PROG
    in_expected_C_TYPE_PROG = [value for value in expected_C_TYPE_PROG if value not in output_C_TYPE_PROG]

    #################################L_DEST_REPORT columns#############################
    # Select of the column L_DEST_REPORT
    output_L_DEST_REPORT = df_output.select("L_DEST_REPORT").rdd.flatMap(lambda x: x).distinct().collect()
    expected_L_DEST_REPORT = df_expected.select("L_DEST_REPORT").rdd.flatMap(lambda x: x).distinct().collect()
    
    # Find differing values in output_L_DEST_REPORT not in expected_L_DEST_REPORT
    in_output_L_DEST_REPORT = [value for value in output_L_DEST_REPORT if value not in expected_L_DEST_REPORT]

    # Find differing values in expected_L_DEST_REPORT not in output_L_DEST_REPORT
    in_expected_L_DEST_REPORT = [value for value in expected_L_DEST_REPORT if value not in output_L_DEST_REPORT]
    
    #################################D_DEPOT_DOSS_INNO columns#############################
    # Select of the column D_DEPOT_DOSS_INNO
    output_D_DEPOT_DOSS_INNO = df_output.select("D_DEPOT_DOSS_INNO").rdd.flatMap(lambda x: x).distinct().collect()
    expected_D_DEPOT_DOSS_INNO = df_expected.select("D_DEPOT_DOSS_INNO").rdd.flatMap(lambda x: x).distinct().collect()
    
    # Find differing values in output_D_DEPOT_DOSS_INNO not in expected_D_DEPOT_DOSS_INNO
    in_output_D_DEPOT_DOSS_INNO = [value for value in output_D_DEPOT_DOSS_INNO if value not in expected_D_DEPOT_DOSS_INNO]

    # Find differing values in expected_D_DEPOT_DOSS_INNO not in output_D_DEPOT_DOSS_INNO
    in_expected_D_DEPOT_DOSS_INNO = [value for value in expected_D_DEPOT_DOSS_INNO if value not in output_D_DEPOT_DOSS_INNO]
    
    #################################D_ENG columns#############################
    # Select the D_ENG column
    output_D_ENG = df_output.select("D_ENG").rdd.flatMap(lambda x: x).distinct().collect()

    expected_D_ENG = df_expected.select("D_ENG").rdd.flatMap(lambda x: x).distinct().collect()

    # Find differing values in expected_D_ENG not in output_D_ENG
    in_expected_D_ENG = [value for value in expected_D_ENG if value not in output_D_ENG]

    # Find differing values in output_D_ENG not in expected_D_ENG
    in_output_D_ENG = [value for value in output_D_ENG if value not in expected_D_ENG]

    #################################D_ENG_REV columns#############################
    # Select the D_ENG_REV column
    output_D_ENG_REV = df_output.select("D_ENG_REV").rdd.flatMap(lambda x: x).distinct().collect()

    expected_D_ENG_REV = df_expected.select("D_ENG_REV").rdd.flatMap(lambda x: x).distinct().collect()

    # Find differing values in expected_D_ENG_REV not in output_D_ENG_REV
    in_expected_D_ENG_REV = [value for value in expected_D_ENG_REV if value not in output_D_ENG_REV]

    # Find differing values in output_D_ENG_REV not in expected_D_ENG_REV
    in_output_D_ENG_REV = [value for value in output_D_ENG_REV if value not in expected_D_ENG_REV]

    #################################D_SIGNATURE_CONTRAT columns#############################
    # Select of the column D_SIGNATURE_CONTRAT
    output_D_SIGNATURE_CONTRAT = df_output.select("D_SIGNATURE_CONTRAT").rdd.flatMap(lambda x: x).distinct().collect()
    expected_D_SIGNATURE_CONTRAT = df_expected.select("D_SIGNATURE_CONTRAT").rdd.flatMap(lambda x: x).distinct().collect()
    
    # Find differing values in output_D_SIGNATURE_CONTRAT not in expected_D_SIGNATURE_CONTRAT
    in_output_D_SIGNATURE_CONTRAT = [value for value in output_D_SIGNATURE_CONTRAT if value not in expected_D_SIGNATURE_CONTRAT]

    # Find differing values in expected_D_SIGNATURE_CONTRAT not in output_D_SIGNATURE_CONTRAT
    in_expected_D_SIGNATURE_CONTRAT = [value for value in expected_D_SIGNATURE_CONTRAT if value not in output_D_SIGNATURE_CONTRAT]
    
    #################################D_DEB_PRJ columns#############################
    # Select of the column D_DEB_PRJ
    output_D_DEB_PRJ = df_output.select("D_DEB_PRJ").rdd.flatMap(lambda x: x).distinct().collect()
    expected_D_DEB_PRJ = df_expected.select("D_DEB_PRJ").rdd.flatMap(lambda x: x).distinct().collect()
    
    # Find differing values in output_D_DEB_PRJ not in expected_D_DEB_PRJ
    in_output_D_DEB_PRJ = [value for value in output_D_DEB_PRJ if value not in expected_D_DEB_PRJ]

    # Find differing values in expected_D_DEB_PRJ not in output_D_DEB_PRJ
    in_expected_D_DEB_PRJ = [value for value in expected_D_DEB_PRJ if value not in output_D_DEB_PRJ]

    #################################D_FIN_PRJ columns#############################
    # Select of the column D_FIN_PRJ
    output_D_FIN_PRJ = df_output.select("D_FIN_PRJ").rdd.flatMap(lambda x: x).distinct().collect()
    expected_D_FIN_PRJ = df_expected.select("D_FIN_PRJ").rdd.flatMap(lambda x: x).distinct().collect()
    
    # Find differing values in output_D_FIN_PRJ not in expected_D_FIN_PRJ
    in_output_D_FIN_PRJ = [value for value in output_D_FIN_PRJ if value not in expected_D_FIN_PRJ]

    # Find differing values in expected_D_FIN_PRJ not in output_D_FIN_PRJ
    in_expected_D_FIN_PRJ = [value for value in expected_D_FIN_PRJ if value not in output_D_FIN_PRJ]

    #################################V_RESUME_PRJ columns#############################
    # Select of the column V_RESUME_PRJ
    output_V_RESUME_PRJ = df_output.select("V_RESUME_PRJ").rdd.flatMap(lambda x: x).distinct().collect()
    expected_V_RESUME_PRJ = df_expected.select("V_RESUME_PRJ").rdd.flatMap(lambda x: x).distinct().collect()
    
    # Find differing values in output_V_RESUME_PRJ not in expected_V_RESUME_PRJ
    in_output_V_RESUME_PRJ = [value for value in output_V_RESUME_PRJ if value not in expected_V_RESUME_PRJ]

    # Find differing values in expected_V_RESUME_PRJ not in output_V_RESUME_PRJ
    in_expected_V_RESUME_PRJ = [value for value in expected_V_RESUME_PRJ if value not in output_V_RESUME_PRJ]

   #################################C_LOC_PRJ columns#############################
    # Select of the column C_LOC_PRJ
    output_C_LOC_PRJ = df_output.select("C_LOC_PRJ").rdd.flatMap(lambda x: x).distinct().collect()
    expected_C_LOC_PRJ = df_expected.select("C_LOC_PRJ").rdd.flatMap(lambda x: x).distinct().collect()
    
    # Find differing values in output_C_LOC_PRJ not in expected_C_LOC_PRJ
    in_output_C_LOC_PRJ = [value for value in output_C_LOC_PRJ if value not in expected_C_LOC_PRJ]

    # Find differing values in expected_C_LOC_PRJ not in output_C_LOC_PRJ
    in_expected_C_LOC_PRJ = [value for value in expected_C_LOC_PRJ if value not in output_C_LOC_PRJ]

   #################################L_THEM_PRJ columns#############################
    # Select of the column L_THEM_PRJ
    output_L_THEM_PRJ = df_output.select("L_THEM_PRJ").rdd.flatMap(lambda x: x).distinct().collect()
    expected_L_THEM_PRJ = df_expected.select("L_THEM_PRJ").rdd.flatMap(lambda x: x).distinct().collect()
    
    # Find differing values in output_L_THEM_PRJ not in expected_L_THEM_PRJ
    in_output_L_THEM_PRJ = [value for value in output_L_THEM_PRJ if value not in expected_L_THEM_PRJ]

    # Find differing values in expected_L_THEM_PRJ not in output_L_THEM_PRJ
    in_expected_L_THEM_PRJ = [value for value in expected_L_THEM_PRJ if value not in output_L_THEM_PRJ]

    #################################V_POT_VISIT columns#############################
    # Select of the column V_POT_VISIT
    output_V_POT_VISIT = df_output.select("V_POT_VISIT").rdd.flatMap(lambda x: x).distinct().collect()
    expected_V_POT_VISIT = df_expected.select("V_POT_VISIT").rdd.flatMap(lambda x: x).distinct().collect()
    
    # Find differing values in output_V_POT_VISIT not in expected_V_POT_VISIT
    in_output_V_POT_VISIT = [value for value in output_V_POT_VISIT if value not in expected_V_POT_VISIT]

    # Find differing values in expected_V_POT_VISIT not in output_V_POT_VISIT
    in_expected_V_POT_VISIT = [value for value in expected_V_POT_VISIT if value not in output_V_POT_VISIT]

   #################################V_APPR_ETAT_PRJ columns#############################
    # Select of the column V_APPR_ETAT_PRJ
    output_V_APPR_ETAT_PRJ = df_output.select("V_APPR_ETAT_PRJ").rdd.flatMap(lambda x: x).distinct().collect()
    expected_V_APPR_ETAT_PRJ = df_expected.select("V_APPR_ETAT_PRJ").rdd.flatMap(lambda x: x).distinct().collect()
    
    # Find differing values in output_V_APPR_ETAT_PRJ not in expected_V_APPR_ETAT_PRJ
    in_output_V_APPR_ETAT_PRJ = [value for value in output_V_APPR_ETAT_PRJ if value not in expected_V_APPR_ETAT_PRJ]

    # Find differing values in expected_V_APPR_ETAT_PRJ not in output_V_APPR_ETAT_PRJ
    in_expected_V_APPR_ETAT_PRJ = [value for value in expected_V_APPR_ETAT_PRJ if value not in output_V_APPR_ETAT_PRJ]

   ################################# compatibilté lignes #############################

    # Colonnes à ignorer lors de la comparaison
    columns_to_ignore = ["V_RESUME_PRJ"]

    # Sélectionner les colonnes à comparer en ignorant celles spécifiées
    df_output = df_output.select([col for col in df_output.columns if col not in columns_to_ignore])
    df_expected = df_expected.select([col for col in df_expected.columns if col not in columns_to_ignore])

    # Vérifier si toutes les lignes de df_output existent dans df_expected
    missing_in_df_expected = df_output.exceptAll(df_expected)  
    # Vérifier si toutes les lignes de df_expected existent dans df_output
    missing_in_df_output = df_expected.exceptAll(df_output)  

    assert (len(in_output_N_MOIS_DONN)==len(in_expected_N_MOIS_DONN)==0)
    assert (len(in_output_N_AN_DONNE)==len(in_expected_N_AN_DONNE)==0)
    assert (len(in_output_V_ABD_REJ)==len(in_expected_V_ABD_REJ)==0) 
    assert (len(in_output_C_TYPE_PROG)==len(in_expected_C_TYPE_PROG)==0)    
    assert (len(in_output_L_DEST_REPORT)==len(in_expected_L_DEST_REPORT)==0)   
    assert (len(in_output_D_DEPOT_DOSS_INNO)==len(in_expected_D_DEPOT_DOSS_INNO)==0)
    assert (len(in_output_D_ENG)==len(in_expected_D_ENG)==0) 
    assert (len(in_output_D_ENG_REV)==len(in_expected_D_ENG_REV)==0) 
    assert (len(in_output_D_SIGNATURE_CONTRAT)==len(in_expected_D_SIGNATURE_CONTRAT)==0)
    assert (len(in_output_D_DEB_PRJ)==len(in_expected_D_DEB_PRJ)==0)
    assert (len(in_output_D_FIN_PRJ)==len(in_expected_D_FIN_PRJ)==0)
    assert (len(in_expected_C_LOC_PRJ)==len(in_output_C_LOC_PRJ)==0)
    assert (len(in_output_L_THEM_PRJ)==len(in_expected_L_THEM_PRJ)==0)
    assert (len (in_output_V_POT_VISIT)==len(in_expected_V_POT_VISIT)==0)
    assert (len(in_output_V_APPR_ETAT_PRJ)==len(in_expected_V_APPR_ETAT_PRJ)==0)
    assert (len(in_output_V_RESUME_PRJ)==len(in_expected_V_RESUME_PRJ))
    assert (len(in_output_V_NOM_PRJ)==len(in_expected_V_NOM_PRJ)==0)
    assert (len(in_output_V_IDENT_PRJ)==len(in_expected_V_IDENT_PRJ)==0)
    assert (missing_in_df_expected.count() == 0)
    assert (missing_in_df_output.count() == 0)
    assert (len(in_output_CONCAT_ACT_PROC)==len(in_expected_CONCAT_ACT_PROC)==0)
    assert (len(in_output_V_PROC_PRJ_2)==len(in_expected_V_PROC_PRJ_2)==0)