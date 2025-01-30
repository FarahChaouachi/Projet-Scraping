from pyspark.sql.types import StructType, StructField, StringType, IntegerType, StringType, StringType,DoubleType,DateType

# Definition of column names
data = [
    "N_MOIS_DONN", "N_AN_DONNE", "N_DOSS_KSP", "C_SDC_CNTN", "V_IDENT_PRJ", "I_TYPE_PROG_INNO",
    "V_NOM_PRJ", "V_PROC_PRJ", "N_PRJ_COLLAB", "C_PRJ_COLLAB", "C_PRJ", "C_REFER_APL_PRJ",
    "B_DOSS_REFER_COLLAB", "V_ABD_REJ", "D_DEPOT_DOSS_INNO", "D_ENG", "CNUPRJ", "D_ENG_REV",
    "D_DEB_PRJ", "D_FIN_PRJ", "V_RESUME_PRJ", "C_LOC_PRJ", "L_SIRET_ETAB_BENEF", "C_STT_CDF",
    "PFIPRO", "M_ENG", "M_DEPE_RET_HT", "M_AIDE_SAISI", "M_NOMINAL", "M_REVERS", "D_SIGNATURE_CONTRAT",
    "M_TOT_DECAIS", "C_SS_FDS", "LFOJUR", "ZINCLA", "C_STT_PUBLIC_PRIVE", "C_TYPE_PROG", "V_ETAPE_PROG",
    "V_STT_PROG", "L_DEST_REPORT", "C_PARTE_REPORT"
]
schema_data = StructType([
    StructField("N_MOIS_DONN", IntegerType(), True),
    StructField("N_AN_DONNE", IntegerType(), True),
    StructField("N_DOSS_KSP", StringType(), True),
    StructField("C_SDC_CNTN", StringType(), True),
    StructField("V_IDENT_PRJ", StringType(), True),
    StructField("I_TYPE_PROG_INNO", StringType(), True),
    StructField("V_NOM_PRJ", StringType(), True),
    StructField("V_PROC_PRJ", StringType(), True),
    StructField("N_PRJ_COLLAB", StringType(), True),
    StructField("C_PRJ_COLLAB", StringType(), True),
    StructField("C_PRJ", StringType(), True),
    StructField("C_REFER_APL_PRJ", StringType(), True),
    StructField("B_DOSS_REFER_COLLAB", StringType(), True),
    StructField("V_ABD_REJ", StringType(), True),
    StructField("D_DEPOT_DOSS_INNO", DateType(), True),
    StructField("D_ENG", DateType(), True),
    StructField("CNUPRJ", StringType(), True),
    StructField("D_ENG_REV", DateType(), True),
    StructField("D_DEB_PRJ", DateType(), True),
    StructField("D_FIN_PRJ", DateType(), True),
    StructField("V_RESUME_PRJ", StringType(), True),
    StructField("C_LOC_PRJ", StringType(), True),
    StructField("L_SIRET_ETAB_BENEF", StringType(), True),
    StructField("C_STT_CDF", StringType(), True),
    StructField("PFIPRO", StringType(), True),
    StructField("M_ENG", StringType(), True),
    StructField("M_DEPE_RET_HT", StringType(), True),
    StructField("M_AIDE_SAISI", StringType(), True),
    StructField("M_NOMINAL", StringType(), True),
    StructField("M_REVERS", StringType(), True),
    StructField("D_SIGNATURE_CONTRAT", DateType(), True),
    StructField("M_TOT_DECAIS", StringType(), True),
    StructField("C_SS_FDS", StringType(), True),
    StructField("LFOJUR", StringType(), True),
    StructField("ZINCLA", StringType(), True),
    StructField("C_STT_PUBLIC_PRIVE", StringType(), True),
    StructField("C_TYPE_PROG", StringType(), True),
    StructField("V_ETAPE_PROG", StringType(), True),
    StructField("V_STT_PROG", StringType(), True),
    StructField("L_DEST_REPORT", StringType(), True),
    StructField("C_PARTE_REPORT", StringType(), True),
])

# Schéma  initial transformation
data_initial=["N_MOIS_DONN", "N_AN_DONNE", "V_IDENT_PRJ", "V_NOM_PRJ", "V_PROC_PRJ", "V_ABD_REJ", "D_DEPOT_DOSS_INNO", "D_ENG", "D_ENG_REV", "D_SIGNATURE_CONTRAT", "D_DEB_PRJ", "D_FIN_PRJ", "C_TYPE_PROG", "L_DEST_REPORT", "I_TYPE_PROG_INNO", "C_PARTE_REPORT", "C_SS_FDS"]

schema_initial_transformation = StructType([
    StructField("N_MOIS_DONN", IntegerType(), True),
    StructField("N_AN_DONNE", IntegerType(), True),
    StructField("V_IDENT_PRJ", StringType(), True),
    StructField("V_NOM_PRJ", StringType(), True),
    StructField("V_PROC_PRJ", StringType(), True),
    StructField("V_ABD_REJ", StringType(), True),
    StructField("D_DEPOT_DOSS_INNO", DateType(), True),
    StructField("D_ENG", DateType(), True),
    StructField("D_ENG_REV", DateType(), True),
    StructField("D_SIGNATURE_CONTRAT", DateType(), True),
    StructField("D_DEB_PRJ", DateType(), True),
    StructField("D_FIN_PRJ", DateType(), True),
    StructField("C_TYPE_PROG", StringType(), True),
    StructField("L_DEST_REPORT", StringType(), True),
    StructField("I_TYPE_PROG_INNO", StringType(), True),
    StructField("C_PARTE_REPORT", StringType(), True),
    StructField("C_SS_FDS", StringType(), True)
])

# Schéma intermediaire transformation
data_intermediate = ['N_MOIS_DONN', 'N_AN_DONNE', 'V_IDENT_PRJ', 'V_NOM_PRJ', 'V_PROC_PRJ', 'V_ABD_REJ', 'C_TYPE_PROG', 'L_DEST_REPORT', 'I_TYPE_PROG_INNO', 'D_DEPOT_DOSS_INNO', 'D_ENG', 'D_ENG_REV', 'D_SIGNATURE_CONTRAT', 'D_DEB_PRJ', 'D_FIN_PRJ', 'CONCAT_ACT_PROC', 'V_PROC_PRJ_2']
schema_intermediate_transformation = StructType([
    StructField('N_MOIS_DONN', IntegerType(), True),
    StructField('N_AN_DONNE', IntegerType(), True),
    StructField('V_IDENT_PRJ', StringType(), True),
    StructField('V_NOM_PRJ', StringType(), True),
    StructField('V_PROC_PRJ', StringType(), True),
    StructField('V_ABD_REJ', StringType(), True),
    StructField('C_TYPE_PROG', StringType(), True),
    StructField('L_DEST_REPORT', StringType(), True),
    StructField('I_TYPE_PROG_INNO', StringType(), True),
    StructField('D_DEPOT_DOSS_INNO', DateType(), True),
    StructField('D_ENG', DateType(), True),
    StructField('D_ENG_REV', DateType(), True),
    StructField('D_SIGNATURE_CONTRAT', DateType(), True),
    StructField('D_DEB_PRJ', DateType(), True),
    StructField('D_FIN_PRJ', DateType(), True),
    StructField('CONCAT_ACT_PROC', StringType(), True),
    StructField('V_PROC_PRJ_2', StringType(), True),
])

# Schéma v2 transformation
data_v2=["N_MOIS_DONN","N_AN_DONNE","V_IDENT_PRJ","V_RESUME_PRJ","C_LOC_PRJ","L_THEM_PRJ","V_POT_VISIT","V_APPR_ETAT_PRJ"]
schema_v2_transformation = StructType([
    StructField("N_MOIS_DONN", IntegerType(), True),
    StructField("N_AN_DONNE", IntegerType(), True),
    StructField("V_IDENT_PRJ", StringType(), True),
    StructField("V_RESUME_PRJ", StringType(), True),
    StructField("C_LOC_PRJ", StringType(), True),
    StructField("L_THEM_PRJ", StringType(), True),
    StructField("V_POT_VISIT", StringType(), True),
    StructField("V_APPR_ETAT_PRJ", StringType(), True)
])

# Schéma v3 transformation
data_v3=["N_MOIS_DONN","N_AN_DONNE","V_IDENT_PRJ"]
schema_v3_transformation = StructType([
    StructField("N_MOIS_DONN", IntegerType(), True),
    StructField("N_AN_DONNE", IntegerType(), True),
    StructField("V_IDENT_PRJ", StringType(), True)
])

# Schéma final transformation 
columns_finale = ["N_MOIS_DONN","N_AN_DONNE","V_IDENT_PRJ","V_NOM_PRJ","CONCAT_ACT_PROC","V_PROC_PRJ_2","V_ABD_REJ","C_TYPE_PROG","L_DEST_REPORT", "D_DEPOT_DOSS_INNO","D_ENG","D_ENG_REV","D_SIGNATURE_CONTRAT","D_DEB_PRJ","D_FIN_PRJ","V_RESUME_PRJ","C_LOC_PRJ","L_THEM_PRJ","V_POT_VISIT","V_APPR_ETAT_PRJ"]
schema_finale = StructType([
    StructField("N_MOIS_DONN", IntegerType(), True),
    StructField("N_AN_DONNE", IntegerType(), True),
    StructField("V_IDENT_PRJ", StringType(), True),
    StructField("V_NOM_PRJ", StringType(), True),
    StructField("CONCAT_ACT_PROC", StringType(), True),
    StructField("V_PROC_PRJ_2", StringType(), True),
    StructField("V_ABD_REJ", StringType(), True),
    StructField("C_TYPE_PROG", StringType(), True),
    StructField("L_DEST_REPORT", StringType(), True),
    StructField("D_DEPOT_DOSS_INNO", DateType(), True),
    StructField("D_ENG", DateType(), True),
    StructField("D_ENG_REV", DateType(), True),
    StructField("D_SIGNATURE_CONTRAT", DateType(), True),
    StructField("D_DEB_PRJ", DateType(), True),
    StructField("D_FIN_PRJ", DateType(), True),
    StructField("V_RESUME_PRJ", StringType(), True),
    StructField("C_LOC_PRJ", StringType(), True),
    StructField("L_THEM_PRJ", StringType(), True),
    StructField("V_POT_VISIT", StringType(), True),
    StructField("V_APPR_ETAT_PRJ", StringType(), True)
])