# Ce fichier contient la class qui permet de faire notre ETL 

# dans la premiere classe il y a les fonctions pour ( initialisation, run{appel de lecture de data , appel de traitement appel , appel d ecriture data }, fonction lecture , fonction traitement , fonction ecriture )
# dans la deuxieme classe c'est une classe de traitement des données ( implecite ) 

from pyspark.sql import DataFrame
from data.Data_Frame_Reader import *
from common.writer import *
from pyspark.sql.functions import (coalesce, col, concat, concat_ws, desc, first_value, lit, max as f_max, min as f_min, when, regexp_replace)
from pyspark.sql import functions as F
from pyspark.sql.functions import max as f_max
from pyspark.sql.functions import min as f_min
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import when
from pyspark.sql.window import Window
from unidecode import unidecode
import re
   
class Resultat:

    def __init__(self,input_path: str,mois,annee,liste,output_path) -> None:
        
        #initialisation par les paths des données 
        self.input_path: str = input_path
        self.df=None
        self.mois_values=mois
        self.annee_values=annee
        self.liste=liste
        self.output_path=output_path

    
    def run(self) -> None:

        #Transformation des des liens en data frame
        self.df: DataFrame = self._get_data_from_csv(self.input_path)

        # Appliquer la transformation pour enlever les accents sur la colonne "col1"
        # Créer une UDF Spark pour appliquer la fonction
        remove_accents_udf = udf(remove_accents, StringType())
        self.df = self.df.withColumn("V_RESUME_PRJ", remove_accents_udf(self.df["V_RESUME_PRJ"]))
        self.df = self.df.withColumn("V_NOM_PRJ", remove_accents_udf(self.df["V_NOM_PRJ"]))
        self.df = self.df.withColumn("V_IDENT_PRJ", remove_accents_udf(self.df["V_IDENT_PRJ"]))
        self.df = self.df.withColumn("I_TYPE_PROG_INNO", remove_accents_udf(self.df["I_TYPE_PROG_INNO"]))
        self.df = self.df.withColumn("V_PROC_PRJ", remove_accents_udf(self.df["V_PROC_PRJ"]))



        #Transformation et les traitements 
        dataset_joined: DataFrame = self._create_dataset_joined(self.df)



        #Enregistrement du resultat dans un fichier CSV
        self._put_data_to_csv(dataset_joined,self.output_path)

    

    def _get_data_from_csv(self,path)-> DataFrame:  
        # converir le csv en DF
        df_read=Data_Frame_Reader(path)
        df_read.read(path)
        return (df_read.df)
    
    def _create_dataset_joined(self, df: DataFrame) -> DataFrame:

        Transfo = TransormationJointure()

        initial_df = Transfo._get_initial_transformation(df)
        intermediate_df = Transfo._get_intermediate_transformation(initial_df)

        v2_df = Transfo._get_v2_data(df)
        v3_df = Transfo._get_v3_data(df)
        result_df = (
            intermediate_df.alias("V1")
            .join(
                v2_df.alias("V2"),
                (col("V1.N_MOIS_DONN") == col("V2.N_MOIS_DONN"))
                & (col("V1.N_AN_DONNE") == col("V2.N_AN_DONNE"))
                & (col("V1.V_IDENT_PRJ") == col("V2.V_IDENT_PRJ")),
                "left_outer",
            )
            .join(
                v3_df.alias("V3"),
                (col("V1.N_MOIS_DONN") == col("V3.N_MOIS_DONN"))
                & (col("V1.N_AN_DONNE") == col("V3.N_AN_DONNE"))
                & (col("V1.V_IDENT_PRJ") == col("V3.V_IDENT_PRJ"))
                & (col("V1.V_ABD_REJ") != "N"),
                "left_outer",
            )
            .where(
                col("V3.V_IDENT_PRJ").isNull()
                & col("V1.N_MOIS_DONN").isin(self.mois_values)
                & col("V1.N_AN_DONNE").isin(self.annee_values)
                & col("V1.L_DEST_REPORT").isin(self.liste)
            )
        )

        return result_df.select(
            col("V1.N_MOIS_DONN"),
            col("V1.N_AN_DONNE"),
            col("V1.V_IDENT_PRJ"),
            col("V1.V_NOM_PRJ"),
            col("V1.CONCAT_ACT_PROC"),
            col("V1.V_PROC_PRJ_2"),
            col("V1.V_ABD_REJ"),
            col("V1.C_TYPE_PROG"),
            col("V1.L_DEST_REPORT"),
            col("V1.D_DEPOT_DOSS_INNO"),
            col("V1.D_ENG"),
            col("V1.D_ENG_REV"),
            col("V1.D_SIGNATURE_CONTRAT"),
            col("V1.D_DEB_PRJ"),
            col("V1.D_FIN_PRJ"),
            col("V2.V_RESUME_PRJ"),
            col("V2.C_LOC_PRJ"),
            col("V2.L_THEM_PRJ"),
            col("V2.V_POT_VISIT"),
            col("V2.V_APPR_ETAT_PRJ"),
        ).distinct()
    
    def _put_data_to_csv(self,df,path):
        return write_to_csv(df,path)
    


class TransormationJointure:
    def __init__(self) -> None:
        self=None

    def _get_initial_transformation(self,df) -> DataFrame:
        """Première transformation correspondant à la sous-requête la plus interne"""
        proc_prj_condition = when(
            (col("C_TYPE_PROG") == "MONOPARTENAIRE")
            & (
                col("I_TYPE_PROG_INNO").isin(
                    "ATF Régional",
                    "PROJ INNO PIA3 REGIO",
                    "French Tech Tremplin",
                    "FNI",
                )
            ),
            col("V_PROC_PRJ"),
        ).otherwise(
            when(
                col("C_TYPE_PROG") == "MONOPARTENAIRE",
                concat_ws("-", col("I_TYPE_PROG_INNO"), col("V_PROC_PRJ")),
            ).otherwise(col("V_PROC_PRJ"))
        )

        return (
            df.select(
                col("N_MOIS_DONN"),
                col("N_AN_DONNE"),
                col("V_IDENT_PRJ"),
                col("V_NOM_PRJ"),
                proc_prj_condition.alias("V_PROC_PRJ"),
                coalesce(col("V_ABD_REJ"), lit("N")).alias("V_ABD_REJ"),
                col("D_DEPOT_DOSS_INNO"),
                when(col("V_ABD_REJ").isNull(), col("D_ENG")).alias("D_ENG"),
                when(col("V_ABD_REJ").isNull(), col("D_ENG_REV")).alias("D_ENG_REV"),
                when(col("V_ABD_REJ").isNull(), col("D_SIGNATURE_CONTRAT")).alias(
                    "D_SIGNATURE_CONTRAT"
                ),
                when(col("V_ABD_REJ").isNull(), col("D_DEB_PRJ")).alias("D_DEB_PRJ"),
                when(col("V_ABD_REJ").isNull(), col("D_FIN_PRJ")).alias("D_FIN_PRJ"),
                col("C_TYPE_PROG"),
                col("L_DEST_REPORT"),
                col("I_TYPE_PROG_INNO"),
                col("C_PARTE_REPORT"),
                col("C_SS_FDS")

            )
            .where(
                (col("V_PROC_PRJ").isNotNull() | (col("I_TYPE_PROG_INNO") == "PUI"))
                & (~col("C_SS_FDS").like("%813%"))
            )
            .distinct()
        )
    
    def _get_intermediate_transformation(self, df) -> DataFrame:
        """Transformation intermédiaire avec des agrégations"""
        return (
        df.groupBy(
            "N_MOIS_DONN",
            "N_AN_DONNE",
            "V_IDENT_PRJ",
            "V_NOM_PRJ",
            "V_PROC_PRJ",
            "V_ABD_REJ",
            "C_TYPE_PROG",
            "L_DEST_REPORT",
            "I_TYPE_PROG_INNO",
        )
        .agg(
            F.min("D_DEPOT_DOSS_INNO").alias("D_DEPOT_DOSS_INNO"),
            F.min("D_ENG").alias("D_ENG"),
            F.max("D_ENG_REV").alias("D_ENG_REV"),
            F.min("D_SIGNATURE_CONTRAT").alias("D_SIGNATURE_CONTRAT"),
            F.min("D_DEB_PRJ").alias("D_DEB_PRJ"),
            F.max("D_FIN_PRJ").alias("D_FIN_PRJ"),
        )
        .withColumn(
            "CONCAT_ACT_PROC",
            F.when(F.col("I_TYPE_PROG_INNO") == "PUI", F.lit("PUIAAP PUI")).otherwise(
                F.concat(F.col("I_TYPE_PROG_INNO"), F.col("V_PROC_PRJ"))
            ),
        )
        .withColumn(
            "V_PROC_PRJ_2",
            F.when(F.col("I_TYPE_PROG_INNO") == "PUI", F.lit("AAP PUI")).otherwise(
                F.col("V_PROC_PRJ")
            ),
        )

    )

    def _get_v2_data(self,df) -> DataFrame:
        """Préparation des données pour V2 avec des fonctions fenêtre"""
        windowSpec = Window.partitionBy("V_IDENT_PRJ", "V_ABD_REJ").orderBy(
            "C_SDC_CNTN"
        )
        windowSpecLoc = Window.partitionBy("V_IDENT_PRJ", "V_ABD_REJ").orderBy(
            "C_SDC_CNTN", desc("B_DOSS_REFER_COLLAB"), desc("C_STT_CDF")
        )

        return (
            df.select(
                "N_MOIS_DONN",
                "N_AN_DONNE",
                "V_IDENT_PRJ",
                first_value("V_RESUME_PRJ").over(windowSpec).alias("V_RESUME_PRJ"),
                first_value("C_LOC_PRJ").over(windowSpecLoc).alias("C_LOC_PRJ"),
                when(
                    col("I_TYPE_PROG_INNO") == "SIA",
                    lit("Multisectoriel; Deep Tech; Start Up"),
                )
                .otherwise(lit(""))
                .alias("L_THEM_PRJ"),
                lit("").alias("V_POT_VISIT"),
                lit("").alias("V_APPR_ETAT_PRJ"),
            )
            .where(~col("C_SS_FDS").like("%813%"))
            .distinct()
        )

    def _get_v3_data(self,df) -> DataFrame:
        """Préparation des données pour V3"""
        return (
            df.where((col("V_ABD_REJ").isNull()) & (~col("C_SS_FDS").like("%813%")))
            .select("N_MOIS_DONN", "N_AN_DONNE", "V_IDENT_PRJ")
            .distinct()
        )

def remove_accents(input_str):
    if input_str:
        return input_str.replace('�', 'é')  
    return None