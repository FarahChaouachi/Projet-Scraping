# main.py
from process.Resultat import Resultat
from config.config import config

# Effectuer les taches dans le run:

input_path = config.get("input_path")

output_file_path_joined = config.get("output_path")

N_MOIS_DONN =config.get("N_MOIS_DONN")

N_AN_DONNE=config.get("N_AN_DONNE")

V_IDENT_PRJ=config.get("V_IDENT_PRJ")
 
Data=Resultat(input_path,
              N_MOIS_DONN,
              N_AN_DONNE,
              V_IDENT_PRJ,
              output_file_path_joined
              )
Data.run()
