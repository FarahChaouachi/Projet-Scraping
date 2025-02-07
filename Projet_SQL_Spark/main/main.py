# main.py
from process.Resultat import Resultat
from config.config import config

# Get the input path from the config file
input_path = config.get("input_path")

# Get the output path from the config file
output_file_path_joined = config.get("output_path")

# Get the number of months from the config file
N_MOIS_DONN =config.get("N_MOIS_DONN")

# Get the number of years from the config file
N_AN_DONNE=config.get("N_AN_DONNE")

# Get the project identifier from the config file
V_IDENT_PRJ=config.get("V_IDENT_PRJ")
 
# Create an instance of the Resultat class
Data=Resultat(input_path,
              N_MOIS_DONN,
              N_AN_DONNE,
              V_IDENT_PRJ,
              output_file_path_joined
              )

print('test')
# Call the run method
Data.run()
