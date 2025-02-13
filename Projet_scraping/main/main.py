from process.Traitement import Traitement
from config.config import config

input_path=config.get("input_path")
output_path_database=config.get("output_path_database")

Resultat=Traitement(input_path,output_path_database)
Resultat.run()