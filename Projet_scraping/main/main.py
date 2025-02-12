from process.Traitement import Traitement
from config.config import config

input_path=config.get("input_path")
output_path=config.get("output_path")

Resultat=Traitement(input_path,output_path)
Resultat.run()