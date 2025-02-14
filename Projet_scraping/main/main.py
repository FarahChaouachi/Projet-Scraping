# Importation de la classe Traitement depuis le module 'process.Traitement'
from process.Traitement import Traitement

# Importation de l'objet de configuration depuis le module 'config.config'
from config.config import config

# Récupération du chemin d'entrée depuis la configuration
input_path = config.get("input_path")

# Récupération du chemin de sortie pour la base de données depuis la configuration
output_path_database = config.get("output_path_database")

# Création d'une instance de la classe 'Traitement' en lui passant les chemins d'entrée et de sortie
Resultat = Traitement(input_path, output_path_database)

# Exécution du traitement via la méthode 'run' de l'objet 'Resultat'
Resultat.run()
