# Dictionnaire de configuration contenant les chemins d'entrée et de sortie
config = { 
    # URL d'entrée, ici un lien vers IMDB pour récupérer des données
    "input_path" : "https://www.imdb.com/",
    
    # URL de connexion à la base de données MySQL où les données seront envoyées
    "output_path_database": "jdbc:mysql://localhost:3306/Scrapping"
}
