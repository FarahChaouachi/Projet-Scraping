# Documentation du Projet

## Objectif de l'exercice

Dans ce projet, nous avons :
- Transformé une requête SQL en utilisant des transformations Spark afin de traiter les données de manière plus efficace et évolutive.
- Mis en place des **tests unitaires** pour valider le bon fonctionnement des différentes étapes de traitement des données.
- Réalisé un **test global** pour vérifier si le résultat final du travail correspondait au résultat attendu.

---

## Difficultés rencontrées

### Partie Développement
- La **complexité des requêtes SQL** peut parfois engendrer de la confusion.
- La **définition des schémas** n'est pas toujours simple à formuler.

### Partie Test
- Les **accents** dans la langue française peuvent provoquer des erreurs ou des confusions lors des comparaisons.
- Des **espaces supplémentaires** dans les valeurs attendues (expected) doivent être pris en compte dans les tests.

---

## Architecture du projet

### Dossiers et Contenu
- **`Context` :**  
  Contient la création de la session Spark.
  
- **`Data` :**  
  - Contient les classes pour la manipulation des données.  
  - Inclut les schémas utilisés pour la lecture des données.

- **`Common` :**  
  Contient les fonctions réutilisables pour la manipulation des données.

- **`Process` :**  
  Regroupe les traitements effectués dans le projet.

- **`Main` :**  
  Permet d'exécuter les traitements présents dans le dossier `Process`.

- **`Output` :**  
  Contient les données de sortie générées par le projet.

- **`Test` :**  
  - Regroupe les tests unitaires des différentes fonctions.
  - Inclut un test global pour vérifier si le résultat des traitements correspond aux résultats attendus.

- **`Config` :**  
  Contient les configurations, y compris les chemins (paths) pour les fichiers d'entrée et de sortie.

---

