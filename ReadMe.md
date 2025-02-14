# Documentation du Projet
# 📌 Pipeline Automatisé de Scraping, ETL et Stockage des Données

## 📖 Introduction

Ce projet automatise l'extraction des données des **Top 10 films de la semaine** depuis IMDb en utilisant **Selenium**.  
Les données sont ensuite envoyées vers **Kafka**, traitées par un pipeline **PySpark ETL**, et stockées dans **MySQL**.  
L'exécution est déclenchée automatiquement **chaque lundi** via **Jenkins**.

---

## ⚙️ Architecture du Système

1️⃣ **Scraping des données (Selenium)**  
   - Extraction des **Top 10 films de la semaine** sur IMDb.  
   - Nettoyage et structuration des données.  

2️⃣ **Envoi des données vers Kafka**  
   - Utilisation d'un **Kafka Producer** pour envoyer les données sous forme de messages Kafka.  

3️⃣ **Traitement ETL avec PySpark**  
   - Consommation des messages Kafka via un **Kafka Consumer PySpark**.  
   - Transformation des données :  
     - Nettoyage et structuration  
     - Enrichissement des informations  

4️⃣ **Stockage des données dans MySQL**  
   - Insertion des données transformées dans une base MySQL.  

5️⃣ **Automatisation avec Jenkins**  
   - **Exécution automatique chaque lundi** pour stocker les nouvelles données.  

---

## 🛠️ Technologies utilisées

| Composant       | Technologie          |
|----------------|----------------------|
| **Scraping Web**  | Selenium, Python    |
| **Message Queue** | Apache Kafka        |
| **Traitement ETL** | PySpark (Kafka Consumer) |
| **Base de données** | MySQL             |
| **CI/CD & Automatisation** | Jenkins, Git |

---

## 🚀 Fonctionnalités principales

✅ Extraction automatique des **Top 10 films de la semaine** 🎬  
✅ Envoi des données vers **Kafka** en temps réel  
✅ Transformation des données avec **PySpark**  
✅ Stockage dans **MySQL** pour analyse et reporting 📊  
✅ **Exécution automatique chaque lundi** grâce à **Jenkins**  

---

## 🔥 Avantages du pipeline

- **Automatisé** : Aucun lancement manuel requis, tout est géré par Jenkins.  
- **Scalable** : Kafka et PySpark permettent de gérer de gros volumes de données.  
- **Fiable** : Kafka garantit qu'aucune donnée ne se perd.  
- **Flexible** : Facilement extensible pour intégrer d'autres sources de données.  

---
## 🏁 Conclusion

🚀 Ce pipeline offre une solution **automatisée, robuste et évolutive** pour extraire, transformer et stocker les **Top 10 films IMDb de la semaine**.  
Avec **Jenkins, Kafka et PySpark**, il garantit un traitement fiable et optimisé des données.  

💡 **Prochaine étape** : Étendre le pipeline pour intégrer d'autres catégories de films et générer des rapports analytiques avancés.  

---