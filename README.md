# Processus décisionnel et modèles de données

## Exercice 1

L'économie française est dans un état catastrophique. Les mêmes fonctionnaires ayant géré les changements de vitesse sont maintenant en charge de redresser l'économie en stoppant, voire en inversant, la tendance des fermetures d'entreprises.

Vous devez leur apporter votre expertise décisionnelle et leur vendre votre approche **speak with data**.

Dans une brève présentation agréable à lire, présentez les étapes du projet. Prouvez la faisabilité du projet avec des jeux de données existants qui aideront à choisir si une baisse d'impôts pourrait stimuler la création d'entreprise. Ces jeux de données devront également permettre, après l'éventuelle application de la baisse, de mesurer son impact.

### Livrables

- Une brève présentation, agréable à lire, des étapes du projet.
- Une preuve de faisabilité s'appuyant sur des jeux de données existants, permettant :
  - de choisir si une baisse d'impôts pourrait stimuler la création d'entreprise ;
  - de mesurer son impact après l'éventuelle application de la baisse.

---

## Exercice 2

Le gouvernement accepte votre prestation !
Il veut du **no code** car tous les développeurs ont démissionné !

Implémentez les traitements à partir des données stockées dans le data lake (bucket S3), jusqu'au format qui va être inséré dans le data warehouse avec l'ETL **Apache Hop**.

### Livrables

- Une documentation concise du fonctionnement d'Apache Hop et des étapes du flux de traitement.

> **Rappel** : l'étape finale est l'insertion dans le data warehouse, vous devez donc fournir le MCD/MLD de ce dernier.

---

## Exercice 3

Le gouvernement accepte votre prestation !
Il veut une instance Docker :

- Un data lake sous forme de bucket S3 qui contient les jeux de données bruts
- Pour les traitements de données de la présentation de l'exercice précédent
- Un environnement de prototypage Jupyter Notebook pour Python, Scala ou autre
- Un Apache Airflow pour l'orchestration et la visualisation
- Une documentation des flux appliqués aux jeux de données de la présentation, dans le README, avec un schéma d'Airflow.

### Livrables

- Un repo Git avec un Docker qui instancie tout ce qui est nécessaire : Airflow, Jupyter Notebook, runtime Python ou autre, etc.
- Doit fonctionner avec `docker compose up` (ou presque), et être expliqué dans le README.
- README bref mais instructif, pas de la diarrhée d'IA.
- Justifier la forme finale des données dans le README.

> **Rappel** : l'étape finale est l'insertion dans le data warehouse, vous devez donc fournir le MCD/MLD de ce dernier.
