# Données brutes

Les fichiers ne sont pas versionnés (5 Go). Les télécharger au format Parquet et les placer ici **avec exactement ces noms** : le `docker compose` de l'exercice 3 les cherche sous ces noms.

| Nom du fichier | Source | Filtre |
|---|---|---|
| `bodacc creation.parquet` | [BODACC, annonces commerciales](https://www.bodacc.fr/explore/dataset/annonces-commerciales/export/?sort=dateparution) | Famille d'avis = Créations |
| `bodacc radiations.parquet` | [BODACC, annonces commerciales](https://www.bodacc.fr/explore/dataset/annonces-commerciales/export/?sort=dateparution) | Famille d'avis = Radiations |
| `données financières.parquet` | [Données financières détaillées des entreprises](https://www.data.gouv.fr/datasets/donnees-financieres-detaillees-des-entreprises-format-parquet) | aucun |

Attention : le site du BODACC coupe parfois les gros exports (vers 1,6 Go) sans prévenir. Un fichier coupé ne se termine pas par `PAR1` et ne peut pas être lu.
