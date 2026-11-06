# Exercice 3 : data lake, Airflow et data warehouse en Docker

Un `docker compose up` monte toute la chaîne : un DAG Airflow télécharge les données publiques dans un bucket S3, les nettoie puis les charge dans un data warehouse PostgreSQL, et un Jupyter permet d'explorer les deux.

## Lancer

Prérequis : Docker et une connexion internet. Les données ne sont pas versionnées, le DAG les télécharge lui-même.

```bash
cd exercice-3
docker compose up -d --build
```

Ouvrir Airflow sur http://localhost:8080, DAG `chargement_dw`, bouton **Trigger**. Ou en ligne de commande :

```bash
docker compose exec airflow airflow dags trigger chargement_dw
```

Le premier lancement prend environ 40 minutes, dont 30 pour télécharger les 5 Go de données ; les suivants ne retéléchargent que l'année en cours. `docker compose down` arrête tout ; `docker compose down -v` efface aussi le bucket et la base, qui seront à retélécharger.

| Service | Rôle | Accès |
|---|---|---|
| `datalake` | Data lake S3 ([RustFS](https://rustfs.com), compatible MinIO/AWS) | API `localhost:9000`, console http://localhost:9001 (`admin` / `admin12345`) |
| `dw` | Data warehouse PostgreSQL (base `dw`) et métadonnées Airflow (base `airflow`) | `localhost:5433` (`dw` / `dw`) |
| `airflow` | Orchestration et visualisation des flux | http://localhost:8080 |
| `jupyter` | Prototypage Python (`notebooks/exploration.ipynb`) | http://localhost:8888 |

Les ports n'écoutent que sur `127.0.0.1` : les services, sans mot de passe pour Airflow et Jupyter, ne sont pas accessibles depuis le réseau.

Airflow et Jupyter partagent la même image (`Dockerfile`) : Airflow 3.3, DuckDB et JupyterLab. DuckDB lit et écrit directement dans le bucket et dans PostgreSQL : pas de fichier intermédiaire en local.

MinIO a été remplacé par RustFS parce que les images MinIO ne sont plus publiées sur Docker Hub. Le protocole S3 est le même.

## Les flux

```mermaid
flowchart LR
    subgraph web[Sources publiques]
        S1[API BODACC : annonces commerciales]
        S2[data.gouv.fr : données financières INPI]
    end
    subgraph lake[Data lake S3 : bucket datalake]
        R1[raw/bodacc/2008.parquet … année en cours]
        R3[raw/inpi/donnees_financieres.parquet]
        C1[clean/annonces.parquet]
        C2[clean/bilans.parquet]
    end
    subgraph dw[Data warehouse PostgreSQL]
        D[dim_temps, dim_territoire, dim_entreprise]
        F1[fait_annonce]
        F2[fait_bilan]
    end
    S1 --> R1
    S2 --> R3
    R1 --> C1
    R3 --> C2
    C1 & C2 --> D
    C1 --> F1
    C2 --> F2
```

### DAG Airflow `chargement_dw`

```mermaid
flowchart LR
    CB[creer_bucket] --> TB["telecharger_bodacc (une tâche par année)"] & TI[telecharger_inpi]
    TB --> NA[nettoyer_annonces]
    TI --> NB[nettoyer_bilans]
    NA & NB --> CD[charger_dimensions]
    CD --> FA[charger_fait_annonce] & FB[charger_fait_bilan]
    FA & FB --> CT[controler]
```

| Tâche | Ce qu'elle fait |
|---|---|
| `creer_bucket` | Crée le bucket `datalake` s'il n'existe pas. |
| `telecharger_bodacc` | Une tâche par année depuis 2008, 4 en parallèle : exporte les créations et radiations de l'année depuis l'API du BODACC vers `raw/bodacc/<année>.parquet`. Le site coupe sans prévenir les gros exports, d'où le découpage : chaque fichier doit être lisible et avoir le nombre de lignes annoncé par l'API, sinon Airflow relance l'année. Les années passées déjà présentes ne sont pas retéléchargées ; l'année en cours l'est à chaque lancement. |
| `telecharger_inpi` | Récupère sur data.gouv.fr l'adresse de la dernière version du fichier et le dépose dans `raw/inpi/`, s'il n'y est pas déjà. |
| `nettoyer_annonces` | Lit toutes les années, garde les avis initiaux, extrait le SIREN, le type de personne et la forme juridique, classe le motif de radiation. Écrit `clean/annonces.parquet`. |
| `nettoyer_bilans` | Garde les comptes sociaux, un bilan par SIREN et par exercice, extrait CA, résultat et impôt de la liasse fiscale. Écrit `clean/bilans.parquet`. |
| `charger_dimensions` | Vide le data warehouse puis charge les 3 dimensions. Le DAG peut donc être relancé sans doublons. |
| `charger_fait_annonce`, `charger_fait_bilan` | Chargent les faits, en parallèle. |
| `controler` | Échoue si une table est vide ; affiche le nombre de lignes et l'indicateur de l'exercice 1 (sociétés créées / radiées hors radiations d'office, par année). |

### Règles de nettoyage

| Règle | Pourquoi |
|---|---|
| Seuls les avis initiaux (`typeavis = annonce`) | Les annulations et rectificatifs compteraient deux fois la même annonce. |
| SIREN = 1ʳᵉ partie de `registre`, sans espaces, 9 chiffres | `registre` vaut `"449 850 908,449850908"` ou l'inverse selon l'annonce. |
| `type_personne` : `pm` (société) ou `pp` (entrepreneur individuel) | Seules les sociétés paient l'IS. |
| Motif de radiation : `office`, `liquidation`, `autre` | Les radiations d'office sont administratives et faussent le nombre de fermetures. |
| Bilans : comptes consolidés écartés, un bilan par SIREN et par exercice (complet prioritaire sur simplifié), clôture entre 2000 et 2030 | Ne pas compter deux fois la même entreprise ; écarter les dates aberrantes (1919, 2029…). |
| Liasse : `FL` / `HN` / `HK` (régime normal), `210+214+218` / `310` / `306` (simplifié) | Codes du chiffre d'affaires, du résultat et de l'impôt sur les bénéfices. |
| Résultat publié mais impôt absent → impôt = 0 | Une ligne vide de la liasse vaut zéro. |

## Forme finale des données

**Deux étapes de stockage.** `raw/` garde les fichiers tels que téléchargés : on peut toujours tout recalculer. `clean/` contient les données nettoyées, au format du data warehouse, en Parquet : on les relit dans Jupyter sans toucher à la base, et le chargement ne refait pas le nettoyage.

**Un schéma en étoile dans le data warehouse.** Les questions de l'exercice 1 se posent toutes de la même façon : *combien* (annonces, impôt, chiffre d'affaires) *par* année, département, type d'entreprise. Les faits portent ce qu'on compte ou additionne, les dimensions ce par quoi on découpe :

- **deux faits**, parce qu'ils n'ont pas le même grain : une annonce est un événement daté, un bilan est un exercice comptable ;
- **trois dimensions partagées** : le SIREN relie annonces et bilans (survie, impôt des sociétés créées) ; la date et le département permettent de comparer avant et après une réforme, et territoire par territoire ;
- les noms des personnes ne sont pas repris : ils ne servent à aucun indicateur.

### MCD

```mermaid
erDiagram
    ENTREPRISE |o--o{ ANNONCE : "fait l'objet de"
    ENTREPRISE ||--o{ BILAN : "dépose"
    TEMPS ||--o{ ANNONCE : "publiée le"
    TEMPS ||--o{ BILAN : "clôturé le"
    TERRITOIRE ||--o{ ANNONCE : "concerne"

    ENTREPRISE {
        string siren PK
        string type_personne
        string forme_juridique
    }
    ANNONCE {
        string annonce_id PK
        string type_annonce
        string motif_radiation
    }
    BILAN {
        string type_bilan
        string confidentialite
        int chiffre_affaires
        int resultat
        int impot_benefices
    }
    TEMPS {
        date jour PK
        int annee
        int trimestre
        int mois
    }
    TERRITOIRE {
        string departement_code PK
        string departement_nom
        string region_code
        string region_nom
    }
```

### MLD

Créé au premier démarrage de PostgreSQL par `sql/01_dw.sql`.

```mermaid
erDiagram
    dim_entreprise ||--o{ fait_annonce : siren
    dim_entreprise ||--o{ fait_bilan : siren
    dim_temps ||--o{ fait_annonce : jour
    dim_temps ||--o{ fait_bilan : date_cloture
    dim_territoire ||--o{ fait_annonce : departement_code

    dim_temps {
        date jour PK
        smallint annee
        smallint trimestre
        smallint mois
    }
    dim_territoire {
        varchar departement_code PK
        varchar departement_nom
        varchar region_code
        varchar region_nom
    }
    dim_entreprise {
        char siren PK
        varchar type_personne
        varchar forme_juridique
    }
    fait_annonce {
        varchar annonce_id PK
        char siren FK
        date jour FK
        varchar departement_code FK
        varchar type_annonce
        varchar motif_radiation
    }
    fait_bilan {
        char siren PK, FK
        date date_cloture PK, FK
        char type_bilan
        varchar confidentialite
        bigint chiffre_affaires
        bigint resultat
        bigint impot_benefices
    }
```
