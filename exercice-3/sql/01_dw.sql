-- Data warehouse en étoile (base dw) : 2 faits, 3 dimensions. Exécuté au premier démarrage de PostgreSQL.

CREATE TABLE dim_temps (
    jour        DATE PRIMARY KEY,
    annee       SMALLINT NOT NULL,
    trimestre   SMALLINT NOT NULL,
    mois        SMALLINT NOT NULL
);

CREATE TABLE dim_territoire (
    departement_code  VARCHAR(3) PRIMARY KEY,
    departement_nom   VARCHAR(100),
    region_code       VARCHAR(3),
    region_nom        VARCHAR(100)
);

-- SIREN stocké en entier (4 octets au lieu de 10 en texte) : tables, index et jointures plus légers.
-- Les zéros de tête se rétablissent à l'affichage : lpad(siren::text, 9, '0').
CREATE TABLE dim_entreprise (
    siren            INTEGER PRIMARY KEY,
    type_personne    VARCHAR(2),        -- pm : société, pp : entrepreneur individuel
    forme_juridique  VARCHAR(150)
);

CREATE TABLE fait_annonce (
    annonce_id        VARCHAR(20) PRIMARY KEY,
    siren             INTEGER     REFERENCES dim_entreprise,
    jour              DATE        NOT NULL REFERENCES dim_temps,
    departement_code  VARCHAR(3)  REFERENCES dim_territoire,
    type_annonce      VARCHAR(10) NOT NULL CHECK (type_annonce IN ('creation', 'radiation')),
    motif_radiation   VARCHAR(12) CHECK (motif_radiation IN ('office', 'liquidation', 'autre'))
);

CREATE TABLE fait_bilan (
    siren             INTEGER  NOT NULL REFERENCES dim_entreprise,
    date_cloture      DATE     NOT NULL REFERENCES dim_temps,
    type_bilan        CHAR(1)  NOT NULL CHECK (type_bilan IN ('C', 'S')),
    confidentialite   VARCHAR(40),
    chiffre_affaires  BIGINT,
    resultat          BIGINT,
    impot_benefices   BIGINT,
    PRIMARY KEY (siren, date_cloture)
);

CREATE INDEX ON fait_annonce (siren);
-- Les annonces sont insérées dans l'ordre des dates : un index BRIN (un résumé par bloc de pages)
-- suffit pour filtrer sur une période, pour quelques Ko au lieu de 85 Mo en B-tree.
CREATE INDEX ON fait_annonce USING brin (jour);
CREATE INDEX ON fait_bilan (date_cloture);

-- Agrégats des indicateurs, recalculés à la fin de chaque chargement (DAG, tâche actualiser_agregats) :
-- le notebook et les rapports lisent quelques milliers de lignes au lieu de joindre 10,9 M d'annonces à 9,2 M d'entreprises.
CREATE MATERIALIZED VIEW kpi_annonces AS
SELECT t.annee, a.type_annonce, a.motif_radiation, e.type_personne, a.departement_code, count(*) AS annonces
FROM fait_annonce a
JOIN dim_temps t USING (jour)
LEFT JOIN dim_entreprise e USING (siren)
GROUP BY 1, 2, 3, 4, 5
WITH NO DATA;

CREATE MATERIALIZED VIEW kpi_bilans AS
SELECT t.annee,
       CASE WHEN chiffre_affaires < 1e5 THEN '1. < 100 k€'
            WHEN chiffre_affaires < 1e6 THEN '2. 100 k€ - 1 M€'
            WHEN chiffre_affaires < 1e7 THEN '3. 1 - 10 M€'
            WHEN chiffre_affaires < 5e7 THEN '4. 10 - 50 M€'
            ELSE '5. >= 50 M€' END AS tranche_ca,
       count(*) AS entreprises,
       count(*) FILTER (WHERE impot_benefices > 0) AS payent_is,
       sum(impot_benefices) FILTER (WHERE impot_benefices > 0) AS impot_benefices
FROM fait_bilan b
JOIN dim_temps t ON t.jour = b.date_cloture
WHERE chiffre_affaires > 0 AND resultat IS NOT NULL
GROUP BY 1, 2
WITH NO DATA;

-- Base séparée pour les métadonnées d'Airflow
CREATE DATABASE airflow;
