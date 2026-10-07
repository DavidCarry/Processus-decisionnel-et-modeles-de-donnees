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

CREATE TABLE dim_entreprise (
    siren            CHAR(9) PRIMARY KEY,
    type_personne    VARCHAR(2),        -- pm : société, pp : entrepreneur individuel
    forme_juridique  VARCHAR(150)
);

CREATE TABLE fait_annonce (
    annonce_id        VARCHAR(20) PRIMARY KEY,
    siren             CHAR(9)     REFERENCES dim_entreprise,
    jour              DATE        NOT NULL REFERENCES dim_temps,
    departement_code  VARCHAR(3)  REFERENCES dim_territoire,
    type_annonce      VARCHAR(10) NOT NULL CHECK (type_annonce IN ('creation', 'radiation')),
    motif_radiation   VARCHAR(12) CHECK (motif_radiation IN ('office', 'liquidation', 'autre'))
);

CREATE TABLE fait_bilan (
    siren             CHAR(9)  NOT NULL REFERENCES dim_entreprise,
    date_cloture      DATE     NOT NULL REFERENCES dim_temps,
    type_bilan        CHAR(1)  NOT NULL CHECK (type_bilan IN ('C', 'S')),
    confidentialite   VARCHAR(40),
    chiffre_affaires  BIGINT,
    resultat          BIGINT,
    impot_benefices   BIGINT,
    PRIMARY KEY (siren, date_cloture)
);

CREATE INDEX ON fait_annonce (siren);
CREATE INDEX ON fait_annonce (jour);
CREATE INDEX ON fait_bilan (date_cloture);

-- Base séparée pour les métadonnées d'Airflow
CREATE DATABASE airflow;
