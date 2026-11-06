"""Sources publiques -> data lake (S3) -> nettoyage -> data warehouse (PostgreSQL).

raw/   : fichiers bruts téléchargés depuis le BODACC et data.gouv.fr
clean/ : fichiers nettoyés, au format du data warehouse
dw     : schéma en étoile chargé depuis clean/
"""
import os
import smtplib
import tempfile
from datetime import timedelta
from email.message import EmailMessage

import boto3
import duckdb
import pendulum
import psycopg2
import requests
from airflow.sdk import dag, task

BUCKET = os.environ["S3_BUCKET"]
RAW = f"s3://{BUCKET}/raw"
CLEAN = f"s3://{BUCKET}/clean"

# Sources : le BODACC est exporté par année (les gros exports sont coupés sans prévenir),
# les données financières sont un seul fichier dont data.gouv.fr donne l'adresse de la dernière version.
BODACC = "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/annonces-commerciales"
INPI = "https://www.data.gouv.fr/api/1/datasets/donnees-financieres-detaillees-des-entreprises-format-parquet/"
PREMIERE_ANNEE_BODACC = 2008


def s3():
    """Client S3 du data lake, pour créer le bucket et y déposer les fichiers bruts."""
    return boto3.client("s3", endpoint_url=f"http://{os.environ['S3_ENDPOINT']}", region_name="us-east-1",
                        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
                        aws_secret_access_key=os.environ["S3_SECRET_KEY"])


def existe(cle):
    """Vrai si le fichier est déjà dans le bucket."""
    return s3().list_objects_v2(Bucket=BUCKET, Prefix=cle).get("KeyCount", 0) > 0


def deposer(url, cle, params=None, lignes_attendues=None):
    """Télécharge un Parquet, vérifie qu'il est complet, puis le dépose dans le bucket.

    Un fichier coupé n'a pas de pied de page Parquet : DuckDB refuse de le lire et la tâche échoue,
    Airflow la relance. Si l'API annonce un nombre de lignes, le fichier doit l'avoir exactement.
    """
    with tempfile.NamedTemporaryFile(suffix=".parquet") as fichier:
        with requests.get(url, params=params, stream=True, timeout=600) as reponse:
            reponse.raise_for_status()
            for bloc in reponse.iter_content(chunk_size=1 << 20):
                fichier.write(bloc)
        fichier.flush()
        lignes = duckdb.sql(f"SELECT count(*) FROM read_parquet('{fichier.name}')").fetchone()[0]
        if lignes_attendues is not None and lignes != lignes_attendues:
            raise ValueError(f"{cle} : {lignes} lignes au lieu de {lignes_attendues}")
        s3().upload_file(fichier.name, BUCKET, cle)
        print(f"{cle} : {lignes:,} lignes")


def connexion():
    """DuckDB relié au bucket S3 (lecture/écriture Parquet) et au data warehouse PostgreSQL."""
    con = duckdb.connect()
    con.execute("SET memory_limit = '2GB'; SET threads = 4; SET temp_directory = '/tmp/duckdb';"
                "SET preserve_insertion_order = false")
    con.execute("LOAD httpfs; LOAD postgres")
    con.execute(f"""
        CREATE SECRET (TYPE s3, KEY_ID '{os.environ["S3_ACCESS_KEY"]}', SECRET '{os.environ["S3_SECRET_KEY"]}',
                       ENDPOINT '{os.environ["S3_ENDPOINT"]}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')
    """)
    con.execute(f"ATTACH '{os.environ['DW_DSN']}' AS dw (TYPE postgres)")
    return con


# Chaque semaine : nouvelles données, chargement, puis rapport envoyé par email
@dag(schedule="@weekly", start_date=pendulum.datetime(2026, 1, 1), catchup=False, tags=["speak-with-data"])
def chargement_dw():

    @task
    def creer_bucket():
        client = s3()
        if BUCKET not in [b["Name"] for b in client.list_buckets()["Buckets"]]:
            client.create_bucket(Bucket=BUCKET)

    @task
    def annees_bodacc():
        return list(range(PREMIERE_ANNEE_BODACC, pendulum.now().year + 1))

    # 4 années à la fois pour ménager l'API ; une année ratée (export coupé, coupure réseau) est relancée seule.
    @task(retries=3, retry_delay=timedelta(seconds=30), max_active_tis_per_dag=4)
    def telecharger_bodacc(annee):
        # Les années passées ne changent plus : déjà présentes, on ne les retélécharge pas.
        cle = f"raw/bodacc/{annee}.parquet"
        if annee < pendulum.now().year and existe(cle):
            return
        filtre = {"where": f"familleavis in ('creation', 'radiation') and year(dateparution) = {annee}"}
        attendu = requests.get(f"{BODACC}/records", params={**filtre, "limit": 0}, timeout=60).json()["total_count"]
        deposer(f"{BODACC}/exports/parquet", cle, params=filtre, lignes_attendues=attendu)

    @task(retries=3, retry_delay=timedelta(seconds=30))
    def telecharger_inpi():
        cle = "raw/inpi/donnees_financieres.parquet"
        if existe(cle):
            return
        ressources = requests.get(INPI, timeout=60).json()["resources"]
        deposer(next(r["url"] for r in ressources if r["format"] == "parquet"), cle)

    @task
    def nettoyer_annonces():
        # Avis initiaux seulement (les id sont uniques) ; SIREN = 1re partie de "registre" sans espaces ;
        # motif de radiation déduit du texte de radiationaurcs.
        connexion().execute(f"""
            COPY (
                SELECT id AS annonce_id,
                       dateparution AS jour,
                       familleavis AS type_annonce,
                       nullif(regexp_extract(replace(split_part(registre, ',', 1), ' ', ''), '^[0-9]{{9}}$'), '') AS siren,
                       numerodepartement AS departement_code,
                       departement_nom_officiel AS departement_nom,
                       CAST(region_code AS VARCHAR) AS region_code,
                       region_nom_officiel AS region_nom,
                       nullif(regexp_extract(listepersonnes, '"typePersonne": "(pm|pp)"', 1), '') AS type_personne,
                       nullif(left(lower(trim(regexp_extract(listepersonnes, '"formeJuridique": "([^"]*)"', 1))), 150), '')
                           AS forme_juridique,
                       CASE WHEN familleavis = 'radiation' THEN
                           CASE WHEN radiationaurcs ILIKE '%radiation d''office%' THEN 'office'
                                WHEN radiationaurcs ILIKE '%liquidation%' THEN 'liquidation'
                                ELSE 'autre' END
                       END AS motif_radiation
                FROM read_parquet('{RAW}/bodacc/*.parquet')
                WHERE typeavis = 'annonce'
            ) TO '{CLEAN}/annonces.parquet' (FORMAT parquet)
        """)

    @task
    def nettoyer_bilans():
        # Comptes sociaux uniquement (C complet, S simplifié), un bilan par SIREN et par exercice (C prioritaire).
        # Régime normal : FL = CA, HN = résultat, HK = impôt ; simplifié : 210+214+218 = CA, 310 = résultat, 306 = impôt.
        # Résultat publié mais ligne d'impôt vide -> impôt = 0.
        connexion().execute(f"""
            COPY (
                SELECT siren,
                       date_cloture_exercice AS date_cloture,
                       type_bilan,
                       confidentiality AS confidentialite,
                       CASE type_bilan WHEN 'C' THEN liasse['FL']::BIGINT
                            ELSE nullif(coalesce(liasse['210'], 0)::BIGINT + coalesce(liasse['214'], 0)
                                        + coalesce(liasse['218'], 0), 0)
                       END AS chiffre_affaires,
                       (CASE type_bilan WHEN 'C' THEN liasse['HN'] ELSE liasse['310'] END)::BIGINT AS resultat,
                       CASE WHEN (CASE type_bilan WHEN 'C' THEN liasse['HN'] ELSE liasse['310'] END) IS NOT NULL
                            THEN coalesce(CASE type_bilan WHEN 'C' THEN liasse['HK'] ELSE liasse['306'] END, 0)::BIGINT
                       END AS impot_benefices
                FROM read_parquet('{RAW}/inpi/donnees_financieres.parquet')
                WHERE type_bilan IN ('C', 'S')
                  AND date_cloture_exercice BETWEEN DATE '2000-01-01' AND DATE '2030-12-31'
                  AND regexp_full_match(siren, '[0-9]{{9}}')
                QUALIFY row_number() OVER (PARTITION BY siren, date_cloture_exercice ORDER BY type_bilan) = 1
            ) TO '{CLEAN}/bilans.parquet' (FORMAT parquet)
        """)

    @task
    def charger_dimensions():
        con = connexion()
        con.execute("CALL postgres_execute('dw', 'TRUNCATE fait_annonce, fait_bilan, dim_entreprise, dim_territoire, dim_temps')")
        con.execute("""
            INSERT INTO dw.dim_temps
            SELECT d::DATE, year(d), quarter(d), month(d)
            FROM range(DATE '2000-01-01', DATE '2031-01-01', INTERVAL 1 DAY) t(d)
        """)
        con.execute(f"""
            INSERT INTO dw.dim_territoire
            SELECT departement_code, any_value(departement_nom), any_value(region_code), any_value(region_nom)
            FROM read_parquet('{CLEAN}/annonces.parquet')
            WHERE departement_code IS NOT NULL
            GROUP BY departement_code
        """)
        # Une ligne par SIREN : infos de l'annonce la plus récente, plus les SIREN connus uniquement par leurs bilans.
        con.execute(f"""
            INSERT INTO dw.dim_entreprise
            SELECT siren::INTEGER,
                   arg_max(type_personne, jour) FILTER (WHERE type_personne IS NOT NULL),
                   arg_max(forme_juridique, jour) FILTER (WHERE forme_juridique IS NOT NULL)
            FROM (
                SELECT siren, type_personne, forme_juridique, jour
                FROM read_parquet('{CLEAN}/annonces.parquet') WHERE siren IS NOT NULL
                UNION ALL
                SELECT DISTINCT siren, NULL, NULL, NULL FROM read_parquet('{CLEAN}/bilans.parquet')
            )
            GROUP BY siren
        """)

    @task
    def charger_fait_annonce():
        connexion().execute(f"""
            INSERT INTO dw.fait_annonce
            SELECT annonce_id, siren::INTEGER, jour, departement_code, type_annonce, motif_radiation
            FROM read_parquet('{CLEAN}/annonces.parquet')
            WHERE jour BETWEEN DATE '2000-01-01' AND DATE '2030-12-31'
            ORDER BY jour  -- ordre physique des dates : c'est ce qui rend efficace l'index BRIN sur jour
        """)

    @task
    def charger_fait_bilan():
        connexion().execute(f"""
            INSERT INTO dw.fait_bilan
            SELECT siren::INTEGER, date_cloture, type_bilan, confidentialite, chiffre_affaires, resultat, impot_benefices
            FROM read_parquet('{CLEAN}/bilans.parquet')
        """)

    @task
    def actualiser_agregats():
        # Agrégats des indicateurs recalculés. Puis VACUUM ANALYZE : statistiques à jour pour que PostgreSQL choisisse
        # les bons plans, et pages marquées comme lues (sans cela, les premières requêtes après le chargement réécrivent
        # les pages et sont lentes). VACUUM refuse de tourner dans une transaction : connexion directe, en autocommit.
        # (pas de « with psycopg2.connect() » : ce bloc ouvre une transaction même en autocommit)
        con = psycopg2.connect(os.environ["DW_DSN"])
        con.autocommit = True
        try:
            with con.cursor() as cur:
                cur.execute("REFRESH MATERIALIZED VIEW kpi_annonces")
                cur.execute("REFRESH MATERIALIZED VIEW kpi_bilans")
                cur.execute("VACUUM ANALYZE")
        finally:
            con.close()

    @task
    def controler():
        con = connexion()
        for table in ("dim_temps", "dim_territoire", "dim_entreprise", "fait_annonce", "fait_bilan"):
            n = con.execute(f"SELECT count(*) FROM dw.{table}").fetchone()[0]
            print(f"{table:<15} {n:>12,}")
            if n == 0:
                raise ValueError(f"{table} est vide")
        # Indicateur de l'exercice 1 : sociétés créées / radiées hors radiations d'office, par année
        print(con.execute("""
            SELECT annee,
                   sum(annonces) FILTER (WHERE type_annonce = 'creation') AS creations,
                   sum(annonces) FILTER (WHERE type_annonce = 'radiation' AND motif_radiation <> 'office')
                       AS radiations_hors_office
            FROM dw.kpi_annonces
            WHERE type_personne = 'pm'
            GROUP BY annee ORDER BY annee
        """).df().to_string(index=False))

    @task
    def generer_rapport():
        # Rapport PDF (chiffres clés et graphiques du notebook), archivé dans le data lake
        from rapport import generer
        cle = f"reports/rapport-{pendulum.now():%Y-%m-%d}.pdf"
        with tempfile.TemporaryDirectory() as dossier:
            pdf = f"{dossier}/rapport.pdf"
            generer(pdf)
            s3().upload_file(pdf, BUCKET, cle)
        return cle

    @task
    def envoyer_rapport(cle):
        message = EmailMessage()
        message["Subject"] = "Speak with Data : rapport hebdomadaire"
        message["From"] = "rapports@speak-with-data.local"
        message["To"] = os.environ["RAPPORT_DESTINATAIRES"]
        message.set_content("Bonjour,\n\nVeuillez trouver ci-joint le rapport des indicateurs, "
                            "produit après le chargement du data warehouse.\n")
        pdf = s3().get_object(Bucket=BUCKET, Key=cle)["Body"].read()
        message.add_attachment(pdf, maintype="application", subtype="pdf", filename=cle.split("/")[-1])
        with smtplib.SMTP(os.environ["SMTP_HOST"], int(os.environ["SMTP_PORT"])) as smtp:
            smtp.send_message(message)

    bucket = creer_bucket()
    bodacc = telecharger_bodacc.expand(annee=annees_bodacc())
    inpi = telecharger_inpi()
    bucket >> [bodacc, inpi]

    dims = charger_dimensions()
    bodacc >> nettoyer_annonces() >> dims
    inpi >> nettoyer_bilans() >> dims
    controle = controler()
    dims >> [charger_fait_annonce(), charger_fait_bilan()] >> actualiser_agregats() >> controle
    rapport = generer_rapport()
    controle >> rapport
    envoyer_rapport(rapport)


chargement_dw()
