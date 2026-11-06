-- Mesures du data warehouse : tailles des tables et index, durée des requêtes de référence.
-- docker compose exec -T dw psql -U dw -d dw < mesures.sql
-- (chaque requête est lancée deux fois : la seconde mesure se fait avec le cache chaud)

\echo '== Tailles'
SELECT c.relname AS objet,
       CASE c.relkind WHEN 'r' THEN 'table' WHEN 'i' THEN 'index' WHEN 'm' THEN 'vue matérialisée' END AS type,
       pg_size_pretty(pg_relation_size(c.oid)) AS taille
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('r', 'i', 'm')
ORDER BY pg_relation_size(c.oid) DESC;
SELECT pg_size_pretty(pg_database_size(current_database())) AS base;

\timing on
\o /dev/null

\echo '== Q1 : sociétés créées et radiées par année, jointure sur les faits'
SELECT t.annee, count(*) FILTER (WHERE a.type_annonce = 'creation'), count(*) FILTER (WHERE a.type_annonce = 'radiation')
FROM fait_annonce a JOIN dim_entreprise e USING (siren) JOIN dim_temps t USING (jour)
WHERE e.type_personne = 'pm' GROUP BY t.annee;
SELECT t.annee, count(*) FILTER (WHERE a.type_annonce = 'creation'), count(*) FILTER (WHERE a.type_annonce = 'radiation')
FROM fait_annonce a JOIN dim_entreprise e USING (siren) JOIN dim_temps t USING (jour)
WHERE e.type_personne = 'pm' GROUP BY t.annee;

\echo '== Q1 : même indicateur, lu dans kpi_annonces'
SELECT annee, sum(annonces) FILTER (WHERE type_annonce = 'creation'), sum(annonces) FILTER (WHERE type_annonce = 'radiation')
FROM kpi_annonces WHERE type_personne = 'pm' GROUP BY annee;
SELECT annee, sum(annonces) FILTER (WHERE type_annonce = 'creation'), sum(annonces) FILTER (WHERE type_annonce = 'radiation')
FROM kpi_annonces WHERE type_personne = 'pm' GROUP BY annee;

\echo '== Q2 : IS 2023 par tranche de chiffre d affaires, sur les faits'
SELECT CASE WHEN chiffre_affaires < 1e6 THEN 1 ELSE 2 END, count(*), sum(impot_benefices) FILTER (WHERE impot_benefices > 0)
FROM fait_bilan b JOIN dim_temps t ON t.jour = b.date_cloture
WHERE t.annee = 2023 AND chiffre_affaires > 0 AND resultat IS NOT NULL GROUP BY 1;
SELECT CASE WHEN chiffre_affaires < 1e6 THEN 1 ELSE 2 END, count(*), sum(impot_benefices) FILTER (WHERE impot_benefices > 0)
FROM fait_bilan b JOIN dim_temps t ON t.jour = b.date_cloture
WHERE t.annee = 2023 AND chiffre_affaires > 0 AND resultat IS NOT NULL GROUP BY 1;

\echo '== Q2 : même indicateur, lu dans kpi_bilans'
SELECT tranche_ca, entreprises, impot_benefices FROM kpi_bilans WHERE annee = 2023;
SELECT tranche_ca, entreprises, impot_benefices FROM kpi_bilans WHERE annee = 2023;

\echo '== Q3 : annonces d une entreprise (SIREN 449850908)'
SELECT * FROM fait_annonce WHERE siren = 449850908;
SELECT * FROM fait_annonce WHERE siren = 449850908;

\echo '== Q4 : nombre d annonces d un mois'
SELECT count(*) FROM fait_annonce WHERE jour BETWEEN '2024-03-01' AND '2024-03-31';
SELECT count(*) FROM fait_annonce WHERE jour BETWEEN '2024-03-01' AND '2024-03-31';
