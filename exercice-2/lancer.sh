#!/bin/bash
# Point d'entrée du conteneur Hop : déclare le projet monté dans /project, puis exécute un workflow ou un pipeline
# (par défaut chargement_dw.hwf). Les arguments sont passés à hop-run, par exemple --parameters=ANNEE=2026.
set -euo pipefail

# Hop ne lit pas les variables d'environnement : les accès définis dans le compose (data lake, data warehouse)
# lui sont passés comme propriétés système, utilisables dans Hop sous la forme ${NOM}.
proprietes=""
for nom in S3_HOST S3_PORT S3_ACCESS_KEY S3_SECRET_KEY S3_BUCKET PGHOST PGPORT PGDATABASE PGUSER PGPASSWORD; do
  proprietes="${proprietes:+${proprietes},}${nom}=${!nom}"
done

/opt/hop/hop-conf.sh --project=speak-with-data --project-create --project-home=/project \
  --project-config-file=project-config.json --project-keep-config-file > /dev/null

exec /opt/hop/hop-run.sh --project=speak-with-data --runconfig=local --level=Basic \
  --system-properties="${proprietes}" --file="/project/${FICHIER:-chargement_dw.hwf}" "$@"
