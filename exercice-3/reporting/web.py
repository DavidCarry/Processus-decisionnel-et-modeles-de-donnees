"""Site de reporting : les graphiques du notebook, servis par FastAPI.

Chaque graphique vient de graphiques.py, le module qu'utilise aussi le notebook.
Plotly.js est servi par le site lui-même (copie livrée avec le paquet plotly) : aucune ressource externe.
"""
import plotly.offline
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, Response

from graphiques import GRAPHIQUES

app = FastAPI(title="Speak with Data")

PAGE = """<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Speak with Data</title>
  <script src="/plotly.js"></script>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 0 auto; max-width: 1000px; padding: 16px; color: #1f2933; }}
    h1 {{ color: #1F4E79; margin-bottom: 0; }}
    p {{ color: #6b7280; margin-top: 4px; }}
  </style>
</head>
<body>
  <h1>Speak with Data</h1>
  <p>Créations d'entreprises et fiscalité : indicateurs du data warehouse, mis à jour à chaque chargement.</p>
  {graphiques}
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def accueil():
    """Page des graphiques, calculés à chaque visite depuis les agrégats du data warehouse."""
    return PAGE.format(graphiques="\n".join(
        graphique().to_html(full_html=False, include_plotlyjs=False) for graphique in GRAPHIQUES))


@app.get("/plotly.js")
def plotly_js():
    """Bibliothèque Plotly.js, mise en cache par le navigateur."""
    return Response(plotly.offline.get_plotlyjs(), media_type="application/javascript",
                    headers={"Cache-Control": "max-age=86400"})
