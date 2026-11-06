"""Rapport PDF des indicateurs : chiffres clés et graphiques du notebook.

Le modèle rapport.html (Jinja2) reçoit les graphiques de graphiques.py, les mêmes que le notebook et le site web ;
Chromium sans fenêtre l'imprime en PDF, avec des graphiques vectoriels.
"""
import datetime
import pathlib
import subprocess
import tempfile

import jinja2
import plotly.offline

from graphiques import ANNEE_BILANS, DERNIERE_ANNEE_COMPLETE, GRAPHIQUES, lire

MODELE = pathlib.Path(__file__).with_name("rapport.html")


def nombre(valeur):
    """12345678 -> '12 345 678' (espace insécable fine, comme en français)."""
    return f"{valeur:,}".replace(",", " ")


def chiffres_cles():
    """Les quatre chiffres en tête du rapport : (valeur affichée, libellé)."""
    creations, hors_office = lire("""
        SELECT (sum(annonces) FILTER (WHERE type_annonce = 'creation'))::int,
               (sum(annonces) FILTER (WHERE type_annonce = 'radiation' AND motif_radiation <> 'office'))::int
        FROM kpi_annonces WHERE type_personne = 'pm' AND annee = %s
    """, DERNIERE_ANNEE_COMPLETE)[0]
    impot, part_grandes = lire("""
        SELECT sum(impot_benefices)::float,
               (sum(impot_benefices) FILTER (WHERE tranche_ca LIKE '5.%%') / sum(impot_benefices))::float
        FROM kpi_bilans WHERE annee = %s
    """, ANNEE_BILANS)[0]
    return [
        (nombre(creations), f"sociétés créées en {DERNIERE_ANNEE_COMPLETE}"),
        (nombre(hors_office), f"radiations hors office en {DERNIERE_ANNEE_COMPLETE}"),
        (f"{impot / 1e9:.1f} Md€".replace(".", ","), f"d'IS déclaré en {ANNEE_BILANS} (sociétés au CA publié)"),
        (f"{part_grandes:.0%}".replace("%", " %"), f"de l'IS payé par les entreprises de plus de 50 M€"),
    ]


def generer(pdf):
    """Écrit le rapport PDF dans le fichier pdf (chemin)."""
    html = jinja2.Template(MODELE.read_text(encoding="utf-8")).render(
        date=datetime.date.today().strftime("%d/%m/%Y"),
        plotly_js=plotly.offline.get_plotlyjs(),
        chiffres=chiffres_cles(),
        graphiques=[g().to_html(full_html=False, include_plotlyjs=False, config={"staticPlot": True})
                    for g in GRAPHIQUES],
    )
    with tempfile.NamedTemporaryFile("w", suffix=".html", encoding="utf-8") as page:
        page.write(html)
        page.flush()
        subprocess.run(["chromium", "--headless", "--no-sandbox", "--disable-gpu", "--no-pdf-header-footer",
                        f"--print-to-pdf={pdf}", f"file://{page.name}"],
                       check=True, capture_output=True, timeout=120)
