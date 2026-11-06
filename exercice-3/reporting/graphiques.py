"""Graphiques des indicateurs, partagés par le notebook, le site web et le rapport.

Un seul code construit chaque graphique (Plotly) : le notebook l'affiche, le site web l'intègre en HTML,
le rapport l'exporte en image. Les graphiques sont donc strictement identiques partout.
Les données viennent des agrégats du data warehouse (kpi_annonces, kpi_bilans).
"""
import os

import plotly.graph_objects as go
import psycopg2

DERNIERE_ANNEE_COMPLETE = 2025
ANNEE_BILANS = 2023

# Couleurs de la présentation (exercice 1)
SARCELLE, ORANGE, BLEU = "#2A9D8F", "#E76F51", "#1F4E79"


def lire(sql, *parametres):
    """Lignes d'une requête sur le data warehouse."""
    con = psycopg2.connect(os.environ["DW_DSN"])
    try:
        with con.cursor() as cur:
            cur.execute(sql, parametres)
            return cur.fetchall()
    finally:
        con.close()


def mise_en_page(fig, titre, **axes):
    """Style commun : titre, fond blanc, légende sous le graphique."""
    fig.update_layout(title=titre, template="plotly_white", height=420, margin=dict(t=60, b=40),
                      legend=dict(orientation="h", y=-0.15), **axes)
    return fig


def societes_par_annee():
    """Sociétés créées et radiées par année, en séparant les radiations d'office (nettoyage administratif)."""
    lignes = lire("""
        SELECT annee,
               sum(annonces) FILTER (WHERE type_annonce = 'creation'),
               sum(annonces) FILTER (WHERE type_annonce = 'radiation'),
               sum(annonces) FILTER (WHERE type_annonce = 'radiation' AND motif_radiation <> 'office')
        FROM kpi_annonces
        WHERE type_personne = 'pm' AND annee <= %s
        GROUP BY annee ORDER BY annee
    """, DERNIERE_ANNEE_COMPLETE)
    annees, creations, radiations, hors_office = zip(*lignes)
    fig = go.Figure([
        go.Scattergl(x=annees, y=creations, name="Créations", mode="lines+markers", line=dict(color=SARCELLE)),
        go.Scattergl(x=annees, y=radiations, name="Radiations (toutes)", mode="lines+markers",
                     line=dict(color=ORANGE)),
        go.Scattergl(x=annees, y=hors_office, name="Radiations hors radiations d'office", mode="lines+markers",
                     line=dict(color=BLEU, dash="dash")),
    ])
    return mise_en_page(fig, "Sociétés créées et radiées par année (BODACC)",
                        xaxis=dict(dtick=2), yaxis=dict(title="sociétés", rangemode="tozero"))


def impot_par_tranche():
    """Impôt sur les bénéfices déclaré, par tranche de chiffre d'affaires."""
    lignes = lire("""
        SELECT tranche_ca, round(impot_benefices / 1e9, 2), entreprises
        FROM kpi_bilans WHERE annee = %s ORDER BY tranche_ca
    """, ANNEE_BILANS)
    tranches, impot, entreprises = zip(*lignes)
    fig = go.Figure(go.Bar(x=impot, y=[t[3:] for t in tranches], orientation="h", marker_color=BLEU,
                           text=impot, textposition="outside", cliponaxis=False, customdata=entreprises,
                           hovertemplate="%{y} : %{x} Md€ d'IS, %{customdata:,} entreprises<extra></extra>"))
    return mise_en_page(fig, f"IS déclaré en {ANNEE_BILANS} par taille d'entreprise",
                        xaxis=dict(title="IS déclaré (Md€)"),
                        yaxis=dict(title="chiffre d'affaires", autorange="reversed"))


GRAPHIQUES = [societes_par_annee, impot_par_tranche]
