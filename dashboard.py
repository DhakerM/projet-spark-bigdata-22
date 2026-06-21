"""
Dashboard de visualisation du graphe de connexions
Se rafraîchit automatiquement en lisant le fichier JSON produit par Spark
Utilise Dash + Plotly pour le rendu interactif
"""

import json
import os
import random
import math
import dash
from dash import dcc, html, Input, Output, callback_context
import plotly.graph_objects as go

GRAPH_FILE = "/tmp/graph_state.json"
REFRESH_INTERVAL_MS = 5000  # 5 secondes

# Couleurs par type de noeud
NODE_COLORS = {
    "user": "#4A90D9",      # bleu
    "seller": "#E67E22",    # orange
    "product": "#27AE60",   # vert
}

EDGE_COLORS = {
    "AIME": "#F39C12",
    "VOUT": "#8E44AD",
    "ACHAT": "#E74C3C",
    "PROPOSE": "#95A5A6",
}

app = dash.Dash(__name__, title="LeBonCoin Graph Dashboard")

app.layout = html.Div(
    style={"backgroundColor": "#1a1a2e", "minHeight": "100vh", "padding": "20px", "fontFamily": "Arial"},
    children=[
        html.H1(
            "Graphe de Connexions - Plateforme LeBonCoin",
            style={"color": "#ECF0F1", "textAlign": "center", "marginBottom": "5px"}
        ),
        html.P(
            "Visualisation dynamique des interactions Utilisateurs / Vendeurs / Produits",
            style={"color": "#95A5A6", "textAlign": "center", "marginBottom": "20px"}
        ),

        # Légende
        html.Div(
            style={"display": "flex", "justifyContent": "center", "gap": "30px", "marginBottom": "15px"},
            children=[
                html.Span("● Utilisateur", style={"color": NODE_COLORS["user"], "fontWeight": "bold"}),
                html.Span("● Vendeur", style={"color": NODE_COLORS["seller"], "fontWeight": "bold"}),
                html.Span("● Produit", style={"color": NODE_COLORS["product"], "fontWeight": "bold"}),
                html.Span("— AIME", style={"color": EDGE_COLORS["AIME"]}),
                html.Span("— VOUT", style={"color": EDGE_COLORS["VOUT"]}),
                html.Span("— ACHAT", style={"color": EDGE_COLORS["ACHAT"]}),
                html.Span("— PROPOSE", style={"color": EDGE_COLORS["PROPOSE"]}),
            ]
        ),

        # Statistiques
        html.Div(
            id="stats-bar",
            style={"display": "flex", "justifyContent": "center", "gap": "40px", "marginBottom": "15px"}
        ),

        # Graphe principal
        dcc.Graph(
            id="graph-plot",
            style={"height": "600px"},
            config={"displayModeBar": False}
        ),

        # Intervalle de rafraîchissement automatique
        dcc.Interval(
            id="interval-refresh",
            interval=REFRESH_INTERVAL_MS,
            n_intervals=0
        ),

        html.P(
            id="last-update",
            style={"color": "#636e72", "textAlign": "center", "marginTop": "10px", "fontSize": "12px"}
        ),
    ]
)


def load_graph_data():
    """Charge les données du graphe depuis le fichier JSON de Spark"""
    if not os.path.exists(GRAPH_FILE):
        return None

    try:
        with open(GRAPH_FILE, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def compute_positions(nodes):
    """
    Disposition en cercles concentriques selon le type de noeud.
    Pas d'algo de layout complexe, ça suffit pour visualiser.
    """
    positions = {}
    type_groups = {"user": [], "seller": [], "product": []}

    for node in nodes:
        t = node.get("type", "user")
        type_groups[t].append(node["id"])

    # rayons différents selon le type
    radii = {"user": 3.0, "seller": 1.5, "product": 5.5}

    for node_type, ids in type_groups.items():
        r = radii[node_type]
        n = len(ids)
        for i, nid in enumerate(ids):
            angle = 2 * math.pi * i / max(n, 1)
            # on ajoute un peu de bruit pour éviter les superpositions parfaites
            x = r * math.cos(angle) + random.uniform(-0.2, 0.2)
            y = r * math.sin(angle) + random.uniform(-0.2, 0.2)
            positions[nid] = (x, y)

    return positions


def build_figure(graph_data):
    """Construit la figure Plotly à partir des données du graphe"""
    nodes = graph_data.get("nodes", [])
    edges = graph_data.get("edges", [])

    # On limite l'affichage aux 80 noeuds les plus actifs pour lisibilité
    nodes_sorted = sorted(nodes, key=lambda n: n.get("count", 0), reverse=True)[:80]
    visible_ids = {n["id"] for n in nodes_sorted}

    positions = compute_positions(nodes_sorted)

    traces = []

    # Traces des arêtes par type
    edge_groups = {}
    for edge in edges:
        src = edge["source"]
        dst = edge["target"]
        etype = edge["type"]
        if src not in visible_ids or dst not in visible_ids:
            continue
        if etype not in edge_groups:
            edge_groups[etype] = {"x": [], "y": [], "weight": []}
        x0, y0 = positions.get(src, (0, 0))
        x1, y1 = positions.get(dst, (0, 0))
        edge_groups[etype]["x"] += [x0, x1, None]
        edge_groups[etype]["y"] += [y0, y1, None]

    for etype, data in edge_groups.items():
        traces.append(go.Scatter(
            x=data["x"],
            y=data["y"],
            mode="lines",
            line={"color": EDGE_COLORS.get(etype, "#ccc"), "width": 1},
            opacity=0.5,
            name=etype,
            hoverinfo="none",
        ))

    # Traces des noeuds par type
    for node_type in ["user", "seller", "product"]:
        type_nodes = [n for n in nodes_sorted if n.get("type") == node_type]
        if not type_nodes:
            continue

        xs = [positions[n["id"]][0] for n in type_nodes if n["id"] in positions]
        ys = [positions[n["id"]][1] for n in type_nodes if n["id"] in positions]
        # taille proportionnelle au nombre d'interactions
        sizes = [max(8, min(30, n.get("count", 1) * 2)) for n in type_nodes if n["id"] in positions]
        texts = [
            f"{n['id']}<br>Interactions: {n.get('count', 0)}"
            for n in type_nodes if n["id"] in positions
        ]

        label_map = {"user": "Utilisateurs", "seller": "Vendeurs", "product": "Produits"}

        traces.append(go.Scatter(
            x=xs,
            y=ys,
            mode="markers",
            marker={
                "size": sizes,
                "color": NODE_COLORS[node_type],
                "line": {"width": 1, "color": "white"},
            },
            text=texts,
            hoverinfo="text",
            name=label_map[node_type],
        ))

    layout = go.Layout(
        paper_bgcolor="#1a1a2e",
        plot_bgcolor="#16213e",
        font={"color": "#ECF0F1"},
        showlegend=True,
        legend={"bgcolor": "rgba(0,0,0,0.3)", "font": {"color": "white"}},
        xaxis={"showgrid": False, "zeroline": False, "showticklabels": False},
        yaxis={"showgrid": False, "zeroline": False, "showticklabels": False},
        margin={"l": 10, "r": 10, "t": 10, "b": 10},
        hovermode="closest",
    )

    return go.Figure(data=traces, layout=layout)


@app.callback(
    Output("graph-plot", "figure"),
    Output("stats-bar", "children"),
    Output("last-update", "children"),
    Input("interval-refresh", "n_intervals"),
)
def refresh_graph(n):
    """Callback déclenché toutes les 5 secondes pour rafraîchir le graphe"""
    from datetime import datetime

    graph_data = load_graph_data()

    if graph_data is None:
        # Graphe vide en attendant les données Spark
        empty_fig = go.Figure(layout=go.Layout(
            paper_bgcolor="#1a1a2e",
            plot_bgcolor="#16213e",
            annotations=[{
                "text": "En attente des données Spark...",
                "x": 0.5, "y": 0.5,
                "xref": "paper", "yref": "paper",
                "font": {"color": "white", "size": 18},
                "showarrow": False,
            }]
        ))
        return empty_fig, [], "En attente des données..."

    nodes = graph_data.get("nodes", [])
    edges = graph_data.get("edges", [])
    batch_id = graph_data.get("batch_id", "?")

    fig = build_figure(graph_data)

    # Calcul des stats
    nb_users = sum(1 for n in nodes if n.get("type") == "user")
    nb_sellers = sum(1 for n in nodes if n.get("type") == "seller")
    nb_products = sum(1 for n in nodes if n.get("type") == "product")
    nb_edges = len(edges)

    def stat_card(label, value, color):
        return html.Div(
            style={"textAlign": "center"},
            children=[
                html.Div(str(value), style={"color": color, "fontSize": "24px", "fontWeight": "bold"}),
                html.Div(label, style={"color": "#95A5A6", "fontSize": "12px"}),
            ]
        )

    stats = [
        stat_card("Utilisateurs", nb_users, NODE_COLORS["user"]),
        stat_card("Vendeurs", nb_sellers, NODE_COLORS["seller"]),
        stat_card("Produits", nb_products, NODE_COLORS["product"]),
        stat_card("Connexions", nb_edges, "#ECF0F1"),
    ]

    update_text = f"Dernier batch Spark : #{batch_id} | Mis à jour : {datetime.now().strftime('%H:%M:%S')}"

    return fig, stats, update_text


if __name__ == "__main__":
    print("[Dashboard] Démarrage sur http://localhost:8050")
    app.run(debug=False, port=8050)
