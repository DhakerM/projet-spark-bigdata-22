# Projet Spark Big Data – LeBonCoin Streaming Graphe
**Dhaker MEDDEB & Nassim — SPARK**
 
---
 
## Présentation
 
Plateforme de streaming temps réel simulant les interactions d'un site type **LeBonCoin**.  
Le système génère en continu des événements utilisateurs (AIME, VOUT, ACHAT), les traite avec **PySpark Structured Streaming**, modélise les relations sous forme de **graphe avec GraphFrames**, et les visualise dynamiquement via un **dashboard Dash/Plotly**.
 
---
 
## Architecture
 
```
[producer.py] ──socket TCP:9999──> [spark_streaming.py] ──JSON──> [dashboard.py]
  Génère 1 event/s                   PySpark + GraphFrames          http://localhost:8050
                                      écrit /tmp/graph_state.json    rafraîchi toutes les 5s
```
 
---
 
## Prérequis
 
### Python
```bash
pip install pyspark dash plotly
```
 
### GraphFrames (JAR Spark — obligatoire)
GraphFrames est une bibliothèque externe à Spark. Il faut la télécharger au lancement via `--packages` :
 
```bash
# Lancer spark_streaming.py avec le package GraphFrames
spark-submit --packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 spark_streaming.py
```
 
> **Alternative sans spark-submit** : définir la variable d'environnement avant de lancer Python :
> ```bash
> export PYSPARK_SUBMIT_ARGS="--packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 pyspark-shell"
> python spark_streaming.py
> ```
 
---
 
## Lancement (3 terminaux dans l'ordre)
 
### Terminal 1 — Producteur de données
```bash
python producer.py
```
→ Lance un serveur socket sur `localhost:9999`.  
→ Affiche `[Producer] En attente de connexion...` et attend que Spark se connecte.
 
### Terminal 2 — Traitement Spark + GraphFrames
```bash
export PYSPARK_SUBMIT_ARGS="--packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 pyspark-shell"
python spark_streaming.py
```
→ Se connecte au socket, traite les micro-batchs toutes les **5 secondes**.  
→ Sauvegarde l'historique en **Parquet** dans `/tmp/spark_events_history/`.  
→ Met à jour le graphe dans `/tmp/graph_state.json` à chaque batch.  
→ Affiche dans la console les agrégations par fenêtre de 30s.
 
### Terminal 3 — Dashboard de visualisation
```bash
python dashboard.py
```
→ Ouvre le dashboard sur **http://localhost:8050**  
→ Se rafraîchit automatiquement toutes les **5 secondes**.
 
---
 
## Concepts PySpark implémentés
 
| Concept | Fichier | Description |
|---|---|---|
| `SparkSession` | `spark_streaming.py` | Initialisation avec config mémoire et shuffle optimisés (`shuffle.partitions=4`) |
| Structured Streaming | `spark_streaming.py` | Lecture depuis socket TCP avec schéma strict |
| Schema Enforcement | `spark_streaming.py` | `EVENT_SCHEMA` défini manuellement — pas d'inférence automatique |
| Windowing (Sliding) | `spark_streaming.py` | Fenêtre glissante de **30s**, recalculée toutes les **10s** |
| Watermarking | `spark_streaming.py` | `withWatermark("event_time", "10 seconds")` — gère les données en retard |
| Output Mode `update` | `spark_streaming.py` | Seules les lignes modifiées sont renvoyées (adapté aux agrégations avec watermark) |
| `foreachBatch` | `spark_streaming.py` | Appel de `process_batch()` à chaque micro-batch pour mise à jour incrémentale |
| Parquet (historique) | `spark_streaming.py` | Chaque batch est sauvegardé en Parquet pour conserver l'historique complet |
| **GraphFrames** | `spark_streaming.py` | Modélisation du graphe : `vertices_df` (id, type) + `edges_agg` (src, dst, type, weight) |
| **inDegree / outDegree** | `spark_streaming.py` | Calcul de la centralité des nœuds (popularité produit, activité utilisateur) |
 
---
 
## Modélisation du Graphe (GraphFrames)
 
### Sommets (Vertices)
| Champ | Description |
|---|---|
| `id` | Identifiant unique (`usr_*`, `sel_*`, `prod_*`) |
| `type` | Type du nœud : `user`, `seller`, `product` |
| `inDegree` | Nombre de connexions reçues |
| `outDegree` | Nombre de connexions émises |
| `count` | `inDegree + outDegree` → taille du nœud sur le dashboard |
 
### Arêtes (Edges)
| Champ | Description |
|---|---|
| `src` | Nœud source |
| `dst` | Nœud destination |
| `type` | Type de relation : `AIME`, `VOUT`, `ACHAT`, `PROPOSE` |
| `weight` | Nombre d'occurrences de cette interaction |
 
---
 
## Structure des événements (flux entrant)
 
```json
{
  "timestamp": "2026-05-25T09:15:30Z",
  "user_id": "usr_0042",
  "user_city": "Paris",
  "product_id": "prod_0071",
  "product_cat": "High-Tech",
  "seller_id": "sel_0005",
  "action_type": "AIME",
  "price": 349.99
}
```
 
---
 
## Dashboard
 
| Élément | Signification |
|---|---|
| 🔵 Nœud bleu | Utilisateur |
| 🟠 Nœud orange | Vendeur |
| 🟢 Nœud vert | Produit |
| Taille du nœud | Proportionnelle au nombre total d'interactions |
| Lien jaune | Action AIME |
| Lien violet | Action VOUT (intention d'achat) |
| Lien rouge | Action ACHAT |
| Lien gris | Relation PROPOSE (vendeur → produit) |
 
---
 
## Structure du projet
 
```
projet_big_data_MEDDEB/
├── producer.py          # Générateur de flux (serveur socket TCP)
├── spark_streaming.py   # Pipeline PySpark + GraphFrames
├── dashboard.py         # Visualisation Dash/Plotly
└── README.md
```
 
---
 
## Fichiers temporaires générés
 
| Chemin | Contenu |
|---|---|
| `/tmp/graph_state.json` | État courant du graphe (lu par le dashboard) |
| `/tmp/spark_events_history/` | Historique complet en Parquet |
| `/tmp/spark_checkpoint/` | Checkpoint Spark Streaming |
 
