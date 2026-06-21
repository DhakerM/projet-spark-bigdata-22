# Projet Spark Big Data – LeBonCoin Streaming Graphe

## Architecture

Le projet est composé de 3 scripts Python indépendants à lancer dans 3 terminaux différents.

```
[producer.py] ──socket──> [spark_streaming.py] ──JSON──> [dashboard.py]
```

## Installation

```bash
pip install pyspark dash plotly graphframes
```

> **Note :** GraphFrames nécessite aussi le package JAR Spark.
> Pour l'utiliser : `pyspark --packages graphframes:graphframes:0.8.2-spark3.2-s_2.12`

## Lancement

### Terminal 1 – Producteur de données
```bash
python producer.py
```
Lance un serveur socket sur `localhost:9999` qui envoie 1 événement/seconde.

### Terminal 2 – Traitement Spark
```bash
python spark_streaming.py
```
Se connecte au socket, traite les flux avec des fenêtres de 30s et met à jour `/tmp/graph_state.json`.

### Terminal 3 – Dashboard
```bash
python dashboard.py
```
Ouvre le dashboard sur [http://localhost:8050](http://localhost:8050). Se rafraîchit toutes les 5 secondes.

---

## Concepts PySpark utilisés

| Concept | Fichier | Description |
|---|---|---|
| SparkSession | `spark_streaming.py` | Initialisation avec config mémoire et shuffle optimisés |
| Structured Streaming | `spark_streaming.py` | Lecture depuis socket TCP avec schéma strict |
| Schema Enforcement | `spark_streaming.py` | `EVENT_SCHEMA` défini manuellement, pas d'inférence |
| Windowing (Sliding) | `spark_streaming.py` | Fenêtre 30s glissante toutes les 10s |
| Watermarking | `spark_streaming.py` | `withWatermark("event_time", "10 seconds")` |
| Output Mode Update | `spark_streaming.py` | Utilisé pour l'agrégation windowed |
| foreachBatch | `spark_streaming.py` | Mise à jour incrémentale du graphe |
| GraphFrames | `spark_streaming.py` | Modélisation sommets/arêtes (vertices/edges) |

## Structure des données

### Événements (flux entrant)
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

### Graphe (état persisté dans `/tmp/graph_state.json`)
- **Noeuds** : `id`, `type` (user/seller/product), `count` (nb d'interactions)
- **Arêtes** : `source`, `target`, `type` (AIME/VOUT/ACHAT/PROPOSE), `weight`
