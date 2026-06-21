"""
Script principal PySpark - Traitement du flux en temps réel
Consomme les événements depuis le socket, applique des fenêtres glissantes
et met à jour le graphe de connexions avec GraphFrames
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, sum as spark_sum, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
import json
import os

# ---- Constantes ----
SOCKET_HOST = "localhost"
SOCKET_PORT = 9999
CHECKPOINT_DIR = "/tmp/spark_checkpoint"
OUTPUT_GRAPH_FILE = "/tmp/graph_state.json"

# ---- Schéma strict des événements (évite l'inférence automatique coûteuse) ----
EVENT_SCHEMA = StructType([
    StructField("timestamp", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_city", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_cat", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("action_type", StringType(), True),
    StructField("price", DoubleType(), True),
])


def init_spark():
    """Initialise la SparkSession avec une config adaptée à du streaming"""
    spark = (
        SparkSession.builder
        .appName("LeBonCoin_Streaming_Graphe")
        .master("local[2]")
        # on réduit le shuffle pour du local (par défaut c'est 200, trop pour du local)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.executor.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def process_batch(batch_df, batch_id):
    """
    Fonction appelée à chaque micro-batch.
    On reconstruit les sommets et arêtes du graphe et on écrit dans un fichier JSON
    que le dashboard lira pour se rafraîchir.
    """
    if batch_df.rdd.isEmpty():
        return

    # Collecte les lignes du batch courant
    rows = batch_df.collect()

    # Chargement de l'état existant du graphe si dispo
    nodes = {}
    edges = {}

    if os.path.exists(OUTPUT_GRAPH_FILE):
        with open(OUTPUT_GRAPH_FILE, "r") as f:
            state = json.load(f)
            nodes = {n["id"]: n for n in state.get("nodes", [])}
            edges = {(e["source"], e["target"], e["type"]): e for e in state.get("edges", [])}

    # Mise à jour incrémentale du graphe
    for row in rows:
        user_id = row["user_id"]
        seller_id = row["seller_id"]
        product_id = row["product_id"]
        action = row["action_type"]
        city = row["user_city"]
        cat = row["product_cat"]

        # Ajout / mise à jour des noeuds
        if user_id not in nodes:
            nodes[user_id] = {"id": user_id, "type": "user", "label": user_id, "city": city, "count": 0}
        nodes[user_id]["count"] += 1

        if seller_id not in nodes:
            nodes[seller_id] = {"id": seller_id, "type": "seller", "label": seller_id, "count": 0}
        nodes[seller_id]["count"] += 1

        if product_id not in nodes:
            nodes[product_id] = {"id": product_id, "type": "product", "label": product_id, "cat": cat, "count": 0}
        nodes[product_id]["count"] += 1

        # Arête utilisateur -> produit (action)
        key_up = (user_id, product_id, action)
        if key_up not in edges:
            edges[key_up] = {"source": user_id, "target": product_id, "type": action, "weight": 0}
        edges[key_up]["weight"] += 1

        # Arête vendeur -> produit (propose)
        key_sp = (seller_id, product_id, "PROPOSE")
        if key_sp not in edges:
            edges[key_sp] = {"source": seller_id, "target": product_id, "type": "PROPOSE", "weight": 0}
        edges[key_sp]["weight"] += 1

    # On écrit l'état mis à jour
    graph_state = {
        "batch_id": batch_id,
        "nodes": list(nodes.values()),
        "edges": list(edges.values()),
    }

    with open(OUTPUT_GRAPH_FILE, "w") as f:
        json.dump(graph_state, f)

    print(f"[Spark] Batch {batch_id} traité - {len(rows)} événements | {len(nodes)} noeuds | {len(edges)} arêtes")


def run_streaming(spark):
    """Lance le pipeline de streaming structuré"""

    # Lecture depuis le socket TCP
    raw_stream = (
        spark.readStream
        .format("socket")
        .option("host", SOCKET_HOST)
        .option("port", SOCKET_PORT)
        .load()
    )

    # Parsing JSON avec le schéma défini
    parsed_stream = raw_stream.select(
        from_json(col("value"), EVENT_SCHEMA).alias("data")
    ).select("data.*")

    # Conversion du timestamp string en vrai timestamp Spark
    parsed_stream = parsed_stream.withColumn(
        "event_time",
        col("timestamp").cast(TimestampType())
    )

    # Watermark pour gérer les données en retard (jusqu'à 10 secondes de délai accepté)
    parsed_with_watermark = parsed_stream.withWatermark("event_time", "10 seconds")

    # Agrégation par fenêtre glissante de 30 secondes, toutes les 10 secondes
    windowed_counts = (
        parsed_with_watermark
        .groupBy(
            window(col("event_time"), "30 seconds", "10 seconds"),
            col("action_type")
        )
        .agg(
            count("*").alias("nb_actions"),
            spark_sum("price").alias("total_price")
        )
    )

    # Output mode "update" : on met à jour seulement les lignes modifiées
    # adapté car on agrège avec watermark
    query_stats = (
        windowed_counts.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .start()
    )

    # Second pipeline : foreachBatch pour la mise à jour du graphe
    query_graph = (
        parsed_stream.writeStream
        .outputMode("append")
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="5 seconds")  # micro-batch toutes les 5s
        .start()
    )

    print("[Spark] Streaming démarré. Ctrl+C pour arrêter.")
    query_graph.awaitTermination()


if __name__ == "__main__":
    spark = init_spark()
    run_streaming(spark)
