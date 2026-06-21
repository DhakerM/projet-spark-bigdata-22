"""
Script principal PySpark - Traitement du flux en temps réel
Consomme les événements depuis le socket, applique des fenêtres glissantes
et met à jour le graphe de connexions avec GraphFrames
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, sum as spark_sum, current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
import json
import os

# Importation de GraphFrames (nécessite le lancement avec --packages)
try:
    from graphframes import GraphFrame
except ImportError:
    print("ATTENTION : Le module graphframes n'est pas trouvé. Lancez Spark avec --packages.")

# ---- Constantes ----
SOCKET_HOST = "localhost"
SOCKET_PORT = 9999
CHECKPOINT_DIR = "/tmp/spark_checkpoint"
OUTPUT_GRAPH_FILE = "/tmp/graph_state.json"
PARQUET_DIR = "/tmp/spark_events_history" # Dossier pour stocker l'historique du flux

# ---- Schéma strict des événements ----
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
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.executor.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def process_batch(batch_df, batch_id):
    """
    Fonction appelée à chaque micro-batch.
    Utilise GraphFrames pour construire le graphe et calculer la centralité.
    """
    if batch_df.rdd.isEmpty():
        return

    # 1. Sauvegarde incrémentale du flux entrant dans un format Big Data (Parquet)
    batch_df.write.mode("append").parquet(PARQUET_DIR)

    # 2. Lecture de l'historique complet pour mettre à jour le graphe global
    history_df = batch_df.sparkSession.read.parquet(PARQUET_DIR)

    # 3. Création du DataFrame des Arêtes (Edges)
    # Actions Utilisateurs vers Produits
    edges_user_prod = history_df.select(
        col("user_id").alias("src"),
        col("product_id").alias("dst"),
        col("action_type").alias("type")
    )
    # Actions Vendeurs vers Produits
    edges_seller_prod = history_df.select(
        col("seller_id").alias("src"),
        col("product_id").alias("dst"),
        lit("PROPOSE").alias("type")
    )
    edges_df = edges_user_prod.unionByName(edges_seller_prod)

    # Agrégation pour calculer le poids des arêtes (évite les doublons)
    edges_agg = edges_df.groupBy("src", "dst", "type").count().withColumnRenamed("count", "weight")

    # 4. Création du DataFrame des Sommets (Vertices)
    users = history_df.select(col("user_id").alias("id"), lit("user").alias("type"))
    sellers = history_df.select(col("seller_id").alias("id"), lit("seller").alias("type"))
    products = history_df.select(col("product_id").alias("id"), lit("product").alias("type"))
    
    vertices_df = users.unionByName(sellers).unionByName(products).distinct()

    # 5. Modélisation avec GraphFrames
    g = GraphFrame(vertices_df, edges_agg)

    # 6. CALCUL DE L'INDICATEUR DE CENTRALITÉ
    # inDegree : Nombre de connexions reçues (ex: popularité d'un produit)
    # outDegree : Nombre de connexions émises (ex: activité d'un utilisateur)
    in_degrees = g.inDegrees
    out_degrees = g.outDegrees

    # Jointure des statistiques avec les sommets
    vertices_with_stats = vertices_df \
        .join(in_degrees, on="id", how="left") \
        .join(out_degrees, on="id", how="left") \
        .fillna(0, subset=["inDegree", "outDegree"])

    # On additionne In et Out pour définir l'importance globale du noeud (qui servira pour la taille sur le dashboard)
    vertices_final = vertices_with_stats.withColumn("count", col("inDegree") + col("outDegree"))

    # 7. Exportation vers le Dashboard
    # C'est la seule fois où l'on ramène les données vers le driver (.collect())
    nodes_list = [row.asDict() for row in vertices_final.collect()]
    edges_list = [{"source": row["src"], "target": row["dst"], "type": row["type"], "weight": row["weight"]} 
                  for row in edges_agg.collect()]

    graph_state = {
        "batch_id": batch_id,
        "nodes": nodes_list,
        "edges": edges_list,
    }

    with open(OUTPUT_GRAPH_FILE, "w") as f:
        json.dump(graph_state, f)

    print(f"[Spark] Batch {batch_id} traité via GraphFrames - {len(nodes_list)} noeuds | {len(edges_list)} arêtes uniques")


def run_streaming(spark):
    """Lance le pipeline de streaming structuré"""

    raw_stream = (
        spark.readStream
        .format("socket")
        .option("host", SOCKET_HOST)
        .option("port", SOCKET_PORT)
        .load()
    )

    parsed_stream = raw_stream.select(
        from_json(col("value"), EVENT_SCHEMA).alias("data")
    ).select("data.*")

    parsed_stream = parsed_stream.withColumn(
        "event_time",
        col("timestamp").cast(TimestampType())
    )

    parsed_with_watermark = parsed_stream.withWatermark("event_time", "10 seconds")

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

    query_stats = (
        windowed_counts.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .start()
    )

    query_graph = (
        parsed_stream.writeStream
        .outputMode("append")
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("[Spark] Streaming démarré. Ctrl+C pour arrêter.")
    query_graph.awaitTermination()


if __name__ == "__main__":
    spark = init_spark()
    run_streaming(spark)
