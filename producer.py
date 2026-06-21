"""
Producteur de données - Génère un flux infini d'événements JSON
Simule les interactions utilisateurs d'une plateforme type LeBonCoin
"""

import json
import time
import random
import socket
from datetime import datetime

# Données de test pour simuler des utilisateurs, vendeurs et produits
USERS = [f"usr_{i:04d}" for i in range(1, 51)]
SELLERS = [f"sel_{i:04d}" for i in range(1, 21)]
PRODUCTS = [f"prod_{i:04d}" for i in range(1, 101)]

CITIES = ["Paris", "Lyon", "Marseille", "Toulouse", "Bordeaux", "Nantes", "Lille", "Strasbourg"]
CATEGORIES = ["Véhicules", "Immobilier", "High-Tech", "Mode", "Maison", "Sports", "Jeux", "Animaux"]
ACTIONS = ["AIME", "VOUT", "ACHAT"]
# les poids : les likes sont plus fréquents que les achats
ACTION_WEIGHTS = [0.6, 0.3, 0.1]

PRICE_RANGES = {
    "Véhicules": (500, 30000),
    "Immobilier": (50000, 500000),
    "High-Tech": (50, 2000),
    "Mode": (5, 300),
    "Maison": (10, 1500),
    "Sports": (20, 800),
    "Jeux": (5, 100),
    "Animaux": (30, 500),
}


def generate_event():
    """Génère un événement aléatoire"""
    cat = random.choice(CATEGORIES)
    price_min, price_max = PRICE_RANGES[cat]

    event = {
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "user_id": random.choice(USERS),
        "user_city": random.choice(CITIES),
        "product_id": random.choice(PRODUCTS),
        "product_cat": cat,
        "seller_id": random.choice(SELLERS),
        "action_type": random.choices(ACTIONS, weights=ACTION_WEIGHTS)[0],
        "price": round(random.uniform(price_min, price_max), 2),
    }
    return event


def start_socket_server(host="localhost", port=9999):
    """Lance un serveur socket qui envoie les événements en continu"""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(1)

    print(f"[Producer] En attente de connexion sur {host}:{port}...")
    conn, addr = server.accept()
    print(f"[Producer] Client connecté : {addr}")

    try:
        while True:
            event = generate_event()
            line = json.dumps(event) + "\n"
            conn.sendall(line.encode("utf-8"))
            print(f"[Producer] Envoyé : {event['action_type']} - {event['user_id']} -> {event['product_id']}")
            # on envoie environ 1 événement par seconde
            time.sleep(random.uniform(0.5, 1.5))
    except (BrokenPipeError, ConnectionResetError):
        print("[Producer] Client déconnecté.")
    finally:
        conn.close()
        server.close()


if __name__ == "__main__":
    start_socket_server()
