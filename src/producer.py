import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

# Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'raw_data'

# Initialiser le producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(0, 10, 1)
)

def generate_sensor_data():
    """Génère des données conformes au format du projet (Page 3)"""
    sensors = [
        {"id": "CAP_001", "loc": "Paris-Centre", "lat": 48.8566, "lon": 2.3522},
        {"id": "CAP_002", "loc": "Agadir-Ville", "lat": 30.4278, "lon": -9.5981},
    ]
    
    selected_sensor = random.choice(sensors)
    
    # Simulation de valeurs réalistes
    # PM2.5 normal entre 5 et 25, anomalie possible > 50
    pm25 = random.uniform(5, 30) if random.random() > 0.1 else random.uniform(60, 100)
    
    payload = {
        "sensor_id": selected_sensor["id"],
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "location": {
            "name": selected_sensor["loc"],
            "lat": selected_sensor["lat"], 
            "lon": selected_sensor["lon"]
        },
        "measurements": {
            "pm2_5": round(pm25, 2),
            "pm10": round(pm25 * 1.2 + random.uniform(0, 5), 2),
            "no2": round(random.uniform(10, 40), 2)
        }
    }
    return payload

def run_producer():
    print(f"🚀 Producteur démarré. Envoi de données simulées sur '{TOPIC_NAME}'...")
    try:
        while True:
            data = generate_sensor_data()
            producer.send(TOPIC_NAME, data)
            print(f"✅ Données envoyées : {data['sensor_id']} | PM2.5: {data['measurements']['pm2_5']}")
            time.sleep(5) # Envoi toutes les 5 secondes pour tester
    except KeyboardInterrupt:
        print("Arrêt du producteur.")

if __name__ == "__main__":
    run_producer()