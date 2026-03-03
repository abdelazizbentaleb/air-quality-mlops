import json
from kafka import KafkaConsumer
from pymongo import MongoClient

# Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'raw_data'
MONGO_URI = 'mongodb://localhost:27017/'

client = MongoClient(MONGO_URI)
db = client['air_quality_db']
collection = db['measurements']

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    api_version=(0, 10, 1)
)

print(f"🎧 Consommateur en attente sur '{TOPIC_NAME}'...")

for message in consumer:
    data = message.value
    # Insertion directe car le format est déjà propre
    collection.insert_one(data)
    print(f"💾 Stocké dans MongoDB : {data['sensor_id']} à {data['timestamp']}")