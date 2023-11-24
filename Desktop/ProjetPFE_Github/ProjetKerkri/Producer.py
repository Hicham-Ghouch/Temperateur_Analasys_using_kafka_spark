from kafka import KafkaProducer
from time import sleep
from json import dumps
import json
# Configuration Kafka
producer = KafkaProducer(bootstrap_servers=[':9092'], #change ip here
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

# Fonction de génération de données simulées
def produce_temperature_data(producer):
    while True:
        temperature = random.uniform(20.0, 30.0)  # Générer une température aléatoire
        producer.send('temperature_topic', value=str(temperature))
        producer.flush()
        time.sleep(1)  # Simulation d'envoi de données toutes les secondes

if __name__ == '__main__':
    produce_temperature_data(producer)