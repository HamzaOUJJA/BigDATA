from kafka import KafkaConsumer
import json

# Définition du consumer Kafka
consumer = KafkaConsumer(
    'capteur',
    bootstrap_servers=['localhost:19092'],  # Connexion au serveur Kafka
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Désérialisation JSON
)

# ANSI escape codes for bold and colored text
BOLD = '\033[1m'
RED = '\033[31m'
GREEN = '\033[32m'
YELLOW = '\033[33m'
RESET = '\033[0m'

print(f"{BOLD}{GREEN}📡 En attente de messages...{RESET}")

for message in consumer:
    transaction = message.value
    # Display the transaction with bold and color
    print(f"{BOLD}{YELLOW}📥 Transaction reçue :{RESET} {BOLD}{RED}{json.dumps(transaction, indent=4, ensure_ascii=False)}{RESET}")
