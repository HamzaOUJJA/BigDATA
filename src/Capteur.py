
from kafka import KafkaProducer
import datetime as dt
import json
import random
import time
import uuid


print("\033[1;32m        ########    Sending Data To Kafka!\033[0m")

# ANSI escape codes for bold and colored text
BOLD = '\033[1m'
RED = '\033[31m'
GREEN = '\033[32m'
YELLOW = '\033[33m'
RESET = '\033[0m'


producer = KafkaProducer(
    bootstrap_servers=['localhost:19092'],  # Adresse du serveur Kafka
    value_serializer=lambda x: json.dumps(x).encode('utf-8')  # Sérialisation JSON
)



def generate_transaction():
    transaction_types = ['achat', 'remboursement', 'transfert']
    payment_methods = ['carte_de_credit', 'especes', 'virement_bancaire', 'erreur']

    current_time = dt.datetime.now().isoformat()

    Villes = ["Paris", "Marseille", "Lyon", "Toulouse", "Nice", "Nantes", "Strasbourg", "Montpellier", "Bordeaux", "Lille", "Rennes", "Reims", "Le Havre", "Saint-Étienne", "Toulon" , None]
    Rues = ["Rue de la République", "Rue de Paris", "rue Auguste Delaune", "Rue Gustave Courbet ", "Rue de Luxembourg", "Rue Fontaine", "Rue Zinedine Zidane", "Rue de Bretagne", "Rue Marceaux", "Rue Gambetta", "Rue du Faubourg Saint-Antoine", "Rue de la Grande Armée", "Rue de la Villette", "Rue de la Pompe", "Rue Saint-Michel" , None]

    transaction_data = {
        "id_transaction": str(uuid.uuid4()),
        "type_transaction": random.choice(transaction_types),
        "montant": round(random.uniform(10.0, 1000.0), 2),
        "devise": "USD",
        "date": current_time,
        "lieu": f"{random.choice(Rues)}, {random.choice(Villes)}",
        "moyen_paiement": random.choice(payment_methods),
        "details": {
            "produit": f"Produit{random.randint(1, 100)}",
            "quantite": random.randint(1, 10),
            "prix_unitaire": round(random.uniform(5.0, 200.0), 2)
        },
        "utilisateur": {
            "id_utilisateur": f"User{random.randint(1, 1000)}",
            "nom": f"Utilisateur{random.randint(1, 1000)}",
            "adresse": f"{random.randint(1, 1000)} {random.choice(Rues)}, {random.choice(Villes)}",
            "email": f"utilisateur{random.randint(1, 1000)}@example.com"
        }
    }

    return transaction_data


# Envoi de 200 transactions
for _ in range(100):
    transaction = generate_transaction()
    producer.send('capteur', transaction)
    # Display the transaction with bold and color
    print(f"{BOLD}{GREEN}Transaction envoyée:{RESET} {BOLD}{YELLOW}{json.dumps(transaction, indent=4, ensure_ascii=False)}{RESET}")
    time.sleep(1)

# Fermer le producteur
producer.close()




# KAFKA PRODUCEUR
# Reception des donnes en Pyspark ou en Spark scala
# Manip
# Envoie dans un bucket Minio ou sur hadoop pour les plus téméraire



