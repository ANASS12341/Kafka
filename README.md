# Envoi de données CSV vers un topic Confluent Cloud avec Python

## Présentation

Ce projet met en œuvre un producteur Apache Kafka en Python pour envoyer des données clients vers un topic hébergé sur Confluent Cloud.

Le programme réalise les opérations suivantes :

1. lecture d’un fichier CSV avec Pandas ;
2. conversion de chaque ligne en objet JSON ;
3. connexion sécurisée au cluster Confluent Cloud ;
4. publication d’un message par client ;
5. utilisation de `customer_id` comme clé Kafka ;
6. suivi du statut de livraison des messages ;
7. attente de la confirmation des messages avec `flush()`.

## Architecture simplifiée

```text
Fichier CSV
    |
    v
Pandas DataFrame
    |
    v
Sérialisation JSON
    |
    v
Producteur Python
    |
    v
Topic Kafka sur Confluent Cloud
```

## Technologies utilisées

- Python
- Pandas
- Apache Kafka
- Confluent Cloud
- `confluent-kafka`
- JSON
- Variables d’environnement

## Structure du projet

```text
confluent-kafka-producer/
├── data/
│   └── first_100_customers.csv
├── producer.py
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## Format des données

Le fichier CSV doit contenir les colonnes suivantes :

| Colonne | Description | Exemple |
|---|---|---|
| `customer_id` | Identifiant unique du client | `0` |
| `name` | Nom du client | `Customer_0` |
| `city` | Ville | `Pune` |
| `state` | État ou région | `Maharashtra` |
| `country` | Pays | `India` |
| `registration_date` | Date d’inscription | `2023-06-29` |
| `is_active` | Statut du client | `false` |

Exemple de message publié dans Kafka :

```json
{
  "customer_id": 0,
  "name": "Customer_0",
  "city": "Pune",
  "state": "Maharashtra",
  "country": "India",
  "registration_date": "2023-06-29",
  "is_active": false
}
```

## Prérequis

Avant d’exécuter le projet, il faut disposer de :

- Python 3.9 ou une version ultérieure ;
- un cluster Confluent Cloud actif ;
- un topic Kafka existant ;
- une clé API Confluent Cloud autorisée à écrire dans ce topic ;
- un fichier CSV conforme au format attendu.

## Installation

### 1. Cloner ou télécharger le projet

```bash
git clone <URL_DU_DEPOT>
cd confluent-kafka-producer
```

### 2. Créer un environnement virtuel

Sous Linux ou macOS :

```bash
python -m venv .venv
source .venv/bin/activate
```

Sous Windows :

```powershell
python -m venv .venv
.venv\Scripts\activate
```

### 3. Installer les dépendances

```bash
pip install -r requirements.txt
```

Contenu du fichier `requirements.txt` :

```text
confluent-kafka
pandas
python-dotenv
```

## Configuration sécurisée

Les identifiants Confluent Cloud ne doivent pas être écrits directement dans le code ni ajoutés au dépôt Git.

Créer un fichier `.env` à la racine du projet :

```dotenv
KAFKA_BOOTSTRAP_SERVERS=<BOOTSTRAP_SERVER>
KAFKA_API_KEY=<API_KEY>
KAFKA_API_SECRET=<API_SECRET>
KAFKA_TOPIC=ecommerce
KAFKA_CLIENT_ID=customers-python-producer
CSV_FILE=data/first_100_customers.csv
```

Créer également un fichier `.env.example` sans valeurs confidentielles :

```dotenv
KAFKA_BOOTSTRAP_SERVERS=
KAFKA_API_KEY=
KAFKA_API_SECRET=
KAFKA_TOPIC=ecommerce
KAFKA_CLIENT_ID=customers-python-producer
CSV_FILE=data/first_100_customers.csv
```

Ajouter `.env` dans `.gitignore` :

```gitignore
.env
.venv/
__pycache__/
*.pyc
.ipynb_checkpoints/
```

## Producteur Kafka

Créer le fichier `producer.py` :

```python
import json
import os
import sys

import pandas as pd
from confluent_kafka import KafkaException, Producer
from dotenv import load_dotenv


load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
API_KEY = os.getenv("KAFKA_API_KEY")
API_SECRET = os.getenv("KAFKA_API_SECRET")
TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce")
CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "customers-python-producer")
CSV_FILE = os.getenv("CSV_FILE", "data/first_100_customers.csv")


def validate_configuration():
    """Vérifie que les variables obligatoires sont définies."""
    required_variables = {
        "KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
        "KAFKA_API_KEY": API_KEY,
        "KAFKA_API_SECRET": API_SECRET,
        "KAFKA_TOPIC": TOPIC,
        "CSV_FILE": CSV_FILE,
    }

    missing_variables = [
        name for name, value in required_variables.items() if not value
    ]

    if missing_variables:
        raise ValueError(
            "Variables manquantes : " + ", ".join(missing_variables)
        )


def create_producer():
    """Crée et retourne le producteur Kafka."""
    kafka_config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": API_KEY,
        "sasl.password": API_SECRET,
        "session.timeout.ms": 45000,
        "client.id": CLIENT_ID,
    }

    return Producer(kafka_config)


def delivery_status(error, message):
    """Traite le résultat de livraison d’un message."""
    if error is not None:
        print(
            f"Échec de livraison : key={message.key()!r}, "
            f"erreur={error}"
        )
        return

    print(
        f"Message livré : topic={message.topic()}, "
        f"partition={message.partition()}, "
        f"offset={message.offset()}"
    )


def load_customers(csv_path):
    """Charge et valide les données clients depuis un fichier CSV."""
    required_columns = {
        "customer_id",
        "name",
        "city",
        "state",
        "country",
        "registration_date",
        "is_active",
    }

    dataframe = pd.read_csv(csv_path)

    missing_columns = required_columns.difference(dataframe.columns)

    if missing_columns:
        raise ValueError(
            "Colonnes manquantes dans le CSV : "
            + ", ".join(sorted(missing_columns))
        )

    return dataframe


def send_customers(producer, dataframe):
    """Publie une ligne du DataFrame par message Kafka."""
    queued_messages = 0

    for record in dataframe.to_dict(orient="records"):
        message_key = str(record["customer_id"])

        # default=str permet notamment de sérialiser certaines valeurs
        # de date ou certains types provenant de Pandas.
        message_value = json.dumps(
            record,
            ensure_ascii=False,
            default=str,
        )

        while True:
            try:
                producer.produce(
                    topic=TOPIC,
                    key=message_key,
                    value=message_value,
                    callback=delivery_status,
                )
                queued_messages += 1
                break

            except BufferError:
                # La file locale est pleine : attendre que certaines
                # livraisons soient traitées avant de recommencer.
                producer.poll(1)

        # Exécute régulièrement les callbacks de livraison.
        producer.poll(0)

    remaining_messages = producer.flush()

    if remaining_messages > 0:
        raise KafkaException(
            f"{remaining_messages} message(s) non livré(s) après flush()."
        )

    return queued_messages


def main():
    validate_configuration()

    dataframe = load_customers(CSV_FILE)
    producer = create_producer()

    print(f"{len(dataframe)} enregistrement(s) chargé(s).")
    print(f"Publication dans le topic '{TOPIC}'...")

    message_count = send_customers(producer, dataframe)

    print(
        f"{message_count} message(s) placé(s) dans la file de production."
    )
    print("Traitement des livraisons terminé.")


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, ValueError, KafkaException) as error:
        print(f"Erreur : {error}", file=sys.stderr)
        sys.exit(1)
```

## Exécution

Lancer le producteur avec la commande suivante :

```bash
python producer.py
```

Exemple de sortie :

```text
100 enregistrement(s) chargé(s).
Publication dans le topic 'ecommerce'...
Message livré : topic=ecommerce, partition=2, offset=10
Message livré : topic=ecommerce, partition=1, offset=15
Message livré : topic=ecommerce, partition=0, offset=12
...
Traitement des livraisons terminé.
```

L’ordre des confirmations peut être différent de celui des lignes du CSV, car les messages peuvent être distribués entre plusieurs partitions.

## Exécution dans Google Colab

Installer les dépendances :

```python
!pip install confluent-kafka pandas python-dotenv
```

Les variables d’environnement peuvent ensuite être définies depuis un mécanisme sécurisé de gestion des secrets.

Il est déconseillé d’écrire directement la clé API et son secret dans une cellule du notebook, particulièrement si celui-ci doit être partagé.

Exemple d’affectation à partir de secrets déjà chargés :

```python
import os

os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "<BOOTSTRAP_SERVER>"
os.environ["KAFKA_API_KEY"] = "<API_KEY>"
os.environ["KAFKA_API_SECRET"] = "<API_SECRET>"
os.environ["KAFKA_TOPIC"] = "ecommerce"
os.environ["CSV_FILE"] = "/content/Customers_data/first_100_customers.csv"
```

## Principe de publication

Chaque ligne du fichier CSV devient un message Kafka indépendant.

La clé du message est construite à partir de `customer_id` :

```python
message_key = str(record["customer_id"])
```

La valeur est sérialisée en JSON :

```python
message_value = json.dumps(
    record,
    ensure_ascii=False,
    default=str,
)
```

Le message est ensuite envoyé dans le topic :

```python
producer.produce(
    topic=TOPIC,
    key=message_key,
    value=message_value,
    callback=delivery_status,
)
```

L’utilisation de `customer_id` comme clé permet généralement aux messages associés au même client d’être dirigés vers la même partition, tant que le partitionnement du topic ne change pas.

## Rôle de `poll()` et `flush()`

### `producer.poll(0)`

Cette méthode permet au client Kafka de traiter les événements internes et d’exécuter les fonctions de rappel de livraison.

```python
producer.poll(0)
```

### `producer.flush()`

Cette méthode attend la fin du traitement des messages encore présents dans la file locale du producteur.

```python
remaining_messages = producer.flush()
```

Elle doit être appelée avant la fin du programme pour éviter de quitter alors que des messages attendent encore d’être envoyés.

## Correction de la conversion JSON

Une conversion comme celle-ci :

```python
json_records = df.to_json(orient="records")

with open("customers.json", "w") as file:
    json.dump(json_records, file)
```

effectue une double sérialisation :

1. `to_json()` produit déjà une chaîne JSON ;
2. `json.dump()` sérialise ensuite cette chaîne une seconde fois.

Cela oblige à utiliser deux désérialisations lors de la lecture :

```python
raw_data = json.load(file)
customers_data = json.loads(raw_data)
```

Pour éviter cette double sérialisation, il est possible d’écrire directement le résultat de `to_json()` :

```python
df.to_json(
    "customers.json",
    orient="records",
    indent=4,
    force_ascii=False,
)
```

Puis de lire le fichier normalement :

```python
with open("customers.json", "r", encoding="utf-8") as file:
    customers_data = json.load(file)
```

Une autre possibilité consiste à ne pas créer de fichier JSON intermédiaire :

```python
customers_data = df.to_dict(orient="records")
```

Cette dernière approche est utilisée dans le producteur proposé.

## JSON valide et représentation Python

Il faut éviter d’envoyer les messages avec :

```python
str(record)
```

La représentation produite correspond à un dictionnaire Python, et non nécessairement à un document JSON valide. Par exemple, Python utilise `True` et `False`, alors que JSON utilise `true` et `false`.

La méthode recommandée est :

```python
json.dumps(record)
```

## Vérification dans Confluent Cloud

Après l’exécution :

1. ouvrir le cluster dans Confluent Cloud ;
2. accéder à la section des topics ;
3. sélectionner le topic `ecommerce` ;
4. ouvrir l’outil d’affichage des messages ;
5. vérifier les clés, valeurs, partitions et offsets.

Les autorisations associées à la clé API doivent permettre l’écriture dans le topic.

## Gestion des erreurs

Le programme traite notamment les situations suivantes :

- variables d’environnement manquantes ;
- fichier CSV introuvable ;
- colonnes obligatoires absentes ;
- file locale du producteur temporairement pleine ;
- erreur de livraison retournée par Kafka ;
- messages encore non livrés après `flush()`.

## Sécurité

Ne jamais publier les éléments suivants dans GitHub, un notebook public, une capture d’écran ou un fichier partagé :

- clé API Confluent Cloud ;
- secret API ;
- identifiants de connexion confidentiels ;
- données personnelles réelles ;
- contenu d’un fichier `.env`.

Si une clé API a été exposée :

1. la révoquer immédiatement dans Confluent Cloud ;
2. générer une nouvelle clé ;
3. mettre à jour les variables d’environnement ;
4. vérifier l’historique Git et les notebooks partagés ;
5. ne pas simplement supprimer la clé du dernier commit, car elle peut rester accessible dans l’historique.

## Améliorations possibles

- ajouter un consommateur Kafka pour valider les messages reçus ;
- envoyer les messages par lots ;
- ajouter des tests automatisés ;
- utiliser Schema Registry avec Avro, Protobuf ou JSON Schema ;
- ajouter un identifiant de suivi ;
- centraliser les logs ;
- mettre en place une stratégie de reprise après erreur ;
- créer une dead-letter queue pour les messages rejetés ;
- conteneuriser le producteur avec Docker.

## Résultat

Le projet permet de publier un ensemble de données clients dans un topic Kafka Confluent Cloud en utilisant :

- une clé Kafka basée sur l’identifiant du client ;
- une valeur sérialisée en JSON valide ;
- une connexion sécurisée avec `SASL_SSL` ;
- une fonction de rappel pour suivre les livraisons ;
- une gestion sécurisée des identifiants par variables d’environnement.
````*
