import argparse
import csv
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient

API_KEY = 'TVM3PGDPSLYOQY3T'
ENDPOINT_SCHEMA_URL = 'https://psrc-35wr2.us-central1.gcp.confluent.cloud'
API_SECRET_KEY = '8+QpSlOX/xtaBKbDbvWgZ54dug2eJ6VDifZFjzM8F+XijF+GRdqB419GK99ORpvy'
BOOTSTRAP_SERVER = 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'XNL5OLFHUFODSM44'
SCHEMA_REGISTRY_API_SECRET = 'RkXRkfIWUFJ1W7Wkcgs0xY0Dzu2ZCji13M7Zr+6Q02A/H5rZfapemi3emg1wukAM'


def sasl_conf():
    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                 #  'security.protocol': 'SASL_PLAINTEXT'}
                 'bootstrap.servers': BOOTSTRAP_SERVER,
                 'security.protocol': SECURITY_PROTOCOL,
                 'sasl.username': API_KEY,
                 'sasl.password': API_SECRET_KEY
                 }
    return sasl_conf


def schema_config():
    return {'url': ENDPOINT_SCHEMA_URL,

            'basic.auth.user.info': f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

            }


class Car:
    def __init__(self, record: dict):
        for k, v in record.items():
            setattr(self, k, v)

        self.record = record

    @staticmethod
    def dict_to_car(data: dict, ctx):
        return Car(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic):
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    schema_subject = 'restaurent-take-away-data-value'
    schema_check = schema_registry_client.get_latest_version(schema_subject)
    schema_str = schema_check.schema.schema_str

    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Car.dict_to_car)

    consumer_conf = sasl_conf()
    consumer_conf.update({
        'group.id': 'group1',
        'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            car = json_deserializer(msg.value(), SerializationContext(
                msg.topic(), MessageField.VALUE))

            if car is not None:
                print("User record {}: car: {}\n"
                      .format(msg.key(), car))
                with open('output.csv', 'a', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([car.Quantity, car.Order_Number, car.Order_Date,
                                     car.Item_Name, car.Product_Price, car.Total_products])

            break
        except KeyboardInterrupt:
            break

    consumer.close()


main("restaurent-take-away-data")
