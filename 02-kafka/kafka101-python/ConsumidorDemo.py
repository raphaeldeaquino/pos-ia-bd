from confluent_kafka import Consumer
import logging

logging.basicConfig(level=logging.DEBUG)

group_id = 'minha-primeira-app'
topic = 'primeiro_topico'

# cria configurações do consumidor
conf = {'bootstrap.servers': 'localhost:9092',
            'group.id': group_id,
            'session.timeout.ms': 6000,
            'default.topic.config': {
                'auto.offset.reset': 'earliest'
            }
    }

# cria consumidor
consumer = Consumer(**conf)

# subscribe consumidor aos tópicos
consumer.subscribe([topic])

# poll for new data
while True:
    msg = consumer.poll(1)
    if msg:
        logging.info("Received new metadata. \nkey: {}\nValue: {}\nTopic: {}\nPartition: {}\nOffset: {}\nTimestamp: {}"
            .format(msg.key(), msg.value(), msg.topic(), msg.partition(), msg.offset(), msg.timestamp()))