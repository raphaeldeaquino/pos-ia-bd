from confluent_kafka import Producer

# cria as propriedades do produtor
conf = {'bootstrap.servers': 'localhost:9092'}

# cria o produtor
producer = Producer(**conf)

# cria um registro do produtor
value_bytes = bytes('hello world', encoding='utf-8')

# envia dados de maneira assíncrona
producer.produce('primeiro_topico', value_bytes)

# flush data
producer.flush()