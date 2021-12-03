#!/bin/bash

# EC2 user data
#!/bin/bash
yum -y update
yum install java-1.8.0
wget https://dlcdn.apache.org/kafka/3.0.0/kafka_2.13-3.0.0.tgz
tar -xvf kafka_2.13-3.0.0.tgz
cd kafka_2.13-3.0.0
export KAFKA_HEAP_OPTS="-Xmx256M -Xms128M"
sed -i '/advertised.listeners=/d' config/server.properties
echo "advertised.listeners=PLAINTEXT://$(curl 169.254.169.254/latest/meta-data/public-ipv4):9092" >> config/server.properties
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

# Parar se em execução
brew services stop zookeeper
brew services stop kafka

# Iniciar zookeeper. Editar o arquivo de propriedades para alterar dataDir
zookeeper-server-start config/zookeeper.properties

# Iniciar kafka. Editar o arquivo de propriedades para alterar log.dirs
kafka-server-start config/server.properties

# Listar os tópicos
kafka-topics -bootstrap-server 127.0.0.1:9092 --list

# Criar um tópico. Neste caso o fator de replicação seria melhor 2 mas como só há um broker não é possível
kafka-topics -bootstrap-server 127.0.0.1:9092 --topic primeiro_topico --create --partitions 3 --replication-factor 1

# Verificar informações sobre um tópico
kafka-topics -bootstrap-server 127.0.0.1:9092 --topic primeiro_topico --describe

# Apagar um tópico
kafka-topics -bootstrap-server 127.0.0.1:9092 --topic primeiro_topico --delete

# Produzir para um tópico
kafka-console-producer --bootstrap-server 127.0.0.1:9092 --topic primeiro_topico

# Produzir para um tópico não existente. Neste caso o tópico será criado com 1 partição e fator de replicação 1, a não ser que 
# você antes edite o arquivo de propriedades em num.partitions. Dessa forma sempre crie um tópico antes de produzir!
kafka-console-producer --bootstrap-server 127.0.0.1:9092 --topic novo_topico

# Consumir de um tópico
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic primeiro_topico

# Consumir do início
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic primeiro_topico --from-beginning

# Inserir grupos. Todos os consumidores de uma mesma aplicação vão dividir a carga, sendo que cada um irá ler de uma partição
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic primeiro_topico --group minha-primeira-app

# Listar grupos de consumidores
kafka-consumer-groups --bootstrap-server localhost:9092 --list

# Descrever um grupo específico
kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group minha-primeira-app