 # kafkaProducerConsumer
A simple Java project to push/read(Producer/Consumer) messages from Kafka brokers

Steps to run this demo:

Download Kafka from here : https://kafka.apache.org/downloads

Extract to a folder, goto bin/

Start Zookeeper :  zookeeper-server-start.sh kafka/config/zookeeper.properties

Start Kafka Broker:  kafka-server-start.sh kafka/config/server.properties

Create topic :  kafka-topics.sh --bootstrap-server localhost:9092 --create --topic twitter-tweets --partitions 6 --replication-factor 1 

Create consumer : kafka-console-consumer.sh --bootstrap-server localhost:9092  --topic twitter-tweets
