# KafkaCourse
Project with what I learned about kafka using .Net Core, producer and consumer examples in it 

# To Run
- Install Java JDK https://www.oracle.com/br/java/technologies/javase/javase-jdk8-downloads.html
- Download Binary Kafka at https://kafka.apache.org/downloads
- Start Zookeeper
- Start a Kafka Server in port 9092

# Helpfull Commands

### Start Zookeeper with default configs
`bin\windows\zookeeper-server-start.bat config\zookeeper.properties`

### Start kafka-server with default configs
`bin\windows\kafka-server-start.bat config\server.properties`

### Create a topic
`bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic NEW_TOPIC`

### List topics
`bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092`

### Create a producer
`bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic NEW_TOPIC`

### Create a consumer
- Read messages from the first offset created

`bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic NEW_TOPIC --from-beginning`

- Read all messages after the consumer is up

`bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic NEW_TOPIC`

### Alter a topic to add more partitions to it 
`bin\windows\kafka-topics.bat --alter --zookeeper localhost:9092 --topic ECOMMERCE_PURCHASE --partitions 3`
