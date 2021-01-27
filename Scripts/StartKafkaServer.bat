set kafkaRootPath=D:\Repos\KafkaServer

start %kafkaRootPath%\bin\windows\zookeeper-server-start.bat %kafkaRootPath%\config\zookeeper.properties
TIMEOUT /T 10
start %kafkaRootPath%\bin\windows\kafka-server-start.bat %kafkaRootPath%\config\server.properties
