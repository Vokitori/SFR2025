1. start docker desktop

2. docker compose up -d

3. docker exec --workdir /opt/kafka/bin/ -it broker-1 sh 

4. ./kafka-topics.sh --bootstrap-server broker-1:19092,broker-2:19092,broker-3:19092 --create --topic research-paper --partitions 3 --replication-factor 3

5. ./kafka-configs.sh --bootstrap-server broker-1:19092,broker-2:19092,broker-3:19092 --entity-type topics --entity-name research-paper --alter --add-config min.insync.replicas=2

6. ./kafka-topics.sh --bootstrap-server broker-1:19092,broker-2:19092,broker-3:19092 --create --topic PROCESSED_PAPERS --partitions 3 -replication-factor 3

start consumer inside container using : ./kafka-console-consumer.sh --bootstrap-server broker-1:19092,broker-2:19092,broker-3:19092 --topic research-paper --from-beginning

