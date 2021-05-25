# Kafka Example

Setup

Run

```bash
docker-compose up -d
```

Then run with

```bash
cargo run
```

You can see a list of useful kafka scripts with

```
docker-compose exec -w /opt/bitnami/kafka/bin kafka ls
```

For example

```
docker-compose exec -w /opt/bitnami/kafka/bin kafka ./kafka-topics.sh --list --zookeeper zookeeper:2181
```