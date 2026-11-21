# Cluster Hadoop – Spark – Hive avec Docker (TP RSI3, ISET Nabeul)

Cluster conteneurisé sur une VM Ubuntu 22.04 :
- HDFS : 1 NameNode + 4 DataNodes (Hadoop 3.2.1)
- Spark Standalone 3.5.6 : 1 master + 1 worker
- Hive 3.1.3 : Metastore (PostgreSQL 13) + HiveServer2

## Démarrage
```bash
mkdir -p hadoop/data/{namenode,datanode1,datanode2,datanode3,datanode4} hive/pgdata
curl -L -o hive/postgresql.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar
docker compose up -d
```

Interfaces : HDFS http://localhost:9870 — Spark http://localhost:8080 — HiveServer2 : port 10000.

Les mots de passe de `compose.yaml` sont ceux d'un laboratoire isolé et ne conviennent pas à la production.
