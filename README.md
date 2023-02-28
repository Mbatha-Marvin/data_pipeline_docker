# spark-airflow

The pipeline submits a spark application to a standalone spark cluster which creates an output on HDFS

### Docker Setup
- Airflow
- Postgres
- Spark Standalone: Master with 2 worker nodes
- Hadoop: NameNode and DataNode

| App| Version |
| --- | --- |
| Airflow | 2.1.2 |
| Spark/PySpark | 3.1.1 |
| Hadoop | 3.2.1 |

### Steps:
- Clone repository
- Run: `cd docker`
- Start services: `docker-compose up`
- Login Airflow on http://localhost:8080
  - user: airflow
  - password: airflow

### WebUI:
- Namenode: http://localhost:9870
- Spark Standalone: http://localhost:4040



To rubuild the image after updating docker files run
  - docker build -t  docker-webserver .


