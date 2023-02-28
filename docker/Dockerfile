FROM apache/airflow:slim-latest-python3.10

USER root

# Postgres
RUN apt-get update \
  && apt-get -y install libaio-dev \
  && apt-get install postgresql-client

# OpenJdk for spark-submit
RUN mkdir -p /usr/share/man/man1 /usr/share/man/man2
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow
# PySpark
RUN pip install pyspark
RUN pip install apache-airflow-providers-apache-spark
RUN pip install plotly scipy scikit-learn polars
RUN pip install psycopg2-binary
RUN pip install SQLAlchemy
RUN pip install pandas
RUN pip install pyarrow

RUN mkdir extra
COPY airflow/init.sh ./init.sh
COPY airflow/airflow.cfg ./airflow.cfg
COPY airflow/setup_connection.py ./setup_connection.py

USER root
RUN chmod +x ./init.sh
# EXPOSE 8081

USER airflow
EXPOSE 3000 4040 5433 5434 7077 8020 8079 8080 8087 8090 8091 8092 9870

ENTRYPOINT ["./init.sh"]

# -----------------------------------------------------------------------

