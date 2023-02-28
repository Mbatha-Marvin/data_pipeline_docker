from airflow import settings
from airflow.models import Connection

if __name__ == "__main__":
    spark_standalone_conn_id = "spark_standalone"
    spark_master = "spark://spark-master:7077"

    session = settings.Session
    conn = Connection(conn_id=spark_standalone_conn_id, conn_type="spark", host=spark_master)
    conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
    if not str(conn_name) == str(spark_standalone_conn_id):
        session.add(conn)
        session.commit()
