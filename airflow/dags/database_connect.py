from airflow.decorators import dag, task
import datetime
import psycopg2

@dag(
    dag_id="Database_connection",
    schedule= None,
    start_date= datetime.datetime(2022, 12, 31),
    catchup=False 
)
def database_connection_tests_flow():

    @task()
    def airflow_database_conn():

        conn = psycopg2.connect(
            host = "postgres",
            user = "airflow",
            password = "airflow",
            database = "airflow",
        )

        cursor = conn.cursor()
        print("**********Successfully connected to Airflow metadata database********")
        print(cursor)
        print("***************Fetching all tables available in the database*****************")
        cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
        print(cursor.fetchall())

        conn.close()
        print("-----------------Database Connection closed----------------")

    @task()
    def metabase_database_conn():

        conn = psycopg2.connect(
            host = "metabase_postgres",
            user = "metabase",
            password = 5713,
            database = "metabase",
            # port = "5433"
        )

        cursor = conn.cursor()
        print("**********Successfully connected to Defaul MetaBase application database********")
        print(cursor)

        print("***************Fetching all tables available in the database*****************")
        cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
        print(cursor.fetchall())

        conn.close()
        print("-----------------Database Connection closed----------------")
    
    @task()
    def processed_database_conn():

        conn = psycopg2.connect(
            host = "processed_postgres",
            user = "metabase_processed",
            password = 5713,
            database = "metabase_processed",
            # port = "5434"
        )

        cursor = conn.cursor()
        print("**********Successfully connected to Visualizations database********")
        print(cursor)

        print("***************Fetching all tables available in the database*****************")
        cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
        print(cursor.fetchall())

        conn.close()
        print("-----------------Database Connection closed----------------")

    airflow_database_conn() >> metabase_database_conn() >> processed_database_conn()

database_connection_tests_flow()
