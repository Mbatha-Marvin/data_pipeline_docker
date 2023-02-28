import psycopg2
from airflow.decorators import task


# ----------------connect to postgres database-------------------------------
@task()
def load_to_database(model_count_json :str):
    print("*****************************************")
    print(type(model_count_json))
    print(f"{model_count_json}")
    # print(f"{model_count_json['model_count_data']}")
    print("-------------------Connecting to postgres database----------------------------")
    conn = psycopg2.connect(
        host = "processed_postgres",
        user = "metabase_processed",
        password = 5713,
        database = "metabase_processed",
        # port = "5434"
    )

    cursor = conn.cursor()
    cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
    print(cursor.fetchall())

    sql = """select * from vehicle_cleaned"""
    cursor.execute(sql)
    print(cursor.fetchmany(50))

    conn.close()
    print("-----------------Database Connection closed----------------")





    # print(cursor)
    # print("-------------------Connected to postgres database----------------------------")


