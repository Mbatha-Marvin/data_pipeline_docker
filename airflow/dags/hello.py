from airflow.decorators import dag, task
import datetime
import os


@dag(
    dag_id="Hello_airflow",
    description="A Dag that simply says hello",
    schedule=None,
    start_date= datetime.datetime(2022, 12, 29),
    catchup=False
)
def helloflow():
    
    @task()
    def getDate() -> str:
        today = datetime.date.today()


        return str(today)

    @task()
    def getName() -> str:
        return "Mbatha Marvin"

    @task()
    def sayHello(date :dict, name : str):

        print(f"Hello {name} today is {date}")

    @task()
    def get_directory():
        print("---------------------------------------------")
        print(os.getcwd())
        print(os.listdir(os.getcwd()))
        print("---------------------------------------------")

    today = getDate()
    name = getName()

    sayHello(today, name) >> get_directory()

helloflow()

    