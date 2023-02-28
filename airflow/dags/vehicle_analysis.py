# importing libraries
import os
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
import sys
sys.path.append("/usr/local/spark/app/")
from loadTransform import load_and_transform
from databaseLoad import load_to_database
import pandas as pd
import plotly.express as px
import numpy as np
from airflow.decorators import dag 
import datetime






@dag(
    dag_id="Vehicle_Data_Analysis",
    schedule=None,
    start_date= datetime.datetime(2022, 12, 31),
    catchup=False
)
def vehicle_analysis_flow():
        

# --------------------------definition of the flow----------------------------------------------

    file_location = os.getcwd() + "/bigData/vehicles.csv"

    models_dictionary = load_and_transform(file_location)
    # model_value_1 = models_dictionary.get("model_count_data")
    load_to_database(models_dictionary)

# ------------------------------------initializing flow------------------------------------------------

vehicle_analysis_flow()