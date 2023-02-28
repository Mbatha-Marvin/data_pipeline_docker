import os
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
from airflow.decorators import task
from pyspark.pandas.config import set_option, reset_option
import pyspark.pandas as ps
import numpy as np
import psycopg2
import pandas as pd


# -------------------------Load and Extract function----------------------------------------------------------
@task()
def load_and_transform(filelocation : str) -> int:
    print("\n-----------------------loading data---------------------")
    print(f"\nloading data from {filelocation}")
    # loading the data from csv file 
    vehicle_dataframe = (ps.read_csv(filelocation, 
                        usecols=[
                            # "id", "region",
                            "manufacturer", "model", "year",
                            "price", 
                            # "condition", "fuel", "transmission",  'drive', 'size', 'type' ,'paint_color'
                        ])).dropna().reset_index(drop= True)


    print("\n----------------------loaded successfully---------------------")
    
    # ------------------------manufacturers cleaning ------------------------------------------------------
    # creating lists of accepted models
    man_list =['ford', 'chevrolet', 'toyota', 'honda', 'nissan', 'jeep', 'ram', 'gmc', 'bmw', 'dodge', 'mercedes-benz', 'hyundai', 'subaru', 
        'volkswagen', 'kia', 'lexus', 'audi', 'cadillac', 'chrysler', 'acura', 'buick', 'mazda', 'infiniti', 'lincoln', 'volvo', 'mitsubishi', 
        'mini', 'pontiac', 'jaguar', 'rover', 'porsche', 'mercury', 'saturn', 'alfa-romeo', 'tesla', 'fiat', 'harley-davidson', 'ferrari', 'dc', ' E150 ', 
        ' Nissan Silverado ', 'datsun', 'aston-martin', 'land rover']

    models_list = [
    # ford_models
        "f-150", "f150", "f250", "f-250", "f350", "f-350", "f450", "f-450", "f550", "f-550", "c-max", "bronco", "edge", "escape", "explorer", "expedition", "ranger", 
        "fiesta", "thunderbird", "transit", "mustang", "taurus", "fusion", "focus", "flex","econoline",  
    # chevrolet
        "colorado", "silverado", "hhr", "impala", "corvette", "volt", "express", "cruze", "tahoe", "aveo", "trax", "suburban", "malibu",  "equinox", "spark", "sonic",
        "blazer", "camaro", "traverse", "cobalt", "avalanche","trailblazer", 
    # toyota
        'camry', 'tacoma', 'corolla', 'rav4', 'prius', 'tundra', 'sienna', '4runner', 'highlander', 'avalon', 'fj cruiser', 'sequoia', 'yaris', 'venza', 'land cruiser', 'matrix'
    # honda
        'accord', 'civic', 'cr-v', 'odyssey', 'pilot', 'fit', 'crv', 'ridgeline', 'element', 'hr-v' 
    #  nissan
        'altima', 'rogue', 'sentra', 'maxima', 'pathfinder', 'versa', 'murano', 'frontier', 'titan', 'xterra', 'armada', 'nismo',  'juke', 'quest', 'leaf', 'nv200',
    #  jeep
        'wrangler', 'grand cherokee', 'cherokee', 'liberty', 'patriot', 'compass', 'renegade', 'gladiator','commander' 
    # ram
        '1500', '2500', '3500', '5500'
    # gmc
        'sierra', 'acadia', 'yukon', 'terrain', 'envoy', 'canyon', 'savana'
    # bmw
        '3 series', 'x5', '5 series', '328i', 'x3', 'x1', '528i', '4 series', '535i', '3-series', '335i', '128i', '7 series', '325i', '328xi', '6 series', '320i', 'm3', 'x6',
    # dodge
        'charger', 'grand caravan', 'challenger', 'durango', 'journey', 'dart', 'dakota', 'avenger', 'caliber', 'nitro', 'caravan', 
    # mercedes-benz
        'c-class', 'e-class', 's-class', 'm-class', 'gl-class', 'benz', 'benz e350', 'benz c300', 'gla', 'glc', 'glk', 'cla-class', 'cls', 'sl-class'
    # hyundai
        'sonata', 'elantra', 'santa fe', 'tucson', 'accent', 'veloster', 'genesis', 'kona'
    # subaru
        'outback', 'forester', 'impreza', 'legacy', 'wrx', 'crosstrek'
    # volkswagen
        'jetta', 'passat', 'tiguan', 'beetle', 'gti',  'cc',  'golf', 'touareg', 'atlas', 'eos komfort',
        ]

    # enabled to allow creation of new dataframe columns

    set_option("compute.ops_on_diff_frames", True)

    # vectorizing the manufacture
    condition = vehicle_dataframe.manufacturer.isin(man_list)
    vehicle_dataframe = vehicle_dataframe[condition]
    # print(f"\n{vehicle_dataframe.count(axis=0)}")

    def car_models(df):
        # print(df)
        my_list = df.to_list()
        # print(my_list[0])
        func = lambda list_, string: tuple(filter(lambda x: x in string, list_))
        model = list(func(models_list, my_list[0]))
        if len(model) == 1 :
            # print(model[0])
            return model[0]

    vehicle_dataframe["model_clean"] = vehicle_dataframe[["model"]].apply(car_models, axis=1)
    
    manufacturer_models = vehicle_dataframe.to_pandas()
    manufacturer_models = manufacturer_models.dropna()
    # print(f"\n{manufacturer_models.head(20)}")
    # print(f"\n{manufacturer_models.manufacturer.unique()}")

    models_dictionary = (
    {
    "ford" :
        ["f-150", "f150", "f250", "f-250", "f350", "f-350", "f450", "f-450", "f550", "f-550", "c-max", "bronco", "edge", "escape", "explorer", "expedition", "ranger", 
            "fiesta", "thunderbird", "transit", "mustang", "taurus", "fusion", "focus", "flex","econoline"] ,
        
    "chevrolet" :
    [ "colorado", "silverado", "hhr", "impala", "corvette", "volt", "express", "cruze", "tahoe", "aveo", "trax", "suburban", "malibu",  "equinox", "spark", "sonic",
        "blazer", "camaro", "traverse", "cobalt", "avalanche","trailblazer"] ,
        
    "toyota" :
        ['camry', 'tacoma', 'corolla', 'rav4', 'prius', 'tundra', 'sienna', '4runner', 'highlander', 
            'avalon', 'fj cruiser', 'sequoia', 'yaris', 'venza', 'land cruiser', 'matrix'] ,
        
    "honda" :
        ['accord', 'civic', 'cr-v', 'odyssey', 'pilot', 'fit', 'crv', 'ridgeline', 'element', 'hr-v'] ,
        
    "nissan" :
        ['altima', 'rogue', 'sentra', 'maxima', 'pathfinder', 'versa', 'murano', 'frontier', 'titan', 
            'xterra', 'armada', 'nismo',  'juke', 'quest', 'leaf', 'nv200'] ,
        
    "jeep" :
        ['wrangler', 'grand cherokee', 'cherokee', 'liberty', 'patriot', 'compass', 'renegade', 'gladiator',
            'commander'] ,
        
    "ram" :
        ['1500', '2500', '3500', '5500'] ,
        
    "gmc" : 
        ['sierra', 'acadia', 'yukon', 'terrain', 'envoy', 'canyon', 'savana'] ,
        
    "bmw" :
        ['3 series', 'x5', '5 series', '328i', 'x3', 'x1', '528i', '4 series', '535i', '3-series', '335i', 
            '128i', '7 series', '325i', '328xi', '6 series', '320i', 'm3', 'x6'] ,
        
    "dodge" :
        ['charger', 'grand caravan', 'challenger', 'durango', 'journey', 'dart', 'dakota', 'avenger', 'caliber',
            'nitro', 'caravan'] ,
        
    "mercedes-benz":
        ['c-class', 'e-class', 's-class', 'm-class', 'gl-class', 'benz', 'benz e350', 'benz c300', 
            'gla', 'glc', 'glk', 'cla-class', 'cls', 'sl-class'] ,
        
    "hyundai" :
            ['sonata', 'elantra', 'santa fe', 'tucson', 'accent', 'veloster', 'genesis', 'kona'] ,
        
    "subaru" :
            ['outback', 'forester', 'impreza', 'legacy', 'wrx', 'crosstrek'] ,
        
    "volkswagen" :
            ['jetta', 'passat', 'tiguan', 'beetle', 'gti',  'cc',  'golf', 'touareg', 'atlas', 'eos komfort']
        
    })

    manufacturer_selection_logic = [
    (manufacturer_models.model_clean.isin(models_dictionary["bmw"])),
    (manufacturer_models.model_clean.isin(models_dictionary["chevrolet"])),
    (manufacturer_models.model_clean.isin(models_dictionary["dodge"])),
    (manufacturer_models.model_clean.isin(models_dictionary["ford"])),
    (manufacturer_models.model_clean.isin(models_dictionary["gmc"])),
    (manufacturer_models.model_clean.isin(models_dictionary["honda"])),
    (manufacturer_models.model_clean.isin(models_dictionary["hyundai"])),
    (manufacturer_models.model_clean.isin(models_dictionary["jeep"])),
    (manufacturer_models.model_clean.isin(models_dictionary["mercedes-benz"])),
    (manufacturer_models.model_clean.isin(models_dictionary["nissan"])),
    (manufacturer_models.model_clean.isin(models_dictionary["ram"])),
    (manufacturer_models.model_clean.isin(models_dictionary["subaru"])),
    (manufacturer_models.model_clean.isin(models_dictionary["toyota"])),
    (manufacturer_models.model_clean.isin(models_dictionary["volkswagen"]))
    ]
    manufacturer_selection_labels = sorted(list(models_dictionary.keys()))


    manufacturer_models["manufacturer_clean"] =  np.select(manufacturer_selection_logic, manufacturer_selection_labels)
    manufacturer_models = manufacturer_models.drop(["model", "manufacturer"], axis=1)
    manufacturer_models = manufacturer_models.rename(columns = {"model_clean" : "model", "manufacturer_clean" : "manufacturer"})
    manufacturer_models = manufacturer_models.astype({'price':'int'})
    manufacturer_models = manufacturer_models[(manufacturer_models["price"] > 0) & (manufacturer_models['year'] != ' Chevrolet ')].reset_index(drop = True)
    manufacturer_models["year"] = pd.to_datetime(manufacturer_models.year, format='%Y')
    manufacturer_models = manufacturer_models.astype({'year':'str'})
    manufacturer_models = manufacturer_models[["manufacturer", "model", "year" , "price"]]
    print(f"\n{manufacturer_models.head(10)}")

    # print(f"\n{manufacturer_models.head(10)}")
    print(f"\n{manufacturer_models.year.unique()}")

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

    cursor.execute("DROP TABLE IF EXISTS vehicle_cleaned")
    cursor.execute("DROP TABLE IF EXISTS model_count")

    create_table = """CREATE TABLE vehicle_cleaned (
        id bigint PRIMARY KEY,
        manufacturer VARCHAR(255) NOT NULL,
        model VARCHAR(255) NOT NULL,
        year date NOT NULL,
        price bigint NOT NULL
    )
    """
    cursor.execute(create_table)
    print("***********Table created successfully........")
    
    print("*****************changes commited*********************")

    values = manufacturer_models.to_records().tolist()
    print(f"\n{len(values)}")
    print(f"\n{values[:5]}")

    print(f"\nData Entry has begun***************")

    # executing the sql statement
    cursor.executemany("INSERT INTO vehicle_cleaned VALUES(%s,%s,%s,%s,%s)", values)

    conn.commit()
    conn.close()

    # manufacturer_models_2 = manufacturer_models[manufacturer_models["count"] >= manufacturer_models["count"].mean()].reset_index(drop = True)


    return "success"