import os
import psycopg2
from sql_queries import copy_sql
import logging

dir_path = os.getcwd()+'/preprocessed'

dimensions = [
    {"source": "dimensions/us_states.csv", "table": "dim_us_states"},
    {"source": "dimensions/us_ports.csv", "table": "dim_us_ports"},
    {"source": "dimensions/us_airport_codes.csv", "table": "dim_us_airports"},
    {"source": "dimensions/us-cities-demographics_race.csv", "table": "dim_demographics_race"},
    {"source": "dimensions/us-cities-demographics_general.csv", "table": "dim_demographics_general"},
    {"source": "dimensions/countries.csv", "table": "dim_countries"},
    {"source": "dimensions/us_temperature.csv", "table":"dim_us_temperatures"}
]


conn = psycopg2.connect("host=127.0.0.1 dbname=imigrationsdb user=student password=student")
cur = conn.cursor()

logging.basicConfig(filename='log.text', level=logging.INFO)

logging.info("Connection to DB: OK")

for dim in dimensions:
    dim['source']
    formatted_copy_sql= copy_sql.format(table=dim['table'], source=f"{dir_path}/{dim['source']}")  
    cur.execute(formatted_copy_sql)
    logging.info(f"Inserted data from: {dir_path}/{dim['source']}")
    
files=[]
for r, d, f in os.walk("fact/"):
    for file in f:
        if ".ipynb_checkpoints" not in r and file.endswith('csv'):
            files.append(os.path.join(r, file))

for file in files:
    formatted_copy_sql= copy_sql.format(table='fact_immigrations', source=f"{dir_path}/{file}")  
    cur.execute(formatted_copy_sql)
    logging.info(f"Inserted data from: {dir_path}/{file}")
    
conn.commit()
logging.info("All Done!")
conn.close()