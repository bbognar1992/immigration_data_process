# Immigration Data Process
### Data Engineering Capstone Project

#### Project Summary

This projects aims to enrich the US I94 immigration data with further data such as demographics and temperature data to have a wider basis for analysis on the immigration data.

### Dataset

##### I94 Immigration Data
This data comes from the US National Tourism and Trade Office. [This](https://travel.trade.gov/research/reports/i94/historical/2016.html) is where the data comes from.

##### World Temperature Data
This dataset came from Kaggle. You can read more about it [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).

##### U.S. City Demographic Data
This data comes from OpenSoft. You can read more about it [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).

##### Airport Code Table
This is a simple table of airport codes and corresponding cities. It comes from [here](https://datahub.io/core/airport-codes#data).

### Conceptual Data Model
##### Dimension Tables
```
dim_us_airports
    id
    type
    name
    elevation_ft
    municipality
    gps_code
    iata_code
    local_code
    latitude
    longitude
    state_code

dim_demographics_general
    city
    state_code
    median_age
    male_population
    female_population
    total_population
    number_of_veterans
    foreign_born
    average_household_size

dim_demographics_race
    city
    state_code
    race
    count
    
dim_us_temperatures
    dt
    avg_temp
    city
 
dim_countries
    country_code
    country_name

dim_us_ports
    municipality
    port_code
    state_code
    
dim_us_states
    state_code
    state_name
```

##### Fact Table
```
fact_immigrations
    id
    coc
    cor
    port_code
    age
    visa_issued_in
    occup
    biryear
    gender
    airline
    admnum
    fltno
    visatype
    arrival_dt
    departure_dt
    added_to_i94
    allowed_until
    arrival_mode
    visit_purpose
```

### Mapping Out Data Pipelines
1. Create tables by executing `create_tables.py`.
2. Pre-process the raw source with `create_clean_fact_with_spark.ipynb` and `create_clean_dimensions_with_pandas.ipynb`
3. Insert data to database tables with the `import.py`
