# Immigration Data Process
### Data Engineering Capstone Project

### Project Overview
This is a Udacity Data Engineering Capstone project to showcase all the learning & skills that been acquired during the course of the nano-degree program. This is an open-ended project and for this udacity has provided four datasets that includes US immigration 2016 data, airport codes, temperature and US demographic data.

My job here was to analyze and explore the source dateset and upload it to a database where Data Analyst can prepare reports from it.

### Scope the Project and Gather Data

#### Scope
This projects aims to enrich the US I94 immigration data with further data such as demographics and temperature data to have a wider basis for analysis on the immigration data.

#### Describe and Gather Data

<table>
<thead>
<tr>
<th>Source name</th>
<th>Filename</th>
<th>Format</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>I94 Immigration Sample Data</td>
<td>immigration_data_sample.csv</td>
<td>csv</td>
<td>This is a sample data which is from the US National Tourism and Trade Office.</td>
</tr>
<tr>
<td><a href="https://travel.trade.gov/research/reports/i94/historical/2016.html">I94 Immigration Data</a></td>
<td>../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat</td>
<td>SAS</td>
<td>This data comes from the US National Tourism and Trade Office.</td>
</tr>
<tr>
<td><a href="https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data">World Temperature Data</a></td>
<td>world_temperature.csv</td>
<td>csv</td>
<td>This dataset contains temperature data of various cities from 1700&#39;s - 2013. This dataset came from Kaggle.</td>
</tr>
<tr>
<td><a href="https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/">U.S. City Demographic Data</a></td>
<td>us-cities-demographics.csv</td>
<td>csv</td>
<td>This dataset contains population details of all US Cities and census-designated places includes gender &amp; race informatoin. This data came from OpenSoft.</td>
</tr>
<tr>
<td><a href="https://datahub.io/core/airport-codes#data">Airport Codes</a></td>
<td>airport-codes_csv.csv</td>
<td>csv</td>
<td>This is a simple table of airport codes and corresponding cities.</td>
</tr>
<tr>
<td>I94 SAS Labels Descriptions</td>
<td>I94_SAS_Labels_Descriptions.SAS</td>
<td>SAS</td>
<td>Shows column description for the immigration data.</td>
</tr>
</tbody>
</table>

### Explore and Assess the Data
#### Explore the Data

##### I94 Immigration Data
Initial schema after loaded to spark dataframe
```
root
|-- cicid: double (nullable = true)
|-- i94yr: double (nullable = true)
|-- i94mon: double (nullable = true)
|-- i94cit: double (nullable = true)
|-- i94res: double (nullable = true)
|-- i94port: string (nullable = true)
|-- arrdate: double (nullable = true)
|-- i94mode: double (nullable = true)
|-- i94addr: string (nullable = true)
|-- depdate: double (nullable = true)
|-- i94bir: double (nullable = true)
|-- i94visa: double (nullable = true)
|-- count: double (nullable = true)
|-- dtadfile: string (nullable = true)
|-- visapost: string (nullable = true)
|-- occup: string (nullable = true)
|-- entdepa: string (nullable = true)
|-- entdepd: string (nullable = true)
|-- entdepu: string (nullable = true)
|-- matflag: string (nullable = true)
|-- biryear: double (nullable = true)
|-- dtaddto: string (nullable = true)
|-- gender: string (nullable = true)
|-- insnum: string (nullable = true)
|-- airline: string (nullable = true)
|-- admnum: double (nullable = true)
|-- fltno: string (nullable = true)
|-- visatype: string (nullable = true)
```

The following changes a made
- Removed rows with invalid  i94ports, i94cit and i94res values
- dropped `['i94mon', 'entdepd', 'insnum', 'entdepu', 'matflag', 'entdepa', 'count', 'i94yr']` columns because they dont have clear description or cantain too many null values
- renamed columns: `i94bir` to `age`, `i94cit` to `coc`, `i94res` to `cor`, `i94port` to `port_code`, `i94addr` to`landing_state`, `visapost` to `visa_issued_in`, `cicid` to `id`
- changed the date format on `arrdate`, `depdate`, `dtadfile`, `dtaddto`
- changed `i94mode` and `i94visa` column values from numbers to descriptive texts

##### World Temperature Data
- removed non-US data
- dropped columns: `['AverageTemperatureUncertainty', 'Latitude', 'Longitude', 'Country']`

##### U.S. City Demographic Data
- created a separate table `general` and `race` to remove redundancy

##### Airport Codes
- removed non-US data
- removed rows wit null `iata_codes`
- split `coordinates` column to separate `latitude` and `longitude` columns
- created a `state_code` column from `iso_region`
- dropped this columns: `['coordinates', 'iso_country', 'continent', 'iso_region']`

##### I94 SAS Labels Descriptions
- from this file I created the following tables: `countries.csv`, `us_ports.csv`, `us_states.csv`

### Data Model Definition
#### Conceptual Data Model
For storing the data in database I proposed the following Star Schema.
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

#### Mapping Out Data Pipelines
1. Create tables by executing `create_tables.py`.
2. Pre-process the raw source with `create_clean_fact_with_spark.ipynb` and `create_clean_dimensions_with_pandas.ipynb`
3. Insert data to database tables with the `etl.py`

<details>
  <summary>Click here to see the log of the etl.py</summary>
<pre>
INFO:root:Connection to DB: OK
INFO:root:Inserted data from: /home/workspace/dimensions/us_states.csv
INFO:root:Inserted data from: /home/workspace/dimensions/us_ports.csv
INFO:root:Inserted data from: /home/workspace/dimensions/us_airport_codes.csv
INFO:root:Inserted data from: /home/workspace/dimensions/us-cities-demographics_race.csv
INFO:root:Inserted data from: /home/workspace/dimensions/us-cities-demographics_general.csv
INFO:root:Inserted data from: /home/workspace/dimensions/countries.csv
INFO:root:Inserted data from: /home/workspace/dimensions/us_temperature.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=MS/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=MS/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=MS/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=GA/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=GA/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=GA/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=PA/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=PA/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=PA/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=KY/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=KY/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=KY/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=KS/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=KS/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=KS/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=RI/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=RI/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=RI/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=MD/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=MD/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=MD/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=CA/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=CA/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=CA/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=GU/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=GU/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=GU/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=HI/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=HI/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=HI/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=NM/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=NM/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=NM/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=ND/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=ND/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=ND/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=IL/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=IL/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=IL/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=IN/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=IN/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=IN/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=WA/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=WA/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=WA/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=NJ/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=NJ/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=NJ/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=DC/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=DC/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=DC/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=WY/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=WY/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=WY/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=MT/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=MT/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=MT/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=NE/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=NE/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=NE/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=SC/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=SC/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=SC/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=NY/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=NY/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=NY/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=CT/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=CT/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=CT/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=VT/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=VT/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=VT/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=OK/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=OK/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=OK/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=TN/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=TN/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=TN/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=OH/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=OH/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=OH/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=NH/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=NH/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=NH/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=AK/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=AK/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=AK/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=AR/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=AR/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=AR/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=FL/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=FL/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=FL/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=NC/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=NC/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=NC/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=AZ/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=AZ/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=AZ/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=MI/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=MI/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=MI/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=OR/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=OR/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=OR/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=NV/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=NV/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=NV/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=ME/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=ME/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=ME/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=DE/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=DE/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=DE/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=MN/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=MN/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=MN/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=LA/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=LA/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=LA/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=VI/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=VI/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=IA/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=IA/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=IA/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=MA/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=MA/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=MA/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=CO/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=CO/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=CO/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=TX/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=TX/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=TX/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=WI/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=WI/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=WI/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=PR/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=PR/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=PR/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=MO/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=MO/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=MO/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=WV/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=WV/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=WV/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=VA/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=VA/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=VA/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=ID/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=ID/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=ID/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=SD/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=SD/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=SD/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=UT/part-00051-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=UT/part-00174-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:Inserted data from: /home/workspace/fact/landing_state=UT/part-00043-a79b467e-15b7-4976-b1d4-aab5af10fbbe.c000.csv
INFO:root:All Done!
</pre>
</details>

### Complete Project Write Up
#### Choice of tools and technologies
I used the `pandas` framework the preprocess small tables. On the huge immigration table I preferred to use `spark` as a distrubuted computation framework.

My choice of database was the postgreSQL because it can handle more lazy queries than for example a noSQL Cassandra.

### Future stages
#### If the data was increased by 100x.
Use Spark to process the data (not just the main table) efficiently in a distributed way e.g. with EMR. In case we recognize that we need a write-heavy operation, I would suggest using a Cassandra database instead of PostgreSQL.

#### If the pipelines were run on a daily basis by 7am.
Use Airflow and create a DAG that performs the logic of the described pipeline. If executing the DAG fails, I recommend to automatically send emails to the engineering team using Airflow's builtin feature, so they can fix potential issues soon.

#### If the database needed to be accessed by 100+ people.
Use RedShift to have the data stored in a way that it can efficiently be accessed by many people. 
