create_immigrations = """
CREATE TABLE "fact_immigrations"
  (
     "id"             INTEGER PRIMARY KEY NOT NULL,
     "coc"            INTEGER,
     "cor"            INTEGER,
     "port_code"      VARCHAR(3),
     "age"            INTEGER,
     "visa_issued_in" VARCHAR(10),
     "occup"          VARCHAR(30),
     "biryear"        INTEGER,
     "gender"         VARCHAR(1),
     "airline"        VARCHAR(30),
     "admnum"         INTEGER,
     "fltno"          VARCHAR(30),
     "visatype"       VARCHAR(5),
     "arrival_dt"     VARCHAR(50),
     "departure_dt"   VARCHAR(50),
     "added_to_i94"   VARCHAR(50),
     "allowed_until"  VARCHAR(50),
     "arrival_mode"   VARCHAR(15),
     "visit_purpose"  VARCHAR(25),
    CONSTRAINT fk_port_code FOREIGN KEY(port_code) REFERENCES dim_us_ports(port_code)
  );
"""

create_airports = """
CREATE TABLE "dim_us_airports"
  (
     "id"           VARCHAR(10) PRIMARY KEY NOT NULL,
     "type"         VARCHAR(50),
     "name"         VARCHAR(255),
     "elevation_ft" REAL,
     "municipality" VARCHAR(255),
     "gps_code"     VARCHAR(255),
     "iata_code"    VARCHAR(3),
     "local_code"   VARCHAR(255),
     "latitude"     REAL,
     "longitude"    REAL,
     "state_code"   VARCHAR(2),
     CONSTRAINT fk_state_code FOREIGN KEY(state_code) REFERENCES dim_us_states(state_code)
  );
"""

create_demographics_general = """
CREATE TABLE "dim_demographics_general"
  (
     "city"                   VARCHAR(255),
     "state_code"             VARCHAR(2),
     "median_age"             REAL,
     "male_population"        REAL,
     "female_population"      REAL,
     "total_population"       INTEGER,
     "number_of_veterans"     REAL,
     "foreign_born"           REAL,
     "average_household_size" REAL,
     PRIMARY KEY (city, state_code)
  );
"""

create_demographics_race = """
CREATE TABLE "dim_demographics_race"
  (
     "city"                   VARCHAR(255),
     "state_code"             VARCHAR(2),
     "race"                   VARCHAR(255),
     "count"                  INTEGER,
      PRIMARY KEY (city, state_code, race)
  );
"""

create_temperature = """
CREATE TABLE "dim_us_temperatures"
  (
     "dt"                            DATE,
     "avg_temp"                      REAL,
     "city"                          VARCHAR(255),
     PRIMARY KEY (dt, city)
  );
"""

create_countries = """
CREATE TABLE "dim_countries"
  (
     "country_code"                  INTEGER PRIMARY KEY NOT NULL, 
     "country_name"                  VARCHAR(255)
  );
"""

create_us_ports = """
CREATE TABLE "dim_us_ports"
  (
     "municipality"               VARCHAR(255),
     "port_code"                  VARCHAR(3) PRIMARY KEY NOT NULL,
     "state_code"                 VARCHAR(2)
  );
"""

create_us_states = """
CREATE TABLE "dim_us_states"
  (
     "state_code"                  VARCHAR(2) PRIMARY KEY NOT NULL,
     "state_name"                  VARCHAR(255)
  );
"""

copy_sql = """
COPY {table} 
FROM '{source}' delimiter ',' csv header"""

drop_us_airports = """
DROP TABLE IF EXISTS "dim_us_airports";
"""

drop_demographics_general = """
DROP TABLE IF EXISTS "dim_demographics_general";
"""

drop_demographics_race = """
DROP TABLE IF EXISTS "dim_demographics_race";
"""

drop_immigrations = """
DROP TABLE IF EXISTS "fact_immigrations";
"""

drop_temperature = """
DROP TABLE IF EXISTS "dim_us_temperatures";
"""

drop_countries = """
DROP TABLE IF EXISTS "dim_countries";
"""

drop_us_ports = """
DROP TABLE IF EXISTS "dim_us_ports";
"""

drop_us_states = """
DROP TABLE IF EXISTS "dim_us_states";
"""

drop_table_queries = [
    drop_us_airports,
    drop_demographics_general, drop_demographics_race,
    drop_immigrations,
    drop_temperature,
    drop_countries,
    drop_us_ports,
    drop_us_states
]

create_table_queries = [
    create_us_states,
    create_us_ports,
    create_airports,
    create_demographics_general,create_demographics_race,
    create_immigrations,
    create_temperature,
    create_countries]
