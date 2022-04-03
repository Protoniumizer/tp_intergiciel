# Team

FOSSEY Mathis

WATIER Julie


## LAUNCH SERVER KAFKA/ZOOKEEPER

On WINDOWS (Use two terminals, one for zookeeper, one for kafka)
```bash
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
.\bin\windows\kafka-server-start.bat .\config\server.properties 
```
On LINUX (Use two terminals, one for zookeeper, one for kafka)
```bash
./bin/zookeeper-server-start.sh ./config/zookeeper.properties
./bin/kafka-server-start.sh ./config/server.properties 
```

## CREATE TOPIC
On WINDOWS

```bash
.\bin\windows\kafka-topics.bat --create --topic Topic1 --bootstrap-server localhost:9092
.\bin\windows\kafka-topics.bat --create --topic Topic2 --bootstrap-server localhost:9092
.\bin\windows\kafka-topics.bat --create --topic Topic3 --bootstrap-server localhost:9092
```
On LINUX 
```bash
./bin/kafka-topics.sh --create --topic Topic1 --bootstrap-server localhost:9092
./bin/kafka-topics.sh --create --topic Topic2 --bootstrap-server localhost:9092
./bin/kafka-topics.sh --create --topic Topic3 --bootstrap-server localhost:9092
```

## CREATE DATABASE (ON POSTGRESQL)

```SQL
-- SCHEMA: covid19

-- DROP SCHEMA IF EXISTS covid19 ;

CREATE SCHEMA IF NOT EXISTS covid19
    AUTHORIZATION "Covid19";
```

```SQL
-- Table: covid19.global

-- DROP TABLE IF EXISTS covid19.global;

CREATE TABLE IF NOT EXISTS covid19.global
(
    covid19_id integer NOT NULL DEFAULT nextval('covid19.global_covid19_id_seq'::regclass),
    data jsonb,
    CONSTRAINT global_pkey PRIMARY KEY (covid19_id)
)
```

## USE

Execute previous command

Open an IDE (IntelliJ was used to code)

Launch the different class
- Pr1 to call the API
- Cs1 to insert data in database.
- Cs3 and Pr2 to process order.
- Pr2 and Cs3 to make requests with the console.

To make queries, you must use the console with these different commands.
- Get_global_values
    - returns global values of covid.
- Get_country_values v_pays 
    - returns country values for country "v_pays", where v_pays is the country code of the requested country (e.g. : France = FR)
- Get_confirmed_avg
    - returns an average of confirmed covid cases
- Get_deaths_avg
    - returns an average of covid deaths
- Get_countries_deaths_percent
    - returns the percentage of Deaths compared to confirmed cases
- Export
    - exports data from database in XML file
