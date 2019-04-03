# World Bank Data on Cities
### Batch ETL with HBase, Phoenix, and Cassandra

First commit is a streaming demo; data is read from csv file, transformed, and sent to Cassandra.
Final version is a batch demo; data is read from HBase with Phoenix layer, transformed, and sent to Cassandra.
The goal was to calculate a population by country.
Resulting numbers are not correct - given that initial data varies in census year, and that some entries are missing - but the whole data pipeline should be understandable and easily reusable.
Upper window in Terminator terminal is Phoenix view, lower left window is Hbase shell, and lower right windows is Cassandra shell.
The data in Cassandra corresponds with direct SQL query made to Phoenix through Squirrel.

### Dataset

Original dataset is downloaded from World Bank Data Catalog.  
[Cities In Europe And Central Asia : A Shifting Story Of Urban Growth And Decline Database](https://datacatalog.worldbank.org/dataset/cities-europe-and-central-asia-shifting-story-urban-growth-and-decline-dataset)

I deleted most of the columns from original data set to speed up the setup.
Modified file 'world_bank_cities.csv' is located in /src/main/resources/.
The whole ETL process is not precise, since different cities had census in different years, and additionally some data is missing.
This version still has header names, which should be deleted before importing to Phoenix.
Command to import data with Phoenix:  

    sudo ./psql.py -t CITIES localhost /home/matko/Desktop/cities_no_header.csv

Apache Phoenix DDL:

     CREATE TABLE cities (
        id varchar(50) not null,
        city varchar(50),
        region varchar(50), 
        country varchar(50),
        lat double,
        lng double,
        year bigint,
        census bigint
        CONSTRAINT pk PRIMARY KEY (id));

Cassandra DDL:

    CREATE KEYSPACE IF NOT EXISTS counting
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    CREATE TABLE IF NOT EXISTS counting.cities (
        country text,
        count bigint,
        PRIMARY KEY(country)
        );
        
### Tools

[Flink 1.7.2](https://flink.apache.org/)  
[HBase 1.4.9](https://hbase.apache.org/)  
[Phoenix 4.14.1](https://phoenix.apache.org/)  
[Cassandra 3.11.4](http://cassandra.apache.org/)  


### Results

![Hbase - Phoenix - Cassandra - Matko Soric](https://raw.githubusercontent.com/matkosoric/CitiesBatch/master/src/main/resources/hbase-phoenix-cassandra.png?raw=true "Hbase - Phoenix - Cassandra - Matko Soric")
  
![Squirrel - Phoenix - Matko Soric](https://raw.githubusercontent.com/matkosoric/CitiesBatch/master/src/main/resources/phoenix_squirrel.png?raw=true "Squirrel - Phoenix - Matko Soric")
