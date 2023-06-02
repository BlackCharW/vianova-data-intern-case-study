# Case study


We want to know the __countries that don't host a megapolis__


The purpose of this exercice is to evaluate your skills in Python and SQL. You'll have to fork this repository and write a program that fetch the [dataset of the population of all cities in the world](https://public.opendatasoft.com/explore/dataset/geonames-all-cities-with-a-population-1000/export/?disjunctive.cou_name_en), stores it in a database, then perform a query that will compute: what are the countries that don't host a megapoliss (a city of more than 10,000,000 inhabitants)? 

The program will save the result (country code and country name) as a tabulated separated value file, ordered by country name. 

You should answer as if you were writting production code within your team. You can imagine that the program will be run automatically every week to update the resulting data.

Please send us the link to your github repository with the answer of the exercise. 


# Workflow : Countries-without-Megapoliss

## Introduction to Workflow Orchestration
In this workflow, I created a [workflow](./data-processor/airflow/dags/data_workflow_dag.py) that downloaded a CSV and processed it so that we could ingest it to Postgres weekly.

It contains 4 steps which could be separated (downloading, ingesting, processing and output), like this:
```
(web) → DOWNLOAD → (csv) → INGEST → (Postgres) → QUERY → (result) → OUTPUT → (csv)
```
![airflow architecture](/image/image.png)

A ***Workflow Orchestration Tool*** allows us to define data workflows and parametrize them; it also provides additional tools such as history and logging.

The tool I will focus on is **[Apache Airflow](https://airflow.apache.org/)**


## Setting up Airflow with Docker-compose

### Pre-requisites
 1. Git Clone this project and open a terminal under the project home directory.
 2. [set up the Airflow user](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#setting-the-right-airflow-user) in `.env` file.
    * In all other operating systems, you may need to generate a `.env` file with the appropiate UID with the following command:
        ```bash
        echo -e "AIRFLOW_UID=$(id -u)" > .env
        ```
### Execution of Airflow

1. Build the image. It may take several minutes. It only need to do this the first time you run Airflow or if you modified the [Dockerfile](/data-processor/airflow/Dockerfile)
    ```bash
    cd ./airflow
    docker-compose build
    ```
2. Initialize configs:
    ```bash
    docker-compose up airflow-init
    ```
3. Run Airflow
    ```bash
    docker-compose up
    ```
4. It also need to create a local postgres db to save the initial data, build the image.
    ```bash
    cd ..
    docker-compose build
    ```

5. Access the Airflow GUI by browsing to `localhost:8080`. Username and password are both `airflow` .

## Ingesting data to Local Postgres with Airflow

When activated, the DAG will trigger the ingestion of cities data **every monday**.

**NOTE :**
Each downloaded csv will be named after `cities_{Y-m-d}.csv`

## Processing data to calculate countries that don't host a megapoliss

```mysql
WITH cte AS (
    SELECT country_code, cou_name_en,
        ROW_NUMBER() OVER (PARTITION BY cou_name_en ORDER BY country_code) AS row_num
    FROM "{table_name}"
    WHERE population < {limit_num}
    AND country_code IS NOT NULL
    AND cou_name_en IS NOT NULL
)
SELECT country_code, cou_name_en
FROM cte
WHERE row_num = 1
ORDER BY cou_name_en;
```

## Output tabulated separated value file weekly
Write the result to a tabulated separated value file (`csv`) named after `result_countries_{Y-m-d}.csv`

* Find the directory result file :
```bash
docker ps

# Get the CONTAINER ID of the airflow-airflow-worker
docker exec -it ${CONTAINER ID} bash

default@${CONTAINER ID}:/opt/airflow$ more result_countries_${Y-m-d}.csv
``` 


