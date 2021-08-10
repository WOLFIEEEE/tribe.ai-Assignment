# Airflow-scheduler with neo4j


### An airflow dag which is scheduled on a daily basis to collect user's data and push that into neo4j database.

## Features:
- This will automatically generate a json file for partcular user and push that json file into the neo4j database
- It will collect the last 30 days data , from the date the dag is started.

## Requirements:
- [Python3](https://www.python.org/downloads/)
- [Neo4j Sandbox](https://neo4j.com/sandbox/)
- [Apache Airflow](https://airflow.apache.org/)

## How to run:
- **Step 1**:
    Create a new directory and create a virtual environment:
    ```bash
    virtualenv env
    ```
- **Step 2**:
    Install Apache airflow in the environment.
- **Step 3**;
    Install the requirement.txt files [Requirement.txt](requirement.txt) 
     ```bash
    pip install -r requiremnet.txt
    ```
- **Step 4**:
    Create a account on neo4j and open Sandbox [neo4j Sandbox](https://sandbox.neo4j.com/)

- **Step 5**:
    Extract Credential from there like username ,password and bolt URL and insert that into [Data/credentials/cred.json]() file and update path in dag functions .

- **Step 6** :
    start apache webserver
     ```bash
    airflow webserver -p 8080
    ```
    Run the Scheduler
    ```bash
    airflow scheduler
    ```
- **Step 7**:
    Open the port [localhost:8080](localhost:8080)

- **Step 8**:
    Start the dag if it is not already started.
- **Step 9** 
    Open neo4j sandbox and run 
    ```bash
    match(n) return(n)
    ```
    To see the results

## Flow of dag:

        [Default user's data]->[generate_json()]->[Updated user's data]->[jsondata_to_neo4j]

- **Defualt user's data**:- We have created a five dummy files in data folder for each user, that will be our default data .It is stored in the location **Data/user's data/**

- **generate_json()**:- It will read those default data and update it accoding to our condition Like changing minute_used varibale of app with random data .

- **Updated user's data**:- These will be the json files that are updated using generate_json() function of given conditions.

-  **jsondata_to_neo4j()**:- This function will read the data from json file and push that data into neo4j database . 

