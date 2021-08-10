
try:
    # Importing all the needed libraries
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from airflow.utils.dates import days_ago
    from datetime import date
    import pandas as pd
    from neo4j import GraphDatabase
    import os
    import random
    from datetime import datetime
    #establish the connection
    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))

# we have specified a genenal schema for all the user in our case it is 5
# This function will generate the random values for minute used for a application and will change the excution date in the json files
def generate_json(**context):
    import json
    # For loop to interate over the available json file in folder
    for file in os.listdir("/home/khushwant/Desktop/tribe.ai-Assignment/Data/user's data"):
        # check if file is in json format or not 
        if file.endswith(".json"): 
            json_path=os.path.join("/home/khushwant/Desktop/tribe.ai-Assignment/Data/user's data", file)
            # open the json file
            with open(json_path, 'r') as data_file:
                data =json.load(data_file)
            # changing the execution date
            ts = context["execution_date"]
            data["usages_date"]=str(ts)
            max_avilable_minutes=480
            # setting up random values for minute used
            for l in data["usages"]:
                l["minute_used"]=random.randint(0,max)
                max_avilable_minutes=max_avilable_minutes-l["minute_used"]
            # changing the json file with the changed data 
            with open(json_path,'w') as data_file:
                json.dump(data , data_file)


# our user data is ready now in next function we will push the data to database

def jsondata_to_neo4j():
    # Establshing the connection with neo4j GraphDatabase
    with open("/home/khushwant/Desktop/tribe.ai-Assignment/Data/Credentials/cred.json", 'r') as data_file:
            data =json.load(data_file)
    driver=GraphDatabase.driver(bolt_URL=data["bo"],auth=(data["username"],data["passowrd"]))
    session=driver.session()

    #iterating over user and inserting their data into the database
    import json
    for file in os.listdir("/home/khushwant/Desktop/tribe.ai-Assignment/Data/user's data"):
        if file.endswith(".json"): 
            json_path=os.path.join("/home/khushwant/Desktop/tribe.ai-Assignment/Data/user's data", file)
            with open(json_path, 'rb') as data_file:
                json_data = json.load(data_file)
                #MERGE commad is used instaed of create or match 

                #first we will create nodes for USER , DEVICE, and BRAND
                #Secondly we will interate over USAGES(app usaged)and make relations accordingly 
                """
                In the below for loop(FOREACH) we are creating the relation the first step is to create the app node( from usage)
                and then creating all the relationship 
                User- [:USED] -> App - [:ON] -> Device - [:OF] -> Brand
                This will be the basic structure of relationship between nodes
                """
                query = """
                with $json_data as data
                UNWIND data.user_id as userid
                UNWIND data.usages as usages
                UNWIND data.usages_date as usage_date
                UNWIND data.device as dev
                MERGE (user:User {user_id: data.user_id}) 
                MERGE (device:Device {os_id: dev.os}) 
                MERGE (brand:Brand {brand_id: dev.brand}) 
                FOREACH (l in usages | 
                    MERGE (app:App {app_name: l.app_name, app_category:l.app_category})
                    MERGE (user)-[:USED {TimeCreated:$current_date,TimeEvent:usage_date,UsageMinutes: l.minute_used}]->(app)
                    MERGE (app)-[:ON {TimeCreated:$current_date}]->(device)
                    MERGE (device)-[:OF{TimeCreated:$current_date}]->(brand))
            """

            session.run(query,json_data=json_data,current_date=datetime.now().strftime('%y-%m-%d'))



with DAG(
        dag_id="tribe",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        },
        start_date=days_ago(30),
        catchup=True) as f:

    # generate_json will change change the default json file according the conditon 
    # 1. generate minute_used randomly 
    # 2. changing usage_date
    generate_json = PythonOperator(
        task_id="generate_json",
        python_callable=generate_json,
        provide_context=True,
    )

    #jsondata_to_neo4j will push the data into database by reading the json files .
    jsondata_to_neo4j = PythonOperator(
        task_id="jsondata_to_neo4j",
        python_callable=jsondata_to_neo4j,
        provide_context=True,
    )

    generate_json >> jsondata_to_neo4j