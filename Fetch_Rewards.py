#Necessary imports. Includes method to install those that may not already be installed.
from datetime import date
import subprocess
import sys
import json
import time
try:
    import psycopg2
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'psycopg2'])
finally:
    import psycopg2
import psycopg2.extras as extras
try:
    import docker
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'docker'])
finally:
    import docker
try:
    import pandas as pd
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'pandas'])
finally:
    import pandas as pd
import numpy as np


#Define global variables
client = docker.from_env()
CONFIGS = {'version':'3.9',
           'services':{
                'localstack':{
                    'image':'fetchdocker/data-takehome-localstack',
                    'ports':'4566:4566'},
                'postgres':{
                    'image':'fetchdocker/data-takehome-postgres',
                    'ports':'5432:5432'}
            }}


def get_containers(configs, client):
    '''
    Uses configs and docker client to retrieve localstack and postgres images.
    Starts running containers from those images and returns running container instances.
    '''

    #get names of images
    localstack_name = configs['services']['localstack']['image']
    postgres_name = configs['services']['postgres']['image']

    #check if images already exist and pulls if not
    try:
        client.images.get(localstack_name)
    except:
        client.images.pull(localstack_name)

    try:
        client.images.get(postgres_name)
    except:
        client.images.pull(postgres_name)

    #run containers
    localstack_cont = client.containers.run(localstack_name, detach=True)
    postgres_cont = client.containers.run(postgres_name, detach=True)

    return localstack_cont, postgres_cont

def get_login_info(container):
    '''
    Retrieves login data from container and stores it in pandas dataframe.
    '''

    #run command to get data
    response = container.exec_run('awslocal sqs receive-message --queue-url http://localhost:4566/000000000000/login-queue')

    #command does not always work correctly right away, this allows for limited retries if it fails initially
    for i in range(5):
        try:
            message = json.loads(response[1].decode('utf-8'))
            data = [json.loads(i['Body']) for i in message['Messages']]
            df = pd.DataFrame.from_dict(data)
            df['create_date'] = [date.today()]*len(data)
            container.stop()
            return df
        except json.decoder.JSONDecodeError:
            time.sleep(1)
            response = container.exec_run('awslocal sqs receive-message --queue-url http://localhost:4566/000000000000/login-queue')

def mask_pii(df):
    '''
    Masks ip addresses and device ids.
    Uses simple number manipulation to change values while retaining format and uniqueness.
    '''

    masked_ips = []
    masked_dev_ids = []

    #create masked values by subtracting the portions from a larger number
    for i in df['ip']:
        masked_ips.append('.'.join([str(255-int(j)) for j in i.split('.')]))

    for i in df['device_id']:
        masked_dev_ids.append('-'.join([str(10000-int(j)) for j in i.split('-')]))

    #drop original values from df, replace with masked values
    masked_df = df.copy().drop(columns = ['ip', 'device_id'])
    masked_df['masked_ip'] = masked_ips
    masked_df['masked_device_id'] = masked_dev_ids

    return masked_df

def insert_to_postgres(conn, df):
    '''
    Inserts masked data into postgres db.
    '''

    #Initializes cursor
    cursor = conn.cursor()
    cols = ','.join(list(df.columns))
    #Defines queries needed
    create_table_query = 'CREATE TABLE IF NOT EXISTS user_logins(user_id varchar(128),device_type varchar(32),masked_ip varchar(256),masked_device_id varchar(256),locale varchar(32),app_version varchar(32),create_date date);'
    insert_query = "INSERT INTO user_logins(%s) VALUES %%s" % (cols)

    #Creates table if does not already exist
    try:
        cursor.execute(create_table_query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1

    #Prepares and inserts data
    tuples = [tuple(x) for x in df.to_numpy()]
    
    try:
        extras.execute_values(cursor, insert_query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1

    cursor.close()

def main():
    '''
    Runs the components of the program.
    '''
    localstack, postgres = get_containers(CONFIGS, client)
    df = get_login_info(localstack)
    masked_df = mask_pii(df)

    conn = psycopg2.connect(database='postgres', user='postgres', password='postgres', host='localhost', port='5432')

    insert_to_postgres(conn, masked_df)

    postgres.stop()

if __name__ == "__main__":
    main()