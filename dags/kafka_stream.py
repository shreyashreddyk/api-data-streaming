from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
from kafka import KafkaProducer
import time
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 9, 12, 00)
}

def get_data():
    import requests

    response = requests.get('https://randomuser.me/api/')
    response_json = response.json()
    response = response_json['results'][0]

    return response

def format_data(response):
    formatted_data = {
        'title': response['name']['title'],
        'first_name': response['name']['first'],
        'last_name': response['name']['last'],
        'gender': response['gender'],
        'address': str(response['location']['street']['number']) + ' ' + str(response['location']['street']['name']) + ', ' + str(response['location']['city']) + ', ' + str(response['location']['state']) + ', ' + str(response['location']['country']),
        'postcode': response['location']['postcode'],
        'latitude': response['location']['coordinates']['latitude'],
        'longitude': response['location']['coordinates']['longitude'],
        'timezone_offset': response['location']['timezone']['offset'],
        'timezone_description': response['location']['timezone']['description'],
        'email': response['email'],
        'username': response['login']['username'],
        'password': response['login']['password'],
        'salt': response['login']['salt'],
        'uuid': response['login']['uuid'],
        'dob_date': response['dob']['date'],
        'dob_age': response['dob']['age'],
        'registered_date': response['registered']['date'],
        'registered_age': response['registered']['age'],
        'phone': response['phone'],
        'cell': response['cell'],
        'id_name': response['id']['name'],
        'id_value': response['id']['value'],
        'picture_large': response['picture']['large'],
        'picture_medium': response['picture']['medium'],
        'picture_thumbnail': response['picture']['thumbnail'],
        'nationality': response['nat']
    }
    return formatted_data

def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms = 5000)
    curr_time = time.time()

    while True:
        if time.time() - curr_time > 60: #1 minute
            break
        try:
            producer.send('users-data', json.dumps(format_data(get_data())).encode('utf-8'))
        except Exception as e:
            logging.error(e)
            continue
        

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    stream_data = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )