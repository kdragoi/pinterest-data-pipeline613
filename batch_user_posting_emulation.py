
import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
from datetime import datetime


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            pin_invoke_url = "https://qklaofl8zd.execute-api.us-east-1.amazonaws.com/Development/topics/0a40ea42f8d1.pin"

            for row in pin_selected_row:
                pin_df = dict(row._mapping)
                pin_payload = json.dumps({
                    "records": [{
                        "value": {"index": pin_df["index"], "unique_id": pin_df["unique_id"], "title": pin_df["title"], 
                                  "description": pin_df["description"], "poster_name": pin_df["poster_name"], 
                                  "follower_count": pin_df["follower_count"], "tag_list": pin_df["tag_list"], 
                                  "is_image_or_video": pin_df["is_image_or_video"], "image_src": pin_df["image_src"],
                                  "downloaded": pin_df["downloaded"], "save_location": pin_df["save_location"],
                                  "category": pin_df["category"]}
                    }]
                })
                
                headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
                pin_response = requests.request("POST", pin_invoke_url, headers=headers, data=pin_payload)
            
            print(pin_response.status_code)


            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            geo_invoke_url = "https://qklaofl8zd.execute-api.us-east-1.amazonaws.com/Development/topics/0a40ea42f8d1.geo"

            for row in geo_selected_row:
                geo_df = dict(row._mapping)
                geo_payload = json.dumps({
                    "records": [{
                        "value": {"ind": geo_df["ind"], "timestamp": (geo_df["timestamp"]).isoformat(), "latitude": geo_df["latitude"], 
                                  "longitude": geo_df["longitude"], "country": geo_df["country"]}
                    }]
                })
                
                headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
                geo_response = requests.request("POST", geo_invoke_url, headers=headers, data=geo_payload)
            
            print(geo_response.status_code)

            #datetime formated values resulted in type error so had to convert to datetime string using isoformat()


            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            user_invoke_url = "https://qklaofl8zd.execute-api.us-east-1.amazonaws.com/Development/topics/0a40ea42f8d1.user"

            for row in user_selected_row:
                user_df = dict(row._mapping)
                user_payload = json.dumps({
                    "records": [{
                        "value": {"ind": user_df["ind"], "first_name": user_df["first_name"], "last_name": user_df["last_name"], 
                                  "age": user_df["age"], "date_joined": (user_df["date_joined"]).isoformat()}
                    }]
                })
                
                headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
                user_response = requests.request("POST", user_invoke_url, headers=headers, data=user_payload)

            print(user_response.status_code)


#%%

if __name__ == "__main__":
    run_infinite_post_data_loop()
