import os
import pandas as pd
import logging
import time
from google.cloud import bigquery
from getEngine import GetEngine

class GetClient:
    def __init__(self):
        try:          
            self.file_path = 'google_cred.json'
            os.path.isfile(self.file_path)
            self.client = bigquery.Client.from_service_account_json(self.file_path)            
        except Exception as e: print(e)
            

class GetBigData(GetEngine):
    data_list = []
    def get_df(self, query):
        logging.info("Start to read data")
        for chunk in pd.read_sql(sql=query[0], con=self.engine, chunksize=1000):
            GetBigData.data_list.append(chunk)
        data = pd.concat(GetBigData.data_list, ignore_index=True)
        return data
