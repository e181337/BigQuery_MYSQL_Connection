import os
from sqlalchemy.engine import create_engine      
import csv

class GetEngine:
    def __init__(self, db_name=None):
        if db_name is None:
            try:          
                self.file_path = 'google_cred.json'
                os.path.isfile(self.file_path)
                self.engine = create_engine('bigquery://', credentials_path=self.file_path)            
            except Exception as e: print(e) 
        else:
            config = GetCredentials()
            self.engine = create_engine('mysql+mysqlconnector://' + config.user + ':' + config.password + '@' + config.host + ':' + str(3306) + '/' + db_name , echo=False)
            
class GetCredentials:
    def __init__(self):
        try:
            with open("./my_sql_credentials.txt") as file:
                reader = list(csv.reader(file))
                self.user = reader[0][0]
                self.password = reader[1][0]
                self.host = '127.0.0.1'               
        except:
             raise ValueError("no credenitals")