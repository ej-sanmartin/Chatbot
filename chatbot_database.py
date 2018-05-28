# Creating a database with sqlite from seperate file or API

import sqlite3
import json
from datetime import datetime

timeframe = '2015-05'
sql_transaction = []

connection = sqlite3.connect('{}.db'.format(timeframe))
c = connection.cursor()

# Adjust according to how data set is organized (ex. colummn names)
def create_table():
    c.execute("""CREATE TABLE IF NOT EXIST parent_reply
    (parent_id TEXT Primary KEY, comment_id TEXT UNIQUE, parent TEXT, comment TEXT,
    subreddit TEXT, unix INT, score INT)""")

# Cleans up data
def format_data(data):    
    data = data.replace("\n"," newlinechar ").replace("\r", " newlinechar ").replace('"', "'")
    return data

def find_parent(pid):
    try:
        sql = "SLECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else: return False
    except Exception as e:
        print("find_parent", e)
        return False
    
if __name__ == "__main__":
    create_table()
    row_counter = 0
    paired_rows = 0
    
    # Open location of file
    with open("#Location of file => ex: J:/chatdata/reddit_data/{}/RC{}".format(timeframe.split("-")[0], timeframe), buffer=1000) as f:
        for row in f:
            row_counter += 1
            row = json.loads(row)
            parent_id = row["parent_id"]
            body = format_data(row["body"])
            created_utc = row["created_utc"]
            score = row["score"]
            subreddit = row["subreddit"]
            
            parent_data = find_parent(parent_id)
