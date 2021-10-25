

# import urllib library
from typing import ChainMap
from urllib.request import urlopen
  
# import json
import json

import psycopg2


def insertFarm(rows):
        tupList = []
        table_name = "farm"
        column_names = ["id", "farm_name", "token"    , "platform", "provider_id", "provider_label", "provider_url", "provider_token", "transaction_token_id", "transaction_address","lp_address", "earns", "chain", "leverage", "compound", "tvl_amount", "tvl_usd"]
        tupList = []
        for row in rows:
            id              = row["id"]
            farm_name       = row["Oname"]
            token           = row["token"] 
            platform        = row["platform"] 
            provider_id     = row["provider"]["id"]     
            provider_label  = row["provider"]["label"]
            provider_url    = row["provider"]["url"]
            provider_token  = row["provider"]["token"]
            transaction_token_id = row["extra"]["transactionToken"]
            transaction_address = row["extra"]["transactionAddress"]
            lp_address      = row["extra"]["lpAddress"]
            earns           = row["earns"]
            chain        = row["chain"]
            tvl_amount   = row["tvl"]["amount"]
            tvl_usd      = row["tvl"]["usd"]
            yield_apy    = row["yield"]["apy"]
            yield_daily  = row["yield"]["daily"]
            yield_apr    = row["yield"]["apr"]

            tup = tuple (  (    edgeUid,
                                edgeTypeUid,
                                SubjectUid,
                                ObjectUid, 
                                contentJSON,
                                subjectPath,
                                objectPath,
                                contentJSON,
                                subjectPath,
                                objectPath                            
                            ) )
            tupList.append(tup)
        cmd = self.bulk_insert_into_edgeTemp(table_name, column_names, tupList)
        self._cur.execute(cmd)
        self._conn.commit()
        



# store the URL in url as 
# parameter for urlopen
url = "https://farm.army/api/v0/farms"
  
# store the response of URL
response = urlopen(url)
  
# storing the JSON response 
# from url in data
data_json = json.loads(response.read())
  
# print the json response

conn = psycopg2.connect(
    host="192.168.87.33",
    database="crypto",
    user="postgres",
    password="G@lw@y1970!")

   # create a cursor
cur = conn.cursor()
      
for row in data_json:
    


