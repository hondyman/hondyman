import urllib3
import json
import psycopg2
import time
import sys
import math
import datetime
from datetime import date






def getDBConnection():
    conn = psycopg2.connect(
                    host="192.168.87.33",
                    database="crypto",
                    user="postgres",
                    password="G@lw@y1970!"
                    )
    return conn

def bulk_insert_into_crypto_balance(table_name, column_names, data):
    column_names = ','.join(column_names)
    args_str = b','.join(cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s , %s, %s, %s, %s, %s, %s, %s)", x) for x in data).decode("utf-8")
    # decode was needed as args_str was a binary string which failed to be inserted into the command string
    command = """ INSERT INTO %s (%s) VALUES %s;""" % (table_name, column_names, args_str)
    return command





def insertCryptoBalance(rows):
    contracts = rows['data']
    wallet_address = contracts['address']
    effective_date = str(date.today())
    chain_id       = contracts['chain_id']
    if chain_id == 56:
        blockchain = 'BSC'

    tupList = []
    table_name = "token_balance"
    column_names = [   
                    "wallet_address",
                    "blockchain",
                    "effective_date",
                    "token_address",
                    "token_name",
                    "ticker",
                    "logo",
                    "transferred_at",
                    "token_type",
                    "balance",
                    "quote_rate",
                    "quote_rate_24h",
                    "balance_24h",
                    "quote",
                    "quote_24h"
                    ]

    tupList = []
    for row in contracts['items']:
        wallet_address      =   wallet_address
        blockchain          =   blockchain
        effective_date      =   effective_date
        token_address       =   row['contract_address']
        token_name          =   row['contract_name']
        ticker              =   row['contract_ticker_symbol']
        logo                =   row['logo_url']
        transferred_at      =   row['last_transferred_at']
        token_type          =   row['type']
        balance             =   row['balance']
        quote_rate          =   row['quote_rate']
        quote_rate_24h      =   row['quote_rate_24h']
        balance_24h         =   row['balance_24h']
        quote               =   row['quote']
        quote_24h           =   row['quote_24h']



        tup = tuple (  (    wallet_address,
                            blockchain,
                            effective_date,
                            token_address,
                            token_name,
                            ticker,
                            logo,
                            transferred_at,
                            token_type,
                            balance,
                            quote_rate,
                            quote_rate_24h,
                            balance_24h,
                            quote,
                            quote_24h                       
                        ) )
        tupList.append(tup)
    cmd = bulk_insert_into_crypto_balance(table_name, column_names, tupList) 
    
    cur.execute(cmd)
    conn.commit()



http = urllib3.PoolManager()
url = "https://api.covalenthq.com/v1/56/address/0x932dC422363267B844CA5c58Fa65dfDc1F1Fa56F/balances_v2/?no-nft-fetch=true&quote-currency=USD&key=ckey_fd757fb340bf4bc383682f3a8ba"

response = http.request('GET', url)
conn = getDBConnection()
cur = conn.cursor()
rows = json.loads(response.data.decode('utf-8'))
insertCryptoBalance(rows)
cur.close()
conn.close()



