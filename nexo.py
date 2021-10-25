from requests.auth import HTTPBasicAuth
import requests
import json
import csv
import math
import psycopg2

def connectCrypto():
    try:
        
        connect_str = "dbname='govern' user='postgres' host='localhost' " + \
                    "password='postgres'"
        # use our connection values to establish a connection
        conn = psycopg2.connect(connect_str)
        return conn
    except Exception as e:
        print("Uh oh, can't connect. Invalid dbname, user or password?")
        print(e)


def processCoinLoan(connection, insertTransSQL):
    with open('coinloan.csv', newline='') as csvfile:
        cursor = connection.cursor()
        coinreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        cnt = 1
        for row in coinreader:
            if cnt == 1: 
                cnt = cnt + 1
            else:
                transaction_type                = row[1]
                transaction_date                = row[0]
                base_currency                   = row[3]
                base_quantity                   = row[2]
                local_currency                  = 'USD'
                local_quantity                  = 0
                unit_price                      = 0
                unit_price_currency             = 'USD'
                trans_hash                      = ''
                trans_address                   = ''
                trans_url                       = ''
                trans_fee_local_quantity        = 0
                trans_fee_local_currency        = row[3]
                trans_fee_base_currency         = 'USD'
                trans_fee_base_quantity         = 0
                trans_base_currency             = row[3]
                trans_base_quantity             = row[2]            
                trans_local_currency            = 'USD'
                trans_local_quantity            = 0
                net_base_currency               = row[3]
                net_base_quantity               = row[2]
                net_local_currency              = 'USD'
                net_local_quantity              = 0
                source                          = 'CoinLoan'
                account_id                       = ''
                transaction_id                   = row[0]
                record_to_insert                = ( transaction_type
                                                , transaction_date
                                                , base_currency
                                                , base_quantity
                                                , local_currency
                                                , local_quantity
                                                , unit_price
                                                , unit_price_currency
                                                , trans_hash
                                                , trans_address
                                                , trans_url
                                                , trans_fee_base_currency
                                                , trans_fee_base_quantity
                                                , trans_base_currency
                                                , trans_base_quantity
                                                , trans_local_currency
                                                , trans_local_quantity
                                                , trans_fee_local_currency
                                                , trans_fee_local_quantity
                                                , net_base_currency
                                                , net_base_quantity
                                                , net_local_currency
                                                , net_local_quantity
                                                , source
                                                , account_id
                                                , transaction_id
                                                )          

                cursor.execute(insertTransSQL, record_to_insert)
        connection.commit()
        csvfile.close()




def processNexo(connection, insertTransSQL):
    with open('nexo.csv', newline='') as csvfile:
        cursor = connection.cursor()
        coinreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        cnt = 1
        for row in coinreader:
            if cnt == 1: 
                cnt = cnt + 1
            else:
                transaction_type                = row[1]
                transaction_date                = row[6]
                base_currency                   = row[2]
                base_quantity                   = row[3]
                local_currency                  = 'USD'
                local_quantity                  = 0
                unit_price                      = 0
                unit_price_currency             = 'USD'
                trans_hash                      = ''
                trans_address                   = ''
                trans_url                       = ''
                trans_fee_local_quantity        = 0
                trans_fee_local_currency        = row[2]
                trans_fee_base_currency         = 'USD'
                trans_fee_base_quantity         = 0
                trans_base_currency             = row[2]
                trans_base_quantity             = row[3]            
                trans_local_currency            = 'USD'
                trans_local_quantity            = 0
                net_base_currency               = row[2]
                net_base_quantity               = row[3]
                net_local_currency              = 'USD'
                net_local_quantity              = 0
                source                          = 'Nexo'
                account_id                       = ''
                transaction_id                   = row[0]
                record_to_insert                = ( transaction_type
                                                , transaction_date
                                                , base_currency
                                                , base_quantity
                                                , local_currency
                                                , local_quantity
                                                , unit_price
                                                , unit_price_currency
                                                , trans_hash
                                                , trans_address
                                                , trans_url
                                                , trans_fee_base_currency
                                                , trans_fee_base_quantity
                                                , trans_base_currency
                                                , trans_base_quantity
                                                , trans_local_currency
                                                , trans_local_quantity
                                                , trans_fee_local_currency
                                                , trans_fee_local_quantity
                                                , net_base_currency
                                                , net_base_quantity
                                                , net_local_currency
                                                , net_local_quantity
                                                , source
                                                , account_id
                                                , transaction_id
                                                )          

                cursor.execute(insertTransSQL, record_to_insert)
        connection.commit()
        csvfile.close()
                                            

def processBlockFi(connection, insertTransSQL):
    with open('blockfi.csv', newline='') as csvfile:
        cursor = connection.cursor()
        coinreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        cnt = 1
        for row in coinreader:
            if cnt == 1: 
                cnt = cnt + 1
            else:
                transaction_type                = row[2]
                transaction_date                = row[3]
                base_currency                   = row[0]
                base_quantity                   = row[1]
                local_currency                  = 'USD'
                local_quantity                  = 0
                unit_price                      = 0
                unit_price_currency             = 'USD'
                trans_hash                      = ''
                trans_address                   = ''
                trans_url                       = ''
                trans_fee_local_quantity        = 0
                trans_fee_local_currency        = 'USD'
                trans_fee_base_currency         = row[0]
                trans_fee_base_quantity         = 0
                trans_base_currency             = row[0]
                trans_base_quantity             = row[1]            
                trans_local_currency            = 'USD'
                trans_local_quantity            = 0
                net_base_currency               = row[0]
                net_base_quantity               = row[1]
                net_local_currency              = 'USD'
                net_local_quantity              = 0
                source                          = 'BlockFi'
                account_id                       = ''
                transaction_id                   = ''
                record_to_insert                = ( transaction_type
                                                , transaction_date
                                                , base_currency
                                                , base_quantity
                                                , local_currency
                                                , local_quantity
                                                , unit_price
                                                , unit_price_currency
                                                , trans_hash
                                                , trans_address
                                                , trans_url
                                                , trans_fee_base_currency
                                                , trans_fee_base_quantity
                                                , trans_base_currency
                                                , trans_base_quantity
                                                , trans_local_currency
                                                , trans_local_quantity
                                                , trans_fee_local_currency
                                                , trans_fee_local_quantity
                                                , net_base_currency
                                                , net_base_quantity
                                                , net_local_currency
                                                , net_local_quantity
                                                , source
                                                , account_id
                                                , transaction_id
                                                )          

                cursor.execute(insertTransSQL, record_to_insert)
        connection.commit()
        csvfile.close()
                                            

insertTransSQL = """INSERT INTO dbo.trans (
 transaction_type
, transaction_date
, base_currency
, base_quantity
, local_currency
, local_quantity
, unit_price
, unit_price_currency
, trans_hash
, trans_address
, trans_url
, trans_fee_base_currency
, trans_fee_base_quantity
, trans_base_currency
, trans_base_quantity
, trans_local_currency
, trans_local_quantity
, trans_fee_local_currency
, trans_fee_local_quantity
, net_base_currency
, net_base_quantity
, net_local_currency
, net_local_quantity
, source
, account_id
, transaction_id
) VALUES ( %s , %s , %s , %s , %s , %s, %s , %s , %s , %s , %s , %s , %s , %s , %s , %s , %s , %s , %s , %s , %s , %s , %s , %s , %s ,%s )"""             

connection = connectCrypto()
processNexo(connection, insertTransSQL)
processCoinLoan(connection, insertTransSQL)
processBlockFi(connection, insertTransSQL)

