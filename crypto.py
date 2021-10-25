import httpretty as hp
import time
import json
import datetime
import requests


from pycoingecko import CoinGeckoAPI



from coinbase.wallet.client import Client
from coinbase.wallet.client import OAuthClient
from coinbase.wallet.error import APIError
from coinbase.wallet.error import AuthenticationError
from coinbase.wallet.error import InvalidTokenError
from coinbase.wallet.error import TwoFactorRequiredError
from coinbase.wallet.error import ExpiredTokenError
from coinbase.wallet.error import RevokedTokenError
from coinbase.wallet.model import APIObject
from coinbase.wallet.model import Account
from coinbase.wallet.model import Merchant
from coinbase.wallet.model import Checkout
from coinbase.wallet.model import Address
from coinbase.wallet.model import Order
from coinbase.wallet.model import Buy
from coinbase.wallet.model import CurrentUser
from coinbase.wallet.model import Deposit
from coinbase.wallet.model import PaymentMethod
from coinbase.wallet.model import Sell
from coinbase.wallet.model import Transaction
from coinbase.wallet.model import User
from coinbase.wallet.model import Withdrawal
from coinbase.wallet.model import Report
import psycopg2
import json

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

def cb_buyTransaction(transaction):
    transaction_type                = 'BUY'
    transaction_date                = transaction['created_at']
    base_currency                   = transaction['amount']['currency']
    base_quantity                   = transaction['amount']['amount']
    local_currency                  = transaction['subtotal']['currency']
    local_quantity                  = transaction['subtotal']['amount']
    unit_price                      = transaction['unit_price']['amount']
    unit_price_currency             = transaction['unit_price']['currency']
    trans_hash                      = ''
    trans_address                   = ''
    trans_url                       = ''
    trans_fee_local_quantity        = 0
    trans_fee_local_currency        = ''
    for fee in transaction['fees']:
            trans_fee_local_currency = fee['amount']['currency']
            trans_fee_local_quantity = trans_fee_local_quantity + float(fee['amount']['amount'])
            trans_fee_base_currency  = transaction['amount']['currency']
    trans_fee_base_quantity         = trans_fee_local_quantity / float(transaction['unit_price']['amount'])
    trans_base_currency             = transaction['amount']['currency']
    trans_base_quantity             = transaction['amount']['amount']            
    trans_local_currency            = transaction['subtotal']['currency']
    trans_local_quantity            = transaction['subtotal']['amount']
    net_base_currency               = transaction['amount']['currency']
    net_base_quantity               = transaction['amount']['amount']
    net_local_currency              = transaction['total']['currency']
    net_local_quantity              = transaction['total']['amount']
    source                          = 'Coinbase'
    account_id                       = ''
    transaction_id                   = transaction['id']
   

 
    record_to_insert = ( transaction_type
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
    return record_to_insert


def cb_interestTransaction(transaction):
    transaction_type                = 'INTEREST'
    transaction_date                = transaction['created_at']
    base_currency                   = transaction['amount']['currency']
    base_quantity                   = float(transaction['amount']['amount'])
    local_currency                  = transaction['native_amount']['currency']
    local_quantity                  = float(transaction['native_amount']['amount'])
    unit_price_currency             = transaction['native_amount']['currency']
    unit_price                      = local_quantity / base_quantity
    trans_hash                      = ''
    trans_address                   = ''
    trans_url                       = ''
    trans_fee_local_quantity        = 0
    trans_fee_local_currency        = ''
    trans_fee_local_currency        = transaction['native_amount']['currency']
    trans_fee_local_quantity        = 0
    trans_fee_base_currency         = transaction['amount']['currency']
    trans_fee_base_quantity         = 0
    trans_base_currency             = transaction['amount']['currency']
    trans_base_quantity             = transaction['amount']['amount']            
    trans_local_currency            = transaction['native_amount']['currency']
    trans_local_quantity            = transaction['native_amount']['amount']
    net_base_currency               = transaction['amount']['currency']
    net_base_quantity               = transaction['amount']['amount']
    net_local_currency              = transaction['native_amount']['currency']
    net_local_quantity              = transaction['native_amount']['amount']
    source                          = 'Coinbase'
    account_id                       = ''
    transaction_id                   = transaction['id']
    record_to_insert = ( transaction_type
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
    return record_to_insert



def cb_depositTransaction(transaction):
    transaction_type                = 'DEPOSIT'
    transaction_date                = transaction['created_at']
    base_currency                   = transaction['amount']['currency']
    base_quantity                   = transaction['amount']['amount']
    local_currency                  = transaction['amount']['currency']
    local_quantity                  = transaction['amount']['amount']
    unit_price                      = 1
    unit_price_currency             = transaction['amount']['currency']
    trans_hash                      = ''
    trans_address                   = ''
    trans_url                       = ''
    trans_fee_local_quantity        = 0
    trans_fee_local_currency        = ''
    for fee in transaction['fees']:
            trans_fee_local_currency = fee['amount']['currency']
            trans_fee_local_quantity = trans_fee_local_quantity + float(fee['amount']['amount'])
            trans_fee_base_currency  = transaction['amount']['currency']
    trans_fee_base_quantity         = float(trans_fee_local_quantity) / float(unit_price)
    trans_base_currency             = transaction['amount']['currency']
    trans_base_quantity             = transaction['amount']['amount']            
    trans_local_currency             = transaction['amount']['currency']
    trans_local_quantity             = transaction['amount']['amount']            
    net_base_currency               = transaction['amount']['currency']
    net_base_quantity               = transaction['amount']['amount']
    net_local_currency              = transaction['amount']['currency']
    net_local_quantity              = transaction['amount']['amount']
    source                          = 'Coinbase'
    account_id                       = ''
    transaction_id                   = transaction['id']
   

 
    record_to_insert = ( transaction_type
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
    return record_to_insert



def cb_withdrawTransaction(transaction):
    transaction_type                = 'WITHDRAW'
    transaction_date                = transaction['created_at']
    base_currency                   = transaction['amount']['currency']
    base_quantity                   = transaction['amount']['amount']
    local_currency                  = transaction['amount']['currency']
    local_quantity                  = transaction['amount']['amount']
    unit_price                      = 1
    unit_price_currency             = transaction['amount']['currency']
    trans_hash                      = ''
    trans_address                   = ''
    trans_url                       = ''
    trans_fee_local_quantity        = 0
    for fee in transaction['fees']:
            trans_fee_local_currency = fee['amount']['currency']
            trans_fee_local_quantity = trans_fee_local_quantity + float(fee['amount']['amount'])
            trans_fee_base_currency  = transaction['amount']['currency']
    trans_fee_base_quantity         = float(trans_fee_local_quantity) / float(unit_price)
    trans_base_currency             = transaction['amount']['currency']
    trans_base_quantity             = transaction['amount']['amount']            
    trans_local_currency            = transaction['amount']['currency']
    trans_local_quantity            = unit_price * float(trans_base_quantity)
    net_base_currency               = transaction['amount']['currency']
    net_base_quantity               = transaction['amount']['amount']
    net_local_currency              = transaction['amount']['currency']
    net_local_quantity              = transaction['amount']['amount']
    source                          = 'Coinbase'
    account_id                       = ''
    transaction_id                   = transaction['id']
   

 
    record_to_insert = ( transaction_type
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
    return record_to_insert
 
def cb_sendTransaction(transaction):
    
    transaction_type                = 'SEND'
    transaction_date                = transaction['created_at']
    base_currency                   = transaction['amount']['currency']
    base_quantity                   = float(transaction['amount']['amount'])
    local_currency                  = transaction['native_amount']['currency']
    local_quantity                  = float(transaction['native_amount']['amount'])
    unit_price                      = float(local_quantity/base_quantity)
    unit_price_currency             = transaction['native_amount']['currency']
    trans_hash                      = ''
    trans_address                   = ''
    trans_url                       = ''
    if 'network' in transaction:
        if 'transaction_fee' in transaction['network']: 
            trans_fee_base_currency = transaction['network']['transaction_fee']['currency'] 
            trans_fee_base_quantity = float(transaction['network']['transaction_fee']['amount'] )
            trans_fee_local_currency = transaction['native_amount']['currency'] 
            trans_fee_local_quantity = unit_price  *  trans_fee_base_quantity    
        else:     
            trans_fee_base_currency = transaction['amount']['currency'] 
            trans_fee_base_quantity = 0 
            trans_fee_local_currency = transaction['native_amount']['currency'] 
            trans_fee_local_quantity = 0 
    
        if 'transaction_amount' in transaction['network']: 
            trans_base_currency  = transaction['network']['transaction_amount']['currency'] 
            trans_base_quantity  = (base_quantity + trans_fee_base_quantity)  
            trans_local_currency = transaction['native_amount']['currency'] 
            trans_local_quantity = (local_quantity + trans_fee_local_quantity) * unit_price 
        else:     
            trans_base_currency = transaction['amount']['currency'] 
            trans_base_quantity = transaction['amount']['amount']
            trans_local_currency = transaction['native_amount']['currency'] 
            trans_local_quantity = transaction['native_amount']['amount'] 

    trans_base_currency = transaction['amount']['currency'] 
    trans_base_quantity = transaction['amount']['amount']
    trans_local_currency = transaction['native_amount']['currency'] 
    trans_local_quantity = transaction['native_amount']['amount'] 
    trans_fee_base_currency = transaction['amount']['currency'] 
    trans_fee_base_quantity = 0 
    trans_fee_local_currency = transaction['native_amount']['currency'] 
    trans_fee_local_quantity = 0 

    net_base_currency               = transaction['amount']['currency']
    net_base_quantity               = transaction['amount']['amount']
    net_local_currency              = transaction['native_amount']['currency']
    net_local_quantity              = transaction['native_amount']['amount']
    source                          = 'Coinbase'
    account_id                       = ''
    transaction_id                   = transaction['id']
    record_to_insert = ( transaction_type
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
    return record_to_insert


def cb_convertTransaction(transaction):
    transaction_type                = 'CONVERT'
    transaction_date                = transaction['created_at']
    base_currency                   = transaction['amount']['currency']
    base_quantity                   = float(transaction['amount']['amount'])
    local_currency                  = transaction['native_amount']['currency']
    local_quantity                  = float(transaction['native_amount']['amount'])
    unit_price                      = float(local_quantity/base_quantity)
    unit_price_currency             = transaction['native_amount']['currency']
    trans_hash                      = ''
    trans_address                   = ''
    trans_url                       = ''
    if 'network' in transaction:
        if 'transaction_fee' in transaction['network']: 
            trans_fee_base_currency = transaction['network']['transaction_fee']['currency'] 
            trans_fee_base_quantity = float(transaction['network']['transaction_fee']['amount'] )
            trans_fee_local_currency = transaction['native_amount']['currency'] 
            trans_fee_local_quantity = unit_price  *  trans_fee_base_quantity    
        else:     
            trans_fee_base_currency = transaction['amount']['currency'] 
            trans_fee_base_quantity = 0 
            trans_fee_local_currency = transaction['native_amount']['currency'] 
            trans_fee_local_quantity = 0 
 
        if 'transaction_amount' in transaction['network']: 
            trans_base_currency  = transaction['network']['transaction_amount']['currency'] 
            trans_base_quantity  = (base_quantity + trans_fee_base_quantity)  
            trans_local_currency = transaction['native_amount']['currency'] 
            trans_local_quantity = (local_quantity + trans_fee_local_quantity) * unit_price 
        else:     
            trans_base_currency = transaction['amount']['currency'] 
            trans_base_quantity = transaction['amount']['amount']
            trans_local_currency = transaction['native_amount']['currency'] 
            trans_local_quantity = transaction['native_amount']['amount'] 
    else:
        trans_fee_base_currency = transaction['amount']['currency'] 
        trans_fee_base_quantity = 0 
        trans_fee_local_currency = transaction['native_amount']['currency'] 
        trans_fee_local_quantity = 0 
        trans_base_currency = transaction['amount']['currency'] 
        trans_base_quantity = transaction['amount']['amount']
        trans_local_currency = transaction['native_amount']['currency'] 
        trans_local_quantity = transaction['native_amount']['amount'] 

    net_base_currency               = transaction['amount']['currency']
    net_base_quantity               = float(transaction['native_amount']['amount']) 
    net_local_currency              = transaction['native_amount']['currency']
    net_local_quantity              = float(transaction['native_amount']['amount']) 
    source                          = 'Coinbase'
    account_id                       = ''
    transaction_id                   = transaction['id'] + '_base'
    record_to_insert = ( transaction_type
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
    return record_to_insert
   
  






    
   

def notDoing():

    # Dummy API key values for use in tests
    api_key = 'HwXBjalVp5eyPKfo'
    api_secret = '8sHXSWQ28czJ0bdsuxZKiyLtZzOh3SoL'
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







   

    api_key = 'HwXBjalVp5eyPKfo'
    api_secret = '8sHXSWQ28czJ0bdsuxZKiyLtZzOh3SoL' 
    client = Client(api_key, api_secret)
    connection = connectCrypto()
    cursor = connection.cursor()
    acct_starting_after = None
    all_account = []
    all_buys = []
    all_deposits = []
    all_withdrawals = []
    all_transactions = []
    all_orders = []

    while True:
        accounts = client.get_accounts(limit=100, starting_after=acct_starting_after)
        if accounts.pagination.next_starting_after is not None:
                acct_starting_after = accounts.pagination.next_starting_after
                for account in accounts.data:
                    
                    all_account.append(account)
                    time.sleep(1)  # Let's not hit the rate limiting
        else:
                for account in accounts.data:
                        all_account.append(account)
                break   


    for account in all_account:

        print('Account::::' + account['currency'] + '  AccountID:' + account['id'])
        buys_starting_after = None
        deposits_starting_after = None
        withdrawals_starting_after = None
        trans_starting_after = None
        orders_starting_after = None
        
        print('Processing.... Transactions' )
        while True:
            trans = client.get_transactions(account['id'], starting_after=trans_starting_after)
            if trans.pagination.next_starting_after is not None:
                trans_starting_after = trans.pagination.next_starting_after
                for transaction in trans.data:
                    all_transactions.append(transaction)
                    time.sleep(1)  # Let's not hit the rate limiting
            else:
                for transaction in trans.data:
                    all_transactions.append(transaction)
                break

        
        print('Processing.... Withdrawals' )
        while True:
            withdrawals = client.get_withdrawals(account['id'], starting_after=withdrawals_starting_after)
            if withdrawals.pagination.next_starting_after is not None:
                withdrawals_starting_after = withdrawals.pagination.next_starting_after
                for withdrawal in withdrawals.data:
                    all_withdrawals.append(withdrawal)
                    time.sleep(1)  # Let's not hit the rate limiting
            else:
                for withdrawal in withdrawals['data']:
                    all_withdrawals.append(withdrawal)
                break
        print('Processing.... Buys' )
        while True:
            buys = client.get_buys(account['id'], starting_after=buys_starting_after)
            if buys.pagination.next_starting_after is not None:
                buys_starting_after = buys.pagination.next_starting_after
                for buy in buys.data:
                    all_buys.append(buy)
                    time.sleep(1)  # Let's not hit the rate limiting
            else:
                for buy in buys.data:
                    all_buys.append(buy)
                break
        print('Processing.... Deposits' )    
        while True:
            deposits = client.get_deposits(account['id'], starting_after=deposits_starting_after)
            if deposits.pagination.next_starting_after is not None:
                deposits_starting_after = deposits.pagination.next_starting_after
                for deposit in deposits.data:
                    all_deposits.append(deposit)
                    time.sleep(1)  # Let's not hit the rate limiting
            else:
                for deposit in deposits.data:
                    all_deposits.append(deposit)
                break



    

    cursor = connection.cursor()

    for order in all_orders:
        x=1
    connection.commit()        

    with open('coinbase.json', 'w') as json_file:
            json.dump(all_transactions, json_file) 

    for transaction in all_transactions:
        print('Transaction:::' + transaction.type)
        if transaction.type in ["send","pro_deposit"]:
            record_to_insert = cb_sendTransaction(transaction)
            cursor.execute(insertTransSQL, record_to_insert)
        if transaction.type == "trade":
            record_to_insert = cb_convertTransaction(transaction)
            cursor.execute(insertTransSQL, record_to_insert)
        if transaction.type == "interest":
            record_to_insert = cb_interestTransaction(transaction)
            cursor.execute(insertTransSQL, record_to_insert)
    connection.commit()        
        

    for transaction in all_withdrawals:
        record_to_insert = cb_withdrawTransaction(transaction)
        cursor.execute(insertTransSQL, record_to_insert)
    connection.commit()

    for transaction in all_buys:
        record_to_insert = cb_buyTransaction(transaction)
        cursor.execute(insertTransSQL, record_to_insert)
    connection.commit()

    for transaction in all_deposits:
        record_to_insert = cb_depositTransaction(transaction)
        cursor.execute(insertTransSQL, record_to_insert)
    connection.commit()

    connection.close()

    coinLoanKey = "b8eb47a895d3732f35a7574a9fec129b"
    coinLoanSecret = "933a2f9cc68b6ff062b92b98d7928a4e2bdac40c26f86bd1a2fc275f06475374"




insertPriceSQL = """INSERT INTO dbo.price_tbl (
    source ,
    effective_date,
    coin ,
    price ) VALUES ( %s , %s , %s , %s ) ON CONFLICT DO NOTHING"""
connection = connectCrypto()
cursor = connection.cursor()
coinList = ['bitcoin', 'binancecoin', 'ethereum', 'usd-coin','binance-usd']
cg = CoinGeckoAPI()
coins = cg.get_price(ids=coinList, vs_currencies='usd')

start_date = datetime.date(2021, 4, 1)
end_date = datetime.date(2021, 4, 1)
delta = datetime.timedelta(days=1)
source = 'CoinGecko'
while start_date <= end_date:
    sStartDate = start_date.strftime('%d-%m-%Y')
    effective_date = start_date
    print ('Effective Date:' + sStartDate)
    for coin in coinList:
        time.sleep(.500)
        coinPrice = cg.get_coin_history_by_id(coin,sStartDate)
        coinSymbol = coinPrice['symbol']
        price = coinPrice['market_data']['current_price']['usd']
        print ('Coin:' + coin + '  Price: ' + str(price) )
        record_to_insert = (  source
                            , effective_date
                            , coinSymbol
                            , price
                            )              
        cursor.execute(insertPriceSQL, record_to_insert)
    start_date += delta
    time.sleep(1)
connection.commit()  
#priceList = ["bnb",'cake','bunny']
#r =requests.get('https://farm.army/api/v0/prices')
#prices = r.json()
#for price in prices:
#    if price.find('cake') or price.find(bunny) or price.find('beef') or price.find('bfi')


insertFarm = """INSERT INTO dbo.farm (
     id
    , name
    , platform
    , tvl_base_curr
    , tvl_local_curr
    , tvl_base_amt
    , tvl_local_amt
    , auto_compounding
    , earn
    , property
    , token
) VALUES (
     %s  -- id text
    , %s -- name text
    , %s -- platform text
    , %s -- tvl_base_curr text
    , %s -- tvl_local_curr text
    , %s -- tvl_base_amt numeric NULLABLE
    , %s -- tvl_local_amt numeric NULLABLE
    , %s -- auto_compounding boolean NULLABLE
    , %s -- earn json NULLABLE
    , %s -- property json NULLABLE
    , %s -- Token
)"""

r =requests.get('https://farm.army/api/v0/farms')
farms = r.json()
for farm in farms:
    
    if farm['platform'] in ['pancake','pancakebunny']:
        id = farm['id']
        name = farm['name']
        token = farm['token']
        platform = farm['platform'] 
        earnings = farm['earnings']
        tvl_base_curr = farm['token']
        tvl_local_amt = farm['tvl']['usd']
        tvl_local_curr = 'usd'
        tvl_base_amt = farm['tvl']['amount']
        auto_compounding = False
        earnings = '{}'
        properties = '{}'
        token = farm['token']

        farm_record_to_insert = ( id
        , name
        , platform
        , tvl_base_curr
        , tvl_local_curr
        , tvl_base_amt
        , tvl_local_amt
        , auto_compounding
        , earnings
        , properties
        , token)
        cursor.execute(insertFarm, farm_record_to_insert)
        connection.commit()

    y = 0
cursor.close()
connection.close()
    