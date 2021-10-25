#!/usr/bin/env python

# This file is part of krakenex.
# Licensed under the Simplified BSD license. See `examples/LICENSE.txt`.

# Saves trade history to CSV file.
#
# WARNING: submits a lot of queries in rapid succession!

# Maintainer: Austin.Deric@gmail.com (@AustinDeric on github)

#import pandas as ps
import krakenex

import datetime
import calendar
import time

# takes date and returns nix time
def date_nix(str_date):
    return calendar.timegm(str_date.timetuple())

# takes nix time and returns date
def date_str(nix_time):
    return datetime.datetime.fromtimestamp(nix_time).strftime('%m, %d, %Y')

# return formatted TradesHistory request data
def req(start, end, ofs):
    req_data = {'type': 'all',
                'trades': 'true',
                'start': str(date_nix(start)),
                'end': str(date_nix(end)),
                'ofs': str(ofs)
                }
    return req_data



def reqLedger(start, end, ofs):
    req_data = {'aclass': 'currency',
                'asset': 'all',
                'type':'all',
                'start': str(date_nix(start)),
                'end': str(date_nix(end)),
                'ofs': str(ofs)
                }
    return req_data
k = krakenex.API()
k.load_key('kraken.key')

data = []
start_date = datetime.datetime(2021,1, 1)
end_date = datetime.datetime(2021, 3, 31)
th = k.query_private('Ledgers', reqLedger(start_date, end_date, 1))
x=1

#    if int(th['result']['count'])>0:
#        count += th['result']['count']
        #data.append(pd.DataFrame.from_dict(th['result']['trades']).transpose())

#trades = pd.DataFrame
#trades = pd.concat(data, axis = 0)
#trades = trades.sort(columns='time', ascending=True)
#trades.to_csv('data.csv')
