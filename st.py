import numpy as np
import pandas as pd
import time
from redis import Redis
import json
import logging
import datetime
import os
import multiprocessing as mp
from pymongo import MongoClient
from configparser import ConfigParser
from Connect import XTSConnect
from Connect2 import XTSConnect2
import ordersender

def getLimitPrice(ltp,orderSide):
    if ltp>=50:
        limitPriceExtra = ltp*0.15
    elif 50>ltp>=30:
        limitPriceExtra=ltp*0.2
    elif 30>ltp>=10:
        limitPriceExtra=ltp*0.3
    else:
        limitPriceExtra=ltp*0.4
        
    if orderSide=="BUY":
        limitPrice = ltp + limitPriceExtra
    elif orderSide=="SELL":
        limitPrice = ltp - limitPriceExtra
    if limitPrice<=0:
        limitPrice=0.05
        return limitPrice
    return round(limitPrice,1)

logFileName = f'./squareofflog/'
try:
    if not os.path.exists(logFileName):
        os.makedirs(logFileName)
except Exception as e:
    print(e)  



humanTime= datetime.datetime.now()
todaydate=str(humanTime.date())
todaydate=todaydate.replace(':', '')
logFileName+=f'{todaydate}.log'
            
logging.basicConfig(level=logging.DEBUG, filename=logFileName,
        format="[%(levelname)s]: %(message)s")

redisconn= Redis(host="localhost", port= 6379,decode_responses=True)

configReader = ConfigParser()
configReader.read('config.ini')

qtlimit={"NIFTY":1800,"BANKNIFTY":900,"FINNIFTY":1800,"MIDCPNIFTY":2700,"SENSEX":500}
clientList=["ANUBHA1201"]

logID=1
proClients = json.loads(configReader.get('ProClients', r'clientList'))
for clientID in clientList:
    clientAPidetails= json.loads(configReader.get('interactiveAPIcred', f'{clientID}'))
    API_KEY    = clientAPidetails['API_KEY']
    API_SECRET = clientAPidetails['API_SECRET']
    source     = clientAPidetails['source']

    if clientID=="PRO1609":
        xt = XTSConnect2(API_KEY, API_SECRET, source)
    else:
        xt = XTSConnect(API_KEY, API_SECRET, source)

    with open(f"/root/new/order2/auth/{clientID}.json", "r") as f:
        auth = json.load(f)

    set_interactiveToken, set_muserID  = str(auth['result']['token']) , str(auth['result']['userID'])
    connectionString= str(auth['connectionString'])

    #set golba variables for Connect.py file
    xt.token = set_interactiveToken
    if clientID in proClients: 
        xt.userID = '*****'
        xt.isInvestorClient = False
    else:
        xt.userID = set_muserID
        xt.isInvestorClient = True
    xt.connectionString= connectionString
# 0  NIFTY24APR2523000PE               -375                NaN  mis-match
# 3  NIFTY24APR2525000CE               -150                NaN  mis-match
    orders= [
             {'symbol': 'NIFTY07AUG2525350CE', 'orderSide': 'SELL', 'quantity':75, 'limitPrice': 2, 'ltp':1.3, 'algoName': 'z'}]
    for order in orders:
        order['ltp']=float(redisconn.get(order['symbol'])) 
        order['limitPrice']=getLimitPrice(ltp=order['ltp'],orderSide=order['orderSide'])
        
        order["clientID"] =clientID 
        print(order)
        p=None
        p = mp.Process(target=ordersender.initialResponse, args=(order,xt,logID))
        p.start()  
        time.sleep(0.05)
        logID+=1
                


# [INFO]: 2025-01-30 14:08:01.260636 {'LoginID': 'DLL13435', 'ClientID': 'PRO756', 'AppOrderID': 1240092665, 'OrderReferenceID': '', 'GeneratedBy': 'TWSAPI', 'ExchangeOrderID': '1300000223565507', 'OrderCategoryType': 'NORMAL', 'ExchangeSegment': 'NSEFO', 'ExchangeInstrumentID': 44235, 'OrderSide': 'Buy', 'OrderType': 'Limit', 'ProductType': 'NRML', 'TimeInForce': 'DAY', 'OrderPrice': 137.9, 'OrderQuantity': 225, 'OrderStopPrice': 0, 'OrderStatus': 'Cancelled', 'OrderAverageTradedPrice': '134.05', 'LeavesQuantity': 150, 'CumulativeQuantity': 75, 'OrderDisclosedQuantity': 0, 'OrderGeneratedDateTime': '2025-01-30T14:08:02.0921673', 'ExchangeTransactTime': '2025-01-30T14:08:00+05:30', 'LastUpdateDateTime': '2025-01-30T14:08:02.1239997', 'OrderExpiryDate': '1980-01-01T00:00:00', 'CancelRejectReason': '17071 : The order could have resulted in self trade ', 'OrderUniqueIdentifier': '4867', 'OrderLegStatus': 'SingleOrderLeg', 'IsSpread': False, 'BoLegDetails': 0, 'BoEntryOrderId': '', 'OrderAverageTradedPriceAPI': 134.05, 'OrderSideAPI': 'BUY', 'OrderGeneratedDateTimeAPI': '30-01-2025 14:08:02', 'ExchangeTransactTimeAPI': '30-01-2025 14:08:00', 'LastUpdateDateTimeAPI': '30-01-2025 14:08:02', 'OrderExpiryDateAPI': '01-01-1980 00:00:00', 'AlgoID': '', 'AlgoCategory': 0, 'MessageSynchronizeUniqueKey': 'DLL13435', 'MessageCode': 9004, 'MessageVersion': 4, 'TokenID': 0, 'ApplicationType': 146, 'SequenceNumber': 1603894589173184, 'TradingSymbol': 'NIFTY 30JAN2025 PE 23250'}
# [ERROR]: 2025-01-30 14:08:01.270392 order cancelled due to 17071 : The order could have resulted in self trade , setting filled =True order_id 1240092665
