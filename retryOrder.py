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

# logFileName = f'./squareofflog/'
# try:
#     if not os.path.exists(logFileName):
#         os.makedirs(logFileName)
# except Exception as e:
#     print(e)  



# humanTime= datetime.datetime.now()
# todaydate=str(humanTime.date())
# todaydate=todaydate.replace(':', '')
# logFileName+=f'{todaydate}.log'
            
# logging.basicConfig(level=logging.DEBUG, filename=logFileName,
#         format="[%(levelname)s]: %(message)s")

redisconn= Redis(host="localhost", port= 6379,decode_responses=True)

configReader = ConfigParser()
configReader.read('config.ini')

qtlimit={"NIFTY":1800,"BANKNIFTY":900,"FINNIFTY":1800,"MIDCPNIFTY":2700}

proClients = json.loads(configReader.get('ProClients', r'clientList'))

def retry(order, clientID):
    time.sleep(1)
    logID=1
    clientAPidetails= json.loads(configReader.get('interactiveAPIcred', f'{clientID}'))
    API_KEY    = clientAPidetails['API_KEY']
    API_SECRET = clientAPidetails['API_SECRET']
    source     = clientAPidetails['source']

    if clientID in ["PRO1609","ITC2544"]:
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
  
    order['ltp']=float(redisconn.get(order['symbol'])) 
    order['limitPrice']=getLimitPrice(ltp=order['ltp'],orderSide=order['orderSide'])
    
    order["clientID"] =clientID 
    print(order)
    p=None
    p = mp.Process(target=ordersender.initialResponse, args=(order,xt,logID))
    p.start()  
    time.sleep(0.05)
    logID+=1
                
