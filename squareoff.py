import numpy as np
import pandas as pd
import time
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

qtlimit={"NIFTY":1800,"BANKNIFTY":900,"FINNIFTY":1800,"MIDCPNIFTY":2700,"SENSEX":600}
clientList = json.loads(configReader.get('clientDetails', r'clientList'))
# clientList=["RGURU1307","RGURU1333","ANUBHA1201","PRO14","KB37","PRO1609"]
# algoList= ["BT_N" ,"chainsell_N"  ,"chainsell_BIB_N"  ,"chainsell_buy_N" ,"e5_1_N"  ,"e5_2_N","REO_N", "MOZ_N","Hopper15_N","Hopper3S_N"]
algoList= ["z"]
logID=1

proClients = json.loads(configReader.get('ProClients', r'clientList'))
for clientID in clientList:
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
    df=pd.read_csv(f"openPosition_{clientID}.csv")
    df.reset_index(inplace=True,drop=True)
    for index, row in df.iterrows():
        if row["algoName"] in algoList:
            symbol=row["symbol"]
            if row["quantity"]<0:
                side="BUY"
            elif row["quantity"]>0:
                side="SELL"
            ltp=1
            ltp=float(redisconn.get(symbol))
            if side=="BUY":
                limitPriceExtra = round(ltp*0.05, 1)
            else:
                limitPriceExtra = -round(ltp*0.05, 1)
            limitPrice= ltp + limitPriceExtra
            
            tradeList=[]
            quantity= abs(row["quantity"])  
            if row["symbol"].startswith("NIFTY"):
                baseSym="NIFTY"
            elif row["symbol"].startswith("BANKNIFTY"):
                baseSym="BANKNIFTY"
            elif row["symbol"].startswith("FINNIFTY"):
                baseSym="FINNIFTY"
            elif row["symbol"].startswith("MIDCPNIFTY"):
                baseSym="MIDCPNIFTY"
            elif row["symbol"].startswith("SENSEX"):
                baseSym="SENSEX"
            
            if quantity>qtlimit[baseSym]:
                for i in range(10):
                    if quantity>qtlimit[baseSym]:
                        tradeList.append(qtlimit[baseSym])
                        quantity-=qtlimit[baseSym]
                    else:
                        tradeList.append(quantity)
                        break
            else:
                tradeList=[quantity]
            for i in tradeList: 
                order={"symbol":symbol,"orderSide":side,"quantity":i,"limitPrice":limitPrice,"ltp":ltp,"algoName":row["algoName"]}
                order["clientID"] =clientID 
                print(order)
                logging.info(f"{logID} {datetime.datetime.now()}: {order}")
                p=None
                p = mp.Process(target=ordersender.initialResponse, args=(order,xt,logID))
                p.start()  
                time.sleep(0.1)
                logID+=1
                
                
            
