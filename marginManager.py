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
import threading
import random
import sys
sys.path.append("/root/new/algos")
from expirytools import getCurrentExpiry,getNextExpiry
from algoPosition import saveAlgoposition
from priceFinder import getSym,getSymbyPrice
import ordersender



redisconn = Redis(host="localhost", port= 6379,decode_responses=True)

configReader = ConfigParser()
configReader.read('/root/new/order2/config.ini')

hedgePrice={"NIFTY":2,"BANKNIFTY":10}
clientList = json.loads(configReader.get('clientDetails', r'clientList'))
proClients = json.loads(configReader.get('ProClients', r'clientList'))

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

df= pd.DataFrame(columns=["clientID","totalMargin","marginUtilized","marginAvailable","percent_marginUtilized"])
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
        try:
            z= xt.get_balance(clientID=clientID)
        
            if z["type"]=="success":
                totalMargin=float(z["result"]["BalanceList"][0]['limitObject']['RMSSubLimits']['cashAvailable'])
                marginUtilized=float(z["result"]["BalanceList"][0]['limitObject']['RMSSubLimits']['marginUtilized'])
                marginAvailable=float(z["result"]["BalanceList"][0]['limitObject']['RMSSubLimits']['netMarginAvailable'])
                percent_marginUtilized=round(marginUtilized*100/totalMargin,2)
                # print(totalMargin)
                # print(marginUtilized)
                # print(marginAvailable)
                # print(percent_marginUtilized)
                df.loc[len(df)]= [clientID] + [totalMargin] + [marginUtilized] + [marginAvailable] + [percent_marginUtilized] 
                      
            else:
                print(z)
        except Exception as e:
            print(e)
df['totalMargin'] = df['totalMargin'].apply(lambda x: f"{x:,.2f}")
df['marginUtilized'] = df['marginUtilized'].apply(lambda x: f"{x:,.2f}")
df['marginAvailable'] = df['marginAvailable'].apply(lambda x: f"{x:,.2f}")     
print(df)     
