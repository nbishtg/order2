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
import ordersender
sys.path.append("/root/new/algos")
from expirytools import getCurrentExpiry,getNextExpiry
from algoPosition import saveAlgoposition
from priceFinder import getSym,getSymbyPrice



qtlimit={"NIFTY":1800,"BANKNIFTY":900,"FINNIFTY":1800,"MIDCPNIFTY":2700,"SENSEX":600}


redisconn = Redis(host="localhost", port= 6379,decode_responses=True)

configReader = ConfigParser()
configReader.read('/root/new/order2/config.ini')

hedgePrice={"NIFTY":1,"BANKNIFTY":10,"SENSEX":7}
clientList = json.loads(configReader.get('clientDetails', r'clientList'))
proClients = json.loads(configReader.get('ProClients', r'clientList'))
qtlimit={"NIFTY":1800,"BANKNIFTY":900,"FINNIFTY":1800,"MIDCPNIFTY":2700,"SENSEX":600}

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

def date_expformat(currentTime):
        t=datetime.datetime.fromtimestamp(currentTime)
        #print(str(t.day)+str(t.month)+str(t.year))
        monthMap = {'JAN':1, 'FEB':2,'MAR':3, 'APR':4, 
                            'MAY':5, 'JUN':6, 'JUL':7, 'AUG':8, 'SEP':9,'OCT':10,'NOV':11,'DEC':12}
        mon_keys= list(monthMap.keys())
        mon_value= list(monthMap.values())
        day_str= str(t.day)
        if len(day_str)==1:
            day_str= str(0)+ day_str
        mon_str=mon_keys[mon_value.index(t.month)]
        year_str= str(t.year)[2:]
        return (str(day_str+mon_str+year_str))   

def getHedgeSymbol(clientID,indexName,strikeDist,hedgeSide,maxLoss):
    df=pd.read_csv(f"/root/new/order2/openPosition_{clientID}.csv",index_col=0) 
    
    df = df[(df["symbol"].str.startswith(indexName)) & (date_expformat(time.time()) not in df["symbol"])]
    df.reset_index(inplace=True,drop=True)
   
    if not df.empty:
        df = df.groupby(['symbol']).sum(numeric_only=True) 
        df= df.reset_index()
        
        for i ,r in df.iterrows():
            if r["symbol"].startswith("NIFTY"):
                lt=12
            elif r["symbol"].startswith("BANKNIFTY"):
                lt=16
            elif r["symbol"].startswith("SENSEX"):
                lt=13
            else:
                continue
            df.loc[i,"symWithExpiry"]= r["symbol"][:lt]
            df.loc[i,"strike"]= int(r["symbol"][lt:-2])
            df.loc[i,"symSide"]= r["symbol"][-2:]
       
        df["strike"]=df["strike"].astype(int)
        # print(df)
        qt_CE=0
        qt_PE=0
        strikeCE={}
        strikePE={}
        

        for i, r in df.iterrows():       
            if r["symSide"]=="CE":
                if r["quantity"]<=0:   
                    qt_CE+= abs(r["quantity"])      
                    strikeCE[r["strike"]] = abs(r["quantity"]) 
                    ltp=float(redisconn.get(r['symbol'])) 
                    maxLoss+= (ltp* abs(r["quantity"]))
            elif r["symSide"]=="PE":  
                if r["quantity"]<=0:     
                    qt_PE+= abs(r["quantity"])             
                    strikePE[r["strike"]] = abs(r["quantity"])
                    ltp=float(redisconn.get(r['symbol'])) 
                    maxLoss+= (ltp* abs(r["quantity"]))
                    
        print(maxLoss)      
        print(clientID)
        print(f"unhedgedCE  {strikeCE}")
        print(f"unhedgedPE  {strikePE}")  
        if strikeCE!={}:
            startStrike= min(strikeCE)
            
            for i in range(startStrike,startStrike+30*strikeDist,strikeDist):
                loss=0
                for s in strikeCE.keys():
                    if i>s:
                        loss+=(i-s)*strikeCE[s]
                if loss>=maxLoss:
                    hedgeStrike=i
                    break
             
            if date_expformat(time.time())!=getCurrentExpiry(indexName):
                expiry = getCurrentExpiry(indexName)
            if date_expformat(time.time())==getCurrentExpiry(indexName):
                expiry = getNextExpiry(indexName)
            symWithExpiry=indexName+expiry
            
            for i in range(10):
                try:
                    callSym= symWithExpiry + str(hedgeStrike)+"CE"
                    ltp=float(redisconn.get(callSym)) 
                    break
                except:      
                    logging.error(f"data not found for strike {hedgeSide}, shifting strike inward")
                    hedgeStrike-=strikeDist
            print(callSym)
            print(qt_CE)
            
            tradeList=[]
            quantity= qt_CE 
            if quantity>qtlimit[indexName]:
                for i in range(10):
                    if quantity>qtlimit[indexName]:
                        tradeList.append(qtlimit[indexName])
                        quantity-=qtlimit[indexName]
                    else:
                        tradeList.append(quantity)
                        break
            else:
                tradeList=[quantity]
            
            side="BUY"
            orders=[]
            for i in tradeList: 
                orders.append({"symbol":callSym,"orderSide":side,"quantity":i,"limitPrice":0,"ltp":0,"algoName":"z"})
                
            print(orders)
            z=input(f"do you want to place mismatch Orders {clientID} ? (y/n)")
            if z=="y":
                placeOrders(clientID,orders)
            
def placeOrders(clientID,orders):
    logID= int(str(int(time.time())) + str(random.randint(100000,1000000)))
    print(logID)
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

clientMaxloss= {"PRO14":1500000,"PRO52":250000,"PRO1609":600000,"RGURU1307":120000,"ITC2544":200000,"ANUBHA1201":120000,"ANUBHA1202":120000}
if __name__ == "__main__": 
    for clientID in ["PRO14"]:
        getHedgeSymbol(clientID=clientID,indexName="NIFTY",strikeDist=50,hedgeSide="CE",maxLoss=clientMaxloss[clientID])