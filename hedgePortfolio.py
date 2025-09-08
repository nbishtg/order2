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

qtlimit={"NIFTY":1800,"BANKNIFTY":900,"FINNIFTY":1800,"MIDCPNIFTY":2700,"SENSEX":600}


redisconn = Redis(host="localhost", port= 6379,decode_responses=True)

configReader = ConfigParser()
configReader.read('/root/new/order2/config.ini')

hedgePrice={"NIFTY":1,"BANKNIFTY":10,"SENSEX":7}
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

def getOrders(clientID):
    df=pd.read_csv(f"/root/new/order2/openPosition_{clientID}.csv",index_col=0)    
    if not df.empty:
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
        unique_symWithExiry= df["symWithExpiry"].unique()
        
        unhedgedCE={}
        unhedgedPE={}
        for i in unique_symWithExiry:
            unhedgedCE[i]=0
            unhedgedPE[i]=0

        for i, r in df.iterrows():       
            if r["symSide"]=="CE":
                if r["quantity"]<=0:
                    unhedgedCE[r["symWithExpiry"]] += abs(r["quantity"])
                else:
                    unhedgedCE[r["symWithExpiry"]] -= abs(r["quantity"])
            elif r["symSide"]=="PE":  
                if r["quantity"]<=0:
                    unhedgedPE[r["symWithExpiry"]] += abs(r["quantity"])
                else:
                    unhedgedPE[r["symWithExpiry"]] -= abs(r["quantity"])
        print(clientID)
        print(f"unhedgedCE {unhedgedCE}")
        print(f"unhedgedPE {unhedgedPE}")   

        orders=[]   
        for symWithExpiry in unhedgedCE.keys():
            if  unhedgedCE[symWithExpiry]>0:    
                if symWithExpiry.startswith("NIFTY"):
                    price=hedgePrice["NIFTY"]
                    baseSym="NIFTY"
                elif symWithExpiry.startswith("BANKNIFTY"):
                    price=hedgePrice["BANKNIFTY"]
                    baseSym="BANKNIFTY"
                elif symWithExpiry.startswith("SENSEX"):
                    price=hedgePrice["SENSEX"]
                    baseSym="SENSEX"
                else:
                    continue
                try:
                    callSym=getSymbyPrice(symSide="CE",symWithExpiry=symWithExpiry,priceReq=price,lesser_grater="grater")
                    print(callSym)
                    print(float(redisconn.get(callSym)))
                    tradeList=[]
                    quantity=abs(unhedgedCE[symWithExpiry])
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
                        orders.append({'symbol': callSym, 'orderSide': 'BUY', 'quantity': i, 'limitPrice': 0, 'ltp': 0, 'algoName': 'z'})
                except Exception as e:
                    print(e)

        for symWithExpiry in unhedgedPE.keys():
            if  unhedgedPE[symWithExpiry]>0:    
                if symWithExpiry.startswith("NIFTY"):
                    price=hedgePrice["NIFTY"]
                    baseSym="NIFTY"
                elif symWithExpiry.startswith("BANKNIFTY"):
                    price=hedgePrice["BANKNIFTY"]
                    baseSym="BANKNIFTY"
                elif symWithExpiry.startswith("SENSEX"):
                    price=hedgePrice["SENSEX"]
                    baseSym="SENSEX"
                else:
                    continue
                try:
                    putSym=getSymbyPrice(symSide="PE",symWithExpiry=symWithExpiry,priceReq=price,lesser_grater="grater")
                    print(putSym)
                    print(float(redisconn.get(putSym)))
                    tradeList=[]
                    quantity=abs(unhedgedPE[symWithExpiry])
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
                        orders.append({'symbol': putSym, 'orderSide': 'BUY', 'quantity': i, 'limitPrice': 0, 'ltp': 0, 'algoName': 'z'})
                except Exception as e:
                    print(e)
        return orders
    else:
        return []
    
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



# for clientID in clientList:
#     df=pd.read_csv(f"/root/new/order2/openPosition_{clientID}.csv",index_col=0)    
#     if not df.empty:
#         for i ,r in df.iterrows():
#             if r["symbol"].startswith("NIFTY"):
#                 lt=12
#             elif r["symbol"].startswith("BANKNIFTY"):
#                 lt=16
#             elif r["symbol"].startswith("SENSEX"):
#                 lt=13
#             else:
#                 continue
#             df.loc[i,"symWithExpiry"]= r["symbol"][:lt]
#             df.loc[i,"strike"]= int(r["symbol"][lt:-2])
#             df.loc[i,"symSide"]= r["symbol"][-2:]
       
#         df["strike"]=df["strike"].astype(int)
#         unique_symWithExiry= df["symWithExpiry"].unique()
        
#         unhedgedCE={}
#         unhedgedPE={}
#         for i in unique_symWithExiry:
#             unhedgedCE[i]=0
#             unhedgedPE[i]=0

#         for i, r in df.iterrows():       
#             if r["symSide"]=="CE":
#                 if r["quantity"]<=0:
#                     unhedgedCE[r["symWithExpiry"]] += abs(r["quantity"])
#                 else:
#                     unhedgedCE[r["symWithExpiry"]] -= abs(r["quantity"])
#             elif r["symSide"]=="PE":  
#                 if r["quantity"]<=0:
#                     unhedgedPE[r["symWithExpiry"]] += abs(r["quantity"])
#                 else:
#                     unhedgedPE[r["symWithExpiry"]] -= abs(r["quantity"])
#         print(clientID)
#         print(f"unhedgedCE {unhedgedCE}")
#         print(f"unhedgedPE {unhedgedPE}")   

#         clientAPidetails= json.loads(configReader.get('interactiveAPIcred', f'{clientID}'))
#         API_KEY    = clientAPidetails['API_KEY']
#         API_SECRET = clientAPidetails['API_SECRET']
#         source     = clientAPidetails['source']

#         xt = XTSConnect(API_KEY, API_SECRET, source)

#         with open(f"/root/new/order2/auth/{clientID}.json", "r") as f:
#             auth = json.load(f)

#         set_interactiveToken, set_muserID  = str(auth['result']['token']) , str(auth['result']['userID'])
#         connectionString= str(auth['connectionString'])

#         #set golba variables for Connect.py file
#         xt.token = set_interactiveToken
#         if clientID in proClients:  
#             xt.userID = '*****'
#             xt.isInvestorClient = False
#         else:
#             xt.userID = set_muserID
#             xt.isInvestorClient = True
#         xt.connectionString= connectionString
     
#         orders=[]   
#         for symWithExpiry in unhedgedCE.keys():
#             if  unhedgedCE[symWithExpiry]>0:    
#                 if symWithExpiry.startswith("NIFTY"):
#                     price=hedgePrice["NIFTY"]
#                     baseSym="NIFTY"
#                 elif symWithExpiry.startswith("BANKNIFTY"):
#                     price=hedgePrice["BANKNIFTY"]
#                     baseSym="BANKNIFTY"
#                 elif symWithExpiry.startswith("SENSEX"):
#                     price=hedgePrice["SENSEX"]
#                     baseSym="SENSEX"
#                 else:
#                     continue
#                 try:
#                     callSym=getSymbyPrice(symSide="CE",symWithExpiry=symWithExpiry,priceReq=price,lesser_grater="grater")
#                     print(callSym)
#                     print(float(redisconn.get(callSym)))
#                     tradeList=[]
#                     quantity=abs(unhedgedCE[symWithExpiry])
#                     if quantity>qtlimit[baseSym]:
#                         for i in range(10):
#                             if quantity>qtlimit[baseSym]:
#                                 tradeList.append(qtlimit[baseSym])
#                                 quantity-=qtlimit[baseSym]
#                             else:
#                                 tradeList.append(quantity)
#                                 break
#                     else:
#                         tradeList=[quantity]
#                     for i in tradeList: 
#                         orders.append({'symbol': callSym, 'orderSide': 'BUY', 'quantity': i, 'limitPrice': 0, 'ltp': 0, 'algoName': 'z'})
#                 except Exception as e:
#                     print(e)

#         for symWithExpiry in unhedgedPE.keys():
#             if  unhedgedPE[symWithExpiry]>0:    
#                 if symWithExpiry.startswith("NIFTY"):
#                     price=hedgePrice["NIFTY"]
#                     baseSym="NIFTY"
#                 elif symWithExpiry.startswith("BANKNIFTY"):
#                     price=hedgePrice["BANKNIFTY"]
#                     baseSym="BANKNIFTY"
#                 elif symWithExpiry.startswith("SENSEX"):
#                     price=hedgePrice["SENSEX"]
#                     baseSym="SENSEX"
#                 else:
#                     continue
#                 try:
#                     putSym=getSymbyPrice(symSide="PE",symWithExpiry=symWithExpiry,priceReq=price,lesser_grater="grater")
#                     print(putSym)
#                     print(float(redisconn.get(putSym)))
#                     tradeList=[]
#                     quantity=abs(unhedgedPE[symWithExpiry])
#                     if quantity>qtlimit[baseSym]:
#                         for i in range(10):
#                             if quantity>qtlimit[baseSym]:
#                                 tradeList.append(qtlimit[baseSym])
#                                 quantity-=qtlimit[baseSym]
#                             else:
#                                 tradeList.append(quantity)
#                                 break
#                     else:
#                         tradeList=[quantity]
#                     for i in tradeList: 
#                         orders.append({'symbol': putSym, 'orderSide': 'BUY', 'quantity': i, 'limitPrice': 0, 'ltp': 0, 'algoName': 'z'})
#                 except Exception as e:
#                     print(e)

#         if orders!=[]:
#             print(orders)
#             permission=input(f"place orders for client {clientID} y/n?")
#             if permission =="y":
#                 for order in orders:  
#                     order['ltp']=float(redisconn.get(order['symbol'])) 
#                     order['limitPrice']=getLimitPrice(ltp=order['ltp'],orderSide=order['orderSide'])
                    
#                     order["clientID"] =clientID 
#                     print(order)
#                     p=None
#                     p = mp.Process(target=ordersender.initialResponse, args=(order,xt,logID))
#                     p.start()  
#                     time.sleep(0.05)
#                     logID+=1
        

if __name__ == "__main__":
    for clientID in clientList:
        orders= getOrders(clientID)
        if orders!=[]:
            print(orders) 
            permission=input(f"place orders for client {clientID} y/n?")
            if permission =="y":
                placeOrders(clientID,orders)