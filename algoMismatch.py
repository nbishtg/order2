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
        limitPriceExtra = ltp*0.1
    elif 50>ltp>=30:
        limitPriceExtra=ltp*0.15
    elif 30>ltp>=10:
        limitPriceExtra=ltp*0.2
    else:
        limitPriceExtra=ltp*0.25
        
    if orderSide=="BUY":
        limitPrice = ltp + limitPriceExtra
    elif orderSide=="SELL":
        limitPrice = ltp - limitPriceExtra
    limitPrice=round(limitPrice,1)
    if limitPrice<=0:
        limitPrice=0.05
        return limitPrice
    return limitPrice


redisconn= Redis(host="localhost", port= 6379,decode_responses=True)

configReader = ConfigParser()
configReader.read('config.ini')

qtlimit={"NIFTY":1800,"BANKNIFTY":900,"FINNIFTY":1800,"MIDCPNIFTY":2700,"SENSEX":400}
clientList = json.loads(configReader.get('clientDetails', r'clientList'))
proClients = json.loads(configReader.get('ProClients', r'clientList'))
items = os.listdir("/root/new/algos/algoPositions")
print(items)

# print(df1)


def mismatch(clientID):
    algoMutiple  = json.loads(configReader.get('AllowedAlgos', f'{clientID}'))
    
    df1= pd.DataFrame(columns=["algoName","Symbol","Quantity"])
    for algoName in items:
        try:
            if algoName[:-4] in algoMutiple.keys():
                algoOpenPosition=pd.read_csv(f"/root/new/algos/algoPositions/{algoName}")  
                algoOpenPosition['Quantity'] = algoOpenPosition.apply(lambda row: -abs(row['Quantity']) if row['PositionStatus'] == -1 else row['Quantity'], axis=1)
                algoOpenPosition=algoOpenPosition[['Symbol','Quantity']] 
                algoOpenPosition = algoOpenPosition.groupby(['Symbol']).sum(numeric_only=True)   
                algoOpenPosition["algoName"]= algoName[:-4]
                algoOpenPosition.reset_index(inplace=True)
                # print(algoOpenPosition)
                df1=pd.concat([df1,algoOpenPosition]) 
        except Exception as e:
            print(e)
    
    df1.rename(columns={'Symbol': 'symbol', 'Quantity': 'quantity'}, inplace=True)
    df1.reset_index(inplace=True,drop=True)
    if not df1.empty:
        df1['quantity'] = df1.apply(lambda row: row['quantity']*algoMutiple[row['algoName']], axis=1)
        df1= df1[df1['quantity'] != 0]
        df1.reset_index(inplace=True,drop=True)
    # print(df1)

    df2= pd.read_csv(f"openPosition_{clientID}.csv",index_col=0)
    df2.reset_index(inplace=True, drop=True)
    df2=df2[['algoName','symbol','quantity']] 
    
    df2= df2[df2['quantity'] != 0]
    df2= df2[df2['algoName']!="z"]
    # print(df2)
    df2['quantity'] = pd.to_numeric(df2['quantity'], errors='coerce')
    df2= df2.groupby(['algoName','symbol']).sum(numeric_only=True)
    
    df2.reset_index(inplace=True)
    # print(df2)

    df=pd.merge(df1,df2,suffixes=["_algos","_database"],on=["symbol",'algoName'],how="outer")
    df['quantity_algos'] = df['quantity_algos'].fillna(0)
    df['quantity_database'] = df['quantity_database'].fillna(0)
    
    mismatchOrder=[]
    if not df.empty:
            for index ,row in df.iterrows():
                    if row["quantity_algos"] == row["quantity_database"]:
                            df.at[index, 'MATCH'] = "match"
                    else:
                            df.at[index, 'MATCH'] = "mis-match"   
                            try:
                                if  row["quantity_algos"]  > row["quantity_database"]:
                                    orderSide="BUY"
                                    quantity= int(row["quantity_algos"] - row["quantity_database"])
                                else:
                                    orderSide="SELL"
                                    quantity= int(row["quantity_database"] - row["quantity_algos"])
                                symbol= row["symbol"]
                                algoName= row["algoName"]
                                order= {'symbol':symbol, 'orderSide':orderSide , 'quantity': quantity, 'limitPrice': 0, 'ltp': 0, 'algoName': algoName}
                                tradeList=[]
                                quantity= abs(order["quantity"])  
                                if order["symbol"].startswith("NIFTY"):
                                    baseSym="NIFTY"
                                elif order["symbol"].startswith("BANKNIFTY"):
                                    baseSym="BANKNIFTY"
                                elif order["symbol"].startswith("FINNIFTY"):
                                    baseSym="FINNIFTY"
                                elif order["symbol"].startswith("MIDCPNIFTY"):
                                    baseSym="MIDCPNIFTY"
                                elif order["symbol"].startswith("SENSEX"):
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
                                print(tradeList)
                                for qt in tradeList:
                                    new_order=order.copy()
                                    new_order["quantity"]=qt          
                                    mismatchOrder.append(new_order)
                                    print(mismatchOrder)
                            except Exception as e:
                                print(e) 
    print(clientID)
    print(df.to_string())
    return mismatchOrder

def placeorder(mismatchOrder):
    if mismatchOrder!=[]:
        print(mismatchOrder)
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
    
        z=input(f"do you want to place mismatch Orders {clientID} ? (y/n)")
        if z=="y":
            logID=0
            for order in mismatchOrder:
                order['ltp']=float(redisconn.get(order['symbol'])) 
                order['limitPrice']=getLimitPrice(ltp=order['ltp'],orderSide=order['orderSide'])
                
                order["clientID"] =clientID 
                print(order)
                p=None
                p = mp.Process(target=ordersender.initialResponse, args=(order,xt,logID))
                p.start()  
                time.sleep(0.15)
                logID+=1 
             
    # print(clientID)
    # print(df.to_string())
if __name__ == "__main__":  
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
      
    for clientID in clientList:
        print(clientID)
        mismatchOrder=mismatch(clientID)
        if mismatchOrder!=[]:
            placeorder(mismatchOrder)
    