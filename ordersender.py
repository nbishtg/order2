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


def placeorder(exchangeInstrumentID,orderSide,orderQuantity,limitPrice,symbol,logID,xt):
    
    if symbol.startswith("SENSEX"):
        exchangeSegment="BSEFO"
    else:
        exchangeSegment="NSEFO"

    logging.info(f"{logID} {datetime.datetime.now()} order details sending to broker-> {exchangeInstrumentID} ,{orderSide} , {symbol}  , {limitPrice} , {orderQuantity}")
    orderplace = xt.place_order(exchangeSegment=exchangeSegment,
                                exchangeInstrumentID=exchangeInstrumentID,
                                productType='NRML',
                                orderType='LIMIT',
                                orderSide=orderSide,
                                timeInForce='DAY',
                                disclosedQuantity=0,
                                orderQuantity=orderQuantity,
                                limitPrice=limitPrice,
                                stopPrice=0,
                                orderUniqueIdentifier='4867'
                                )
    logging.info(f"{logID} {datetime.datetime.now()}  Response:{orderplace}")
    return(orderplace)



def rmsCheck(symbol,action,quantity,limitPrice,algoName):
    maxPriceLimit=200
    lotSize=50
    
    if quantity%lotSize==0:
        pass
    else:
        return [False,f"lotSize error {quantity}"]
       
    if action=="BUY" or action=="SELL":
        pass
    else:
        return [False, f"action other than BUY SELL i.e {action}"]
    
    if limitPrice < maxPriceLimit:
        pass
    else:
        return [False, f"limitPrice exceeds maxPricelimit of {maxPriceLimit}"]
    
    return [True, "all checks cleared"]

def getLimitPrice(ltp,orderSide):
    if ltp>=50:
        limitPriceExtra = ltp*0.15
    elif 50>ltp>=30:
        limitPriceExtra=ltp*0.2
    elif 30>ltp>=10:
        limitPriceExtra=ltp*0.3
    else:
        limitPriceExtra=ltp*0.5
        
    if orderSide=="BUY":
        limitPrice = ltp + limitPriceExtra
    elif orderSide=="SELL":
        limitPrice = ltp - limitPriceExtra
    if limitPrice<=0:
        limitPrice=0.05
        return limitPrice
    return round(limitPrice,1)


configReader = ConfigParser()
configReader.read('config.ini')

# ID Mapping [Key:Symbol] [Value:InstrumentId]
with open("/root/new/tickdata/idMapforDay.json", "r") as f:
    idMapforDay = json.load(f)
 
todaydate = datetime.date.today()
redisconn= Redis(host="localhost", port= 6379,decode_responses=True) 
proClients = json.loads(configReader.get('ProClients', r'clientList'))
orderStatusKey= f"order_status_{todaydate}"


def initialResponse(order,xt,logID):
    initial_response=None
    try:
        orderSentTime= time.time()
        initial_response=placeorder(exchangeInstrumentID=str(idMapforDay[order["symbol"]]),
                                    orderSide=order["orderSide"],
                                    orderQuantity=order["quantity"],
                                    limitPrice=order["limitPrice"],
                                    symbol=order["symbol"],
                                    logID=logID,
                                    xt=xt)

        responseDelay= round((time.time() - orderSentTime), 2)
    except Exception as e:
        logging.error(e)
      
    if initial_response:
        logging.info(f"{logID} {datetime.datetime.now()}: initial order response received from broker ,delay by {responseDelay} seconds")
        try:
            if initial_response["type"] == "success":
                # logging.info(f"{logID} {datetime.datetime.now()} {initial_response}")
                order_id= initial_response['result']['AppOrderID'] 
                order["order_id"]     = order_id
                order["filled"]       = False
                order["filledPrice"]  = 0                        
                order["algoOrderTime"]= orderSentTime 
                order["orderSentTime"]= time.time() 
                order["modifyCount"]  = 0
                order["responseDelay"]=responseDelay
                
                key= order["clientID"] + "_" + str(order["order_id"])
                redisconn.hset(orderStatusKey, key, "0")
                redisconn.expire(orderStatusKey, 30000)
                redisconn.set(key,json.dumps(order),ex=30000)         
                logging.info(f"{logID} {datetime.datetime.now()} order Placed sucessfully, setting order to redisKey {key}")
                logging.info("..")
                 
                redisconn.rpush(f"modifyOrder2_{order['clientID']}",json.dumps(order))

            elif initial_response["type"]== "error": 
                logging.error(f"{logID} {datetime.datetime.now()} {initial_response}") 
                
                if "Gateway:Supplied Quantity is not in multiple of LotSize." in initial_response['description']:
                    print("Quantity is not in multiple of LotSize")
                logging.info("..")
            else: 
                logging.error(f"{logID} {datetime.datetime.now()} {initial_response}") 
                logging.info("..")   
        except Exception as e:
            logging.fatal(f"{logID} {datetime.datetime.now()} {initial_response}")  
            logging.fatal(e) 

def ordersenders(clientID):
    logID= int( str(int(time.time()))  + str(random.randint(100000,1000000)) )
    todaydate = datetime.date.today()

    logFileName = f'./orderlogdata/{todaydate}/'
    try:
        if not os.path.exists(logFileName):
            os.makedirs(logFileName)
    except Exception as e:
        print(e)  

       

    humanTime= datetime.datetime.now()
    todaydate=str(humanTime.date())
    todaydate=todaydate.replace(':', '')
    logFileName+=f'{clientID}.log'
                
    logging.basicConfig(level=logging.DEBUG, filename=logFileName,
            format="[%(levelname)s]: %(message)s")


    # interactive API Credentials
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
    
    pubObj = redisconn.pubsub(ignore_subscribe_messages=True)
    pubObj.subscribe("algo_order")

    algoMutiple  = json.loads(configReader.get('AllowedAlgos', f'{clientID}'))
    freezeQuantiy={"NIFTY":1800,"BANKNIFTY":900,"FINNIFTY":1800,"MIDCPNIFTY":2800,"SENSEX":400}    

    processNO=1
    processList={1:mp.Process() , 2:mp.Process() , 3:mp.Process() , 4:mp.Process(), 5:mp.Process(),
                 6:mp.Process() , 7:mp.Process() , 8:mp.Process() , 9:mp.Process(), 10:mp.Process()}
    while True:
        
        order = pubObj.get_message()       
        if order:
            order = json.loads(order['data']) 
        else:
            time.sleep(0.1)
        if order:    
            if order['algoName'] not in algoMutiple.keys(): 
                continue
            if "squareoff" not in order.keys(): 
                if order['algoName'] in algoMutiple.keys(): 
                    order["quantity"]*=algoMutiple[order['algoName']]
                    if order["quantity"]==0:
                        continue
                else:
                    pass
            else:
                del order["squareoff"]
            order["clientID"] =clientID  
            logging.info(f"{logID} {datetime.datetime.now()}: order received from algo: {order['algoName']}, now placing order")
            logging.info(f"{logID} {datetime.datetime.now()} {order}") 
            tradeList=[]    
            qt=order['quantity']
            for baseSym in freezeQuantiy.keys():
                if order['symbol'].startswith(baseSym):
                    if qt> freezeQuantiy[baseSym]:
                        for i in range(10):
                            if qt>freezeQuantiy[baseSym]:
                                tradeList.append(freezeQuantiy[baseSym])
                                qt-=freezeQuantiy[baseSym]
                            else:
                                tradeList.append(qt)
                                break
                    else:
                        tradeList=[qt]
                    break
            logging.info(f"{order['algoName']} {tradeList}")
            for i in tradeList:
                order['quantity']=i
                while True:
                    p=processList[processNO]
                    if not p.is_alive():
                        p = mp.Process(target=initialResponse, args=(order,xt,logID,))
                        p.start() 
                        logID+=1
                        processList[processNO]=p
                        print(f"process {processNO} started")
                        if list(processList.keys())[-1]==processNO:
                            processNO=1
                        else:
                            processNO+=1
                        time.sleep(0.02)
                        break 
                    if list(processList.keys())[-1]==processNO:
                        processNO=1
                    else:
                        processNO+=1
                    time.sleep(0.02)
            # p = mp.Process(target=initialResponse, args=(order,xt,logID,))
            # p.start()      
            # time.sleep(0.02)
            
            

                            
                    
if __name__ == "__main__":
    clientList = json.loads(configReader.get('clientDetails', r'clientList'))
    for clientID in clientList:
        p=None
        p = mp.Process(target=ordersenders, args=(clientID,))
        p.start()                  
                    
            