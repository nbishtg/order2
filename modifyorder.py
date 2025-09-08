
import numpy as np
import pandas as pd
import time
from Connect import XTSConnect
from Connect2 import XTSConnect2
import time
from redis import Redis
import json
import logging
import datetime
import os
import multiprocessing as mp
from pymongo import MongoClient
from configparser import ConfigParser

todaydate = datetime.date.today()
modifiedOrdersKey=  f"modifiedOrders_{todaydate}"

def initialResponse(clientID,order_id,xt,z):
    orderQuantity = z['quantity']
    if z['clientID'] in ["PRO1609","ITC2544"] and z["symbol"].startswith("SENSEX"):
        ltp=float(redisconn.get(z['symbol'])) 
        modifiedPrice=getLimitPrice(ltp=ltp,orderSide=z['orderSide'])
        logging.info(f'{datetime.datetime.now()} modifying order_id {z["order_id"]} with LimitPrice @{modifiedPrice}')
        modify_response=modifyorderLimit(order_id=z["order_id"],orderQuantity=orderQuantity,modifiedPrice=modifiedPrice,clientID=clientID,xt=xt)
        limitPrice=modifiedPrice
    else:
        logging.info(f'{datetime.datetime.now()} modifying order_id {z["order_id"]} with MarketPrice')
        modify_response=modifyorderMarket(order_id=z["order_id"],orderQuantity=orderQuantity,clientID=clientID,xt=xt)
        limitPrice='MARKET'
    
    # logging.info(f'{datetime.datetime.now()} modifying order_id {z["order_id"]} with MarketPrice')
    # modify_response=modifyorderMarket(order_id=z["order_id"],orderQuantity=orderQuantity,clientID=clientID,xt=xt)
    
    if modify_response['type']=='success':
        z["modifyCount"]=z["modifyCount"]+1
        z["orderSentTime"]=time.time()
        redisconn.hset(modifiedOrdersKey,f"{clientID}_{order_id}",json.dumps({"modifiedPrice":limitPrice,"modifyCount":z["modifyCount"]}))
        redisconn.expire(modifiedOrdersKey, 30000)
        if limitPrice!="MARKET":
            if z["modifyCount"]==5:
                logging.critical("")
        logging.info(f"{datetime.datetime.now()} {modify_response}")
        logging.info(f'{datetime.datetime.now()} order_id {z["order_id"]} => {limitPrice},  modified sucessfully ') 
        if limitPrice!="MARKET":
            if z["modifyCount"]>=5:
                logging.critical(f'modified order_id {z["order_id"]} 5 times')
            else:
                redisconn.rpush(f"modifyOrder2_{clientID}",json.dumps(z))
                logging.info(f'{datetime.datetime.now()} order_id {z["order_id"]} pushed back to redisKey modifyOrder2_{clientID} ') 
        # orderDB.update_one({'order_id': order_id,'clientID':z['clientID']},
        #                 {"$set": {"limitPrice":limitPrice,"orderSentTime": time.time(),"modifyCount":1}})                
        logging.info("..")
    elif modify_response['type']=='error':
        try:             
            if 'is not found in OpenOrder List' in modify_response['description']:
                logging.info(f"{datetime.datetime.now()} order_id {order_id} filled => is not found in OpenOrder List , Removing from modifyOrder queue =>  modify_response {modify_response}") 
            else:
                logging.error(f"{modify_response}")
        except Exception as e:
            logging.error(e)
        logging.info("..")                 

def modifyorderMarket(order_id,orderQuantity,clientID,xt):
   
    """Modify Order Request"""
    z = xt.modify_order(
        appOrderID=order_id,
        modifiedProductType='NRML',
        modifiedOrderType='MARKET',
        modifiedOrderQuantity=orderQuantity,
        modifiedDisclosedQuantity=0,
        modifiedLimitPrice=0,
        modifiedStopPrice=0,
        modifiedTimeInForce='DAY',
        orderUniqueIdentifier='4867',
        clientID=clientID
    )
    return(z)

def modifyorderLimit(order_id,orderQuantity,modifiedPrice,clientID,xt):
   
    """Modify Order Request"""
    z = xt.modify_order(
        appOrderID=order_id,
        modifiedProductType='NRML',
        modifiedOrderType='LIMIT',
        modifiedOrderQuantity=orderQuantity,
        modifiedDisclosedQuantity=0,
        modifiedLimitPrice=modifiedPrice,
        modifiedStopPrice=0,
        modifiedTimeInForce='DAY',
        orderUniqueIdentifier='4867',
        clientID=clientID
    )
    return(z)

def getLimitPrice(ltp,orderSide):
    if ltp>=50:
        limitPriceExtra = ltp*0.10
    elif 50>ltp>=30:
        limitPriceExtra=ltp*0.15
    elif 30>ltp>=10:
        limitPriceExtra=ltp*0.2
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

configReader = ConfigParser()
configReader.read('config.ini')

todaydate = datetime.date.today()

redisconn= Redis(host="localhost", port= 6379,decode_responses=False)
proClients = json.loads(configReader.get('ProClients', r'clientList'))
orderStatusKey= f"order_status_{todaydate}"


def modifyloop(clientID):
    todaydate = datetime.date.today()

    logFileName = f'./modifylogdata/{todaydate}/'
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
    processNO=1
    processList={1:mp.Process() , 2:mp.Process() , 3:mp.Process() , 4:mp.Process()}
    while True: 
        z=redisconn.lpop(f"modifyOrder2_{clientID}")
        if z:
            z=json.loads(z)    
            if z["orderSentTime"] < (time.time()-1):
                order_id=z['order_id']
                raw_order=None        
                try:
                    key=z['clientID'] + "_" + str(order_id)
                    raw_order={}
                    if redisconn.hget(orderStatusKey, key).decode()=="1":  
                        raw_order["filled"]=True
                    else:
                        raw_order["filled"]=False
                except Exception as e:
                    logging.error(e)
                
                if raw_order: 
                    if raw_order["filled"]==True:
                        logging.info(f"{datetime.datetime.now()} order_id {order_id} filled => LimitPrice , Removing from modifyOrder queue")
                        logging.info("..")
                    elif raw_order["filled"]==False: 
                        while True:
                            p=processList[processNO]
                            if not p.is_alive():
                                p = mp.Process(target=initialResponse, args=(clientID,order_id,xt,z,))
                                p.start() 
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
            else:
                redisconn.rpush(f"modifyOrder2_{clientID}",json.dumps(z))
            time.sleep(0.01) 
        else:
            time.sleep(0.1)  
            
            
if __name__ == "__main__":
    clientList = json.loads(configReader.get('clientDetails', r'clientList'))
    for clientID in clientList:
        p = mp.Process(target=modifyloop, args=(clientID,))
        p.start()         
        
        
        
        
        
        
        
        
        

            