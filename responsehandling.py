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
from addposition import updatePosition
import retryOrder

todaydate = datetime.date.today()


redisconn= Redis(host="localhost", port= 6379,decode_responses=False)

logFileName = f'./responselogdata/'
try:
    if not os.path.exists(logFileName):
        os.makedirs(logFileName)
except Exception as e:
    print(e)  

humanTime= datetime.datetime.now()
todaydate=str(humanTime.date())
todaydate=todaydate.replace(':', '')
logFileName+=f'{todaydate}_logfile.log'
            
logging.basicConfig(level=logging.DEBUG, filename=logFileName,
        format="[%(levelname)s]: %(message)s")

orderStatusKey= f"order_status_{todaydate}"

while True:
    order_response=redisconn.lpop("orderResponse2")
    if order_response:
        order_response=json.loads(order_response)
        if order_response['GeneratedBy']== 'TWSAPI':
            logging.info(f"{datetime.datetime.now()} {order_response}")
            
            if order_response['OrderStatus'] in ['PendingNew','New','PendingReplace','Replaced']:
                continue
            
            if order_response['OrderStatus']=='PartiallyFilled':
                logging.info(f"order Partially filled")
                continue
            raw_order=None
            try:
                key= order_response['ClientID'] + "_" + str(order_response['AppOrderID'])
                raw_order = json.loads(redisconn.get(key))
            except Exception as e:
                logging.error(e)
                    
            if not raw_order:
                logging.info(f"{datetime.datetime.now()} order_id {order_response['AppOrderID']} not found in redis")
                # if time.time()-raw_order['orderSentTime']<50:
                logging.info(f'{datetime.datetime.now()} Pushing order back to redis key -> orderResponse2')
                time.sleep(0.1)
                redisconn.rpush("orderResponse2",json.dumps(order_response))
                continue
            
            if raw_order["filled"]!=True: 
                if order_response['OrderStatus']=='Filled':
                    order_response['OrderAverageTradedPrice']=order_response['OrderAverageTradedPrice'].replace(",","")
                    try:
                        filledPrice= float(order_response['OrderAverageTradedPrice'])
                    except Exception as e:
                        logging.error(e)
                        filledPrice= order_response['OrderAverageTradedPrice']
                    raw_order['filled']= True
                    raw_order['filledPrice']= filledPrice
                    try:
                        raw_order['OrderGeneratedDateTime'] = order_response['OrderGeneratedDateTime']  
                        raw_order['LastUpdateDateTime']     = order_response['LastUpdateDateTime']
                    except Exception as e:
                        logging.error(e)
                        raw_order['OrderGeneratedDateTime'] = 0
                        raw_order['LastUpdateDateTime']     = 0
                    key = raw_order["clientID"] + "_" + str(raw_order["order_id"])
                    redisconn.hset(orderStatusKey, key, "1")
                    redisconn.expire(orderStatusKey, 30000)
                    redisconn.set(key,json.dumps(raw_order))
                    redisconn.rpush("processed_orders",json.dumps(raw_order))
                    logging.info(f"{datetime.datetime.now()} order filled, setting filled =True order_id {order_response['AppOrderID']}")
                    updatePosition(algoName=raw_order["algoName"],symbol=raw_order["symbol"],quantity=raw_order["quantity"],action=raw_order["orderSide"],clientID=raw_order['clientID'])         
                elif order_response['OrderStatus'] in ['Cancelled','Rejected']:
                    try:
                        if order_response['OrderStatus']=='Cancelled':
                            if "The order could have resulted in self trade" in order_response['CancelRejectReason']: 
                                if int(order_response['CumulativeQuantity']) != 0:
                                    order_response['OrderAverageTradedPrice']=order_response['OrderAverageTradedPrice'].replace(",","")
                                    filledPrice= float(order_response['OrderAverageTradedPrice'])
                                    filledQuantity= int(order_response['CumulativeQuantity'])
                                    LeavesQuantity=int(order_response['LeavesQuantity'])
                                    if (filledQuantity+ LeavesQuantity)!= raw_order["quantity"]:
                                        logging.error(f"{datetime.datetime.now()} quntity not equal to as initial order")
                                    raw_order['filled']= True
                                    raw_order['filledPrice']= filledPrice
                                    raw_order['quantity']= filledQuantity
                                    try:
                                        raw_order['OrderGeneratedDateTime'] = order_response['OrderGeneratedDateTime']  
                                        raw_order['LastUpdateDateTime']     = order_response['LastUpdateDateTime']
                                    except Exception as e:
                                        logging.error(e)
                                        raw_order['OrderGeneratedDateTime'] = 0
                                        raw_order['LastUpdateDateTime']     = 0
                                    key = raw_order["clientID"] + "_" + str(raw_order["order_id"])
                                    redisconn.hset(orderStatusKey, key, "1")
                                    redisconn.expire(orderStatusKey, 30000)
                                    redisconn.set(key,json.dumps(raw_order))
                                    redisconn.rpush("processed_orders",json.dumps(raw_order))
                                    logging.error(f"{datetime.datetime.now()} order partially filled due to {order_response['CancelRejectReason']}, updating quantity and setting filled =True order_id {order_response['AppOrderID']}")
                                    updatePosition(algoName=raw_order["algoName"],symbol=raw_order["symbol"],quantity=filledQuantity,action=raw_order["orderSide"],clientID=raw_order['clientID'])  
                                    if LeavesQuantity!=0:
                                        order={'symbol': raw_order['symbol'], 'orderSide': raw_order['orderSide'], 'quantity': LeavesQuantity, 'limitPrice': 0, 'ltp': 0, 'algoName': raw_order['algoName']}
                                        p=None
                                        p = mp.Process(target=retryOrder.retry, args=(order,raw_order["clientID"]))
                                        p.start()  
                                        logging.error(f"{datetime.datetime.now()} {raw_order['clientID']} remaining quantity retry sent,{order}")
                                    continue
                                else:
                                    LeavesQuantity=int(order_response['LeavesQuantity'])
                                    if LeavesQuantity!=raw_order["quantity"]:
                                        logging.error("Qt not same as leavesQt")
                                    if LeavesQuantity!=0:
                                        order={'symbol': raw_order['symbol'], 'orderSide': raw_order['orderSide'], 'quantity': LeavesQuantity, 'limitPrice': 0, 'ltp': 0, 'algoName': raw_order['algoName']}
                                        p=None
                                        p = mp.Process(target=retryOrder.retry, args=(order,raw_order["clientID"]))
                                        p.start()  
                                        logging.error(f"{datetime.datetime.now()} {raw_order['clientID']} remaining sameOrder,{order}")
                    except Exception as e:
                        logging.error(e)
                    raw_order['filled']= True
                    raw_order['filledPrice']= 0     
                    raw_order['OrderGeneratedDateTime'] = 0
                    raw_order['LastUpdateDateTime']     = 0    
                    
                    key = raw_order["clientID"] + "_" + str(raw_order["order_id"])
                    redisconn.hset(orderStatusKey, key, "1")
                    redisconn.expire(orderStatusKey, 30000)
                    redisconn.set(key,json.dumps(raw_order))
                    redisconn.rpush("processed_orders",json.dumps(raw_order))
                    logging.error(f"{datetime.datetime.now()} order cancelled due to {order_response['CancelRejectReason']}, setting filled =True order_id {order_response['AppOrderID']}")
                    # rejectionList=["The Price is out of the current execution range","Order Price is significantly away from the LTP","Price should be within LowPriceRange and HighPriceRange"]
                    # for i in rejectionList:
                    #     if i in  order_response['CancelRejectReason']:  
                    #         retryOrder={'symbol': raw_order['symbol'], 'orderSide': raw_order['orderSide'], 'quantity': raw_order['quantity'], 'algoName': raw_order['algoName']}
                    #         retryOrder['ltp']=float(redisconn.get(retryOrder['symbol'])) 
                    #         retryOrder['limitPrice']=float(redisconn.get(retryOrder['symbol'])) 
                    #         redisconn.publish(f'algo_order',json.dumps(retryOrder))
                    #         break
                
                else:
                    logging.fatal(f"{datetime.datetime.now()} New  type of orderStatus")
            else:
                logging.fatal(f"duplicate filled response of orderID {order_response['AppOrderID']}")
        else:
            logging.info(f"{datetime.datetime.now()} {order_response}")
            logging.info("Order Response recieved from XTS Terminal")        
        time.sleep(0.005)
    else:
        time.sleep(0.1)


