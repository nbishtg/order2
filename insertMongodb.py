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


# logFileName = f'./responselogdata/'
# try:
#     if not os.path.exists(logFileName):
#         os.makedirs(logFileName)
# except Exception as e:
#     print(e)  

# humanTime= datetime.datetime.now()
# todaydate=str(humanTime.date())
# todaydate=todaydate.replace(':', '')
# logFileName+=f'{todaydate}_logfile.log'
            
# logging.basicConfig(level=logging.DEBUG, filename=logFileName,
#         format="[%(levelname)s]: %(message)s")

todaydate = datetime.date.today()

client= MongoClient(host="localhost",port=27017,username="nb",password="nb")
orderDB = client["order2"][f"order_{todaydate}"]
redisconn= Redis(host="localhost", port= 6379,decode_responses=False)

modifiedOrdersKey=  f"modifiedOrders_{todaydate}"

while True:
    processed_orders=redisconn.lpop("processed_orders")
    if processed_orders:
        processed_orders=json.loads(processed_orders)
        clientID= processed_orders["clientID"]
        order_id= processed_orders["order_id"]
        z= redisconn.hget(modifiedOrdersKey,f"{clientID}_{order_id}")
        if z:
            z=json.loads(z) 
            processed_orders["limitPrice"] = z["modifiedPrice"]
            processed_orders["modifyCount"]= z["modifyCount"]
        # updatePosition(algoName=processed_orders["algoName"],symbol=processed_orders["symbol"],quantity=processed_orders["quantity"],action=processed_orders["orderSide"],clientID=processed_orders['clientID'])  
        # redisconn.set(f"{clientID}_{order_id}",json.dumps(processed_orders),ex=30000) 
        orderDB.insert_one(processed_orders)
        # logging.info(f"{datetime.datetime.now()} {processed_orders}")
    else:
        time.sleep(0.05)