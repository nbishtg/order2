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
from addposition import updatePosition

redisconn= Redis(host="localhost", port= 6379,decode_responses=True)

logFileName = f'./missingresponselogdata/'
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

configReader = ConfigParser()
configReader.read('config.ini')

todaydate = datetime.date.today()

client= MongoClient(host="localhost",port=27017,username="nb",password="nb")
orderDB = client["order2"][f"order_{todaydate}"]
proClients = json.loads(configReader.get('ProClients', r'clientList'))
orderStatusKey= f"order_status_{todaydate}"

while True:    
        try:
            order_status = redisconn.hgetall(orderStatusKey)
            # Filter fields where the value is '0'
            unfilled_orders = [field for field, value in order_status.items() if value == '0']  
        except Exception as e:
            logging.error(e)
            print(e)
        
        if unfilled_orders:
            for i in unfilled_orders:
                i= json.loads(redisconn.get(i))
                if i["algoOrderTime"]< (time.time()-20):
                    # interactive API Credentials
                    clientID= i["clientID"]
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
                        logging.info(f"{datetime.datetime.now()} checking orderStatus for order_id {i['order_id']}")
                        requestSentTime=time.time()
                        response=xt.get_order_history(appOrderID=i["order_id"])
                        responseDelay=round(time.time()-requestSentTime,2)
                    except Exception as e:
                        logging.error(e)
                        print(e)
                    
                    if response:
                        try:
                            print(response)
                            if response['type']== 'success':             
                                logging.info(f"{datetime.datetime.now()} {response}")
                                logging.info(f"{datetime.datetime.now()} response delay {responseDelay}")
                                for j in  response["result"]:
                                    if j["OrderStatus"] in ["Cancelled" , "Rejected" , "Filled"]: 
                                        logging.info(f'order {j["OrderStatus"]} , pushing detail to orderResponse2 key')   
                                        redisconn.rpush("orderResponse2",json.dumps(j))           
                                    # else:
                                    #     logging.info(f"{datetime.datetime.now()} order yet not filled")
                                    
                            elif response["type"]=="error":
                                logging.error(f"{datetime.datetime.now()} {response}")      
                            else:
                                logging.critical(f"{datetime.datetime.now()} {response}")                          
                        except Exception as e:
                            print(e)
                        logging.info("..") 
                
        else:
            pass
            # logging.info(f"{datetime.datetime.now()} no filled =False")
        time.sleep(15)



