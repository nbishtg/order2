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

# logFileName = f'./logdata/'
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
#                     format="[%(levelname)s]: %(message)s")
    
    
   
def updatePosition(algoName,symbol,quantity,action,clientID):   
    try:
        openPnl= pd.read_csv(f'openPosition_{clientID}.csv',index_col=0)
        openPnl.reset_index(inplace=True,drop=True)
    except Exception as e:
        openPnl = pd.DataFrame(columns=['algoName','symbol','quantity','lastTradedTime','lastTradedSide'])
        logging.error(e)
    try:       
        df= openPnl[(openPnl["symbol"]==symbol)  &  (openPnl["algoName"]==algoName)]
        if not df.empty:
            for index, row in openPnl.iterrows(): 
                if (symbol==row["symbol"]) and (algoName==row["algoName"]):
                    if action=="BUY":
                        if (row["quantity"]+int(quantity))==0:
                            openPnl.drop(index,inplace=True)
                        else:    
                            openPnl.at[index, 'quantity']= (row["quantity"]+quantity)
                    elif action=="SELL":
                        if (row["quantity"]-int(quantity))==0:
                            openPnl.drop(index,inplace=True)
                        else:    
                            openPnl.at[index, 'quantity']= (row["quantity"]-quantity)
                            openPnl.at[index, 'lastTradedTime']= datetime.datetime.now()
                            openPnl.at[index, 'lastTradedSide']= action
                             
                            
            openPnl.reset_index(inplace=True,drop=True)
        
        elif df.empty:
            if action=="BUY":
                openPnl.loc[len(openPnl)] = [algoName] + [symbol] + [quantity] + [datetime.datetime.now()] + [action] 
            elif action=="SELL":
                openPnl.loc[len(openPnl)] = [algoName] + [symbol] + [-quantity] + [datetime.datetime.now()] + [action] 
            
        
        logging.info(f"{clientID},{algoName},{symbol},{quantity},{action}")   
        openPnl.sort_values(by='algoName', inplace=True)
        openPnl.reset_index(inplace=True,drop=True)
        # logging.info(openPnl.to_string())
        openPnl.to_csv(f'openPosition_{clientID}.csv')                                 
    except Exception as e:
        logging.error(e)    
        
