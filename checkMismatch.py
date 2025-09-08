import algoMismatch
from configparser import ConfigParser
import json
import logging
import datetime
import time
import os

configReader = ConfigParser()
configReader.read('config.ini')

clientList = json.loads(configReader.get('clientDetails', r'clientList'))
proClients = json.loads(configReader.get('ProClients', r'clientList'))

logFileName = f'./mismatch/'
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


while True:
    humanTime= datetime.datetime.now()
    if datetime.time(15,30,0)>humanTime.time()> datetime.time(9,15,1):
        if humanTime.second in [30,31,32]:
            for clientID in clientList:
                mismatchOrder=algoMismatch.mismatch(clientID)
                if mismatchOrder!=[]:
                    no=len(mismatchOrder)
                    logging.error(f"{datetime.datetime.now()}   {no} mismatchOrders in {clientID}")
                    # logging.error(f"{datetime.datetime.now()}   {mismatchOrder}")
            time.sleep(10)
    time.sleep(1)                