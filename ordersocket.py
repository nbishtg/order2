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
from InteractiveSocketClient import OrderSocket_io
import threading


configReader = ConfigParser()
configReader.read('config.ini')
redisconn = Redis(host="localhost", port=6379, decode_responses=False)
proClients = json.loads(configReader.get('ProClients', r'clientList'))

def setup_connection(clientID):
    # interactive API Credentials
    clientAPidetails= json.loads(configReader.get('interactiveAPIcred', f'{clientID}'))
    API_KEY    = clientAPidetails['API_KEY']
    API_SECRET = clientAPidetails['API_SECRET']
    source     = clientAPidetails['source']
    
    if clientID in ["PRO1609","ITC2544"]:
        xt = XTSConnect2(API_KEY, API_SECRET, source)
    else:
        xt = XTSConnect(API_KEY, API_SECRET, source)
    
    with open(f'./auth/{clientID}.json', 'r') as f: 
        auth = json.load(f)
        
    set_marketDataToken, set_muserID = str(auth['result']['token']), str(auth['result']['userID'])
    connectionString= str(auth['connectionString'])
 
    # Set global variables for Connect.py file
    xt.token = set_marketDataToken
    if clientID in proClients: 
        xt.userID = '*****'
        xt.isInvestorClient = False
    else:
        xt.userID = set_muserID
        xt.isInvestorClient = True
    xt.connectionString= connectionString
    port = connectionString
    if clientID in ["PRO1609","ITC2544"]:
        # port = connectionString
        socketio_path='/interactive/socket.io'
    else:
        # port= clientAPidetails['socket_url']
        socketio_path='/socket.io'
    
    # socketio_path='/socket.io'
    # Connecting to Interactive socket
    soc = OrderSocket_io(set_marketDataToken, set_muserID, port,socketio_path)

    def on_connect():
        """Connect from the socket."""
        print('Interactive socket connected successfully!')
        
    # Callback for order
    def on_order(ticks):
        ticks=json.loads(ticks)
        redisconn.rpush("orderResponse2",json.dumps(ticks))
        
    soc.on_order = on_order
    soc.on_connect = on_connect

    # Event listener
    el = soc.get_emitter()
    el.on('order', on_order)
    el.on('connect', on_connect)

    soc.connect()
if __name__ == "__main__":
    clientList = json.loads(configReader.get('clientDetails', r'clientList'))
    for clientID in clientList:
        p = mp.Process(target=setup_connection, args=(clientID,))
        p.start()
        