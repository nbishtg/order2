from Connect import XTSConnect
from Connect2 import XTSConnect2
from configparser import ConfigParser
import json
import datetime
import os

configReader = ConfigParser()
configReader.read('/root/new/order2/config.ini')

FileName = '/root/new/order2/auth/'
try:
    if not os.path.exists(FileName):
        os.makedirs(FileName)
except Exception as e:
    print(e)  

def login(clientID):
    # interactive API Credentials
    try:
        clientAPidetails= json.loads(configReader.get('interactiveAPIcred', f'{clientID}'))
        API_KEY    = clientAPidetails['API_KEY']
        API_SECRET = clientAPidetails['API_SECRET']
        source     = clientAPidetails['source']

        
        if clientID in ["PRO1609","ITC2544"]:
            xt = XTSConnect2(API_KEY, API_SECRET, source)
        else:
            xt = XTSConnect(API_KEY, API_SECRET, source)
        
        xt._hostlookup_uri = clientAPidetails['hostlookup_url']
        xt._accesspassword= clientAPidetails['accesspassword']
        xt._version= clientAPidetails['version']

        auth =xt.hostlookup_login()
        print(auth)

        connectionString=auth['result']['connectionString']
        auth = xt.interactive_login()
        print(auth)
        if auth["type"]=="success":
            auth["loginTime"]= str(datetime.datetime.now())
            set_interactiveToken= str(auth['result']['token'])
            set_muserID = str(auth['result']['userID'])
            auth['connectionString']=connectionString
            with open(f'/root/new/order2/auth/{clientID}.json', 'w') as f:
                json.dump(auth, f)
            return set_interactiveToken , set_muserID
        return set_interactiveToken , set_muserID
    except Exception as e:
        print(e)
if __name__ == "__main__":
    clientList = json.loads(configReader.get('clientDetails', r'clientList'))
    print(clientList)
    for clientID in clientList:
        login(clientID)

    