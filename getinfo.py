from Connect import XTSConnect
from Connect2 import XTSConnect2
from configparser import ConfigParser
import json
import datetime
import pandas as pd

configReader = ConfigParser()
configReader.read('/root/new/order2/config.ini')

clientList = json.loads(configReader.get('clientDetails', r'clientList'))
proClients = json.loads(configReader.get('ProClients', r'clientList'))

def DB_Terminal_mismatch(clientID):
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
        try:      
                if xt.userID=="*****":
                        response= xt.get_dealerposition_netwise()              
                else:
                        response=xt.get_position_netwise()
                # print(response)
                if response['type']=="success":
                        print(".............")
                        terminaldf= pd.DataFrame(response['result']['positionList'])
                        terminaldf= terminaldf[['TradingSymbol','Quantity']]
                        # terminaldf=pd.read_csv("op1.csv",index_col=0)
                        terminaldf= terminaldf[['TradingSymbol','Quantity']]
                        for index, row in terminaldf.iterrows():
                                z=row["TradingSymbol"].split(" ")
                                z= z[0] + z[1][:-4] + z[1][-2:] + z[3] + z[2] 
                                terminaldf.loc[index,'TradingSymbol'] = z 
                        terminaldf["Quantity"] = pd.to_numeric(terminaldf["Quantity"])

                        df1= terminaldf[terminaldf["Quantity"]!=0]
                        df1.rename(columns = {'Quantity':'quantity', 'TradingSymbol':'symbol'}, inplace = True)
                        df1.reset_index(inplace=True,drop=True)
                        # print(df1)
                        # df1.to_csv('terminalOpenPosition.csv')

                        # df1=pd.read_csv('terminalOpenPosition.csv',index_col=0)
                        df1 = df1.groupby(['symbol']).sum(numeric_only=True) 
                        df1=df1.sort_values(by="symbol")
                        df1.reset_index(inplace=True)
                        if not df1.empty:
                                df1 = df1[df1['quantity'] != 0]
                        df1.reset_index(inplace=True,drop=True)
                        # print(df1)

                        df2= pd.read_csv(f"/root/new/order2/openPosition_{clientID}.csv",index_col=0)
                        df2 = df2.groupby(['symbol']).sum(numeric_only=True) 
                        df2=df2.sort_values(by="symbol")
                        df2.reset_index(inplace=True)
                        
                        if not df2.empty:
                                df2 = df2[df2['quantity'] != 0]
                        df2.reset_index(inplace=True,drop=True)
                        # print(df2)

                        df=pd.merge(df1,df2,suffixes=["_terminal","_database"],on="symbol",how="outer")

                        df["MATCH"]=0
                        if not df.empty:
                                for index ,row in df.iterrows():
                                        if row["quantity_terminal"] == row["quantity_database"]:
                                                df.at[index, 'MATCH'] = "match"
                                        else:
                                                df.at[index, 'MATCH'] = "mis-match"        
                        print(clientID)
                        print(df)
                        return df
                else:
                        print(response)
        except Exception as e:
                print(e)

if __name__ == "__main__":
        for clientID in clientList:
              DB_Terminal_mismatch(clientID)  