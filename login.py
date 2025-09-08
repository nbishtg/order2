from interactivelogin import login
import json
import datetime
import traceback


lt=["ITC2544"]
for clientID in lt:
    try:
        with open(f"/root/new/order2/auth/{clientID}.json", "r") as f:
            auth = json.load(f)
    except:
        try:
            login(clientID)
        except Exception as e:
            print(f"{clientID} {e}")
        continue
    loginTime= auth['loginTime']
    # Define the format of the date string
    date_format = "%Y-%m-%d %H:%M:%S.%f"

    # Convert the string to a datetime object
    loginTime = datetime.datetime.strptime(loginTime, date_format)

    z=False
    if loginTime.date()==datetime.date.today():
        z= input(f"account was logged in today, do you want to login again {clientID} (y/n) ? ")
        if z=="y":
            try:
                login(clientID)
            except Exception as e:
                print(f"{clientID} {e}")
        else: 
            pass
    else:
        try:
            login(clientID)
        except Exception as e:
            print(f"{clientID} {e}")
            traceback.print_exc()
    