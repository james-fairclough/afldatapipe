import requests 
import json 
import pandas as pd
import pandas_gbq # type: ignore
from google.cloud import bigquery as bq
from google.cloud import secretmanager as sm
from time import sleep
from flask import Flask
import os

def getDatasets():
  client = bq.Client(project = 'aflanalyticsproject')

  datasets = list(client.list_datasets())  # Make an API request.
  project = client.project


  print("Datasets in project {}:".format(project))
  for dataset in datasets:
    print("\t{}".format(dataset.dataset_id))

def getSecret():
    client = sm.SecretManagerServiceClient()

    # Build the resource name of the secret.
    name = f"projects/aflanalyticsproject/secrets/Sport-Data-Keys/versions/1"

    # Get the secret.
    response = client.access_secret_version(request={"name": name})
    secret_value = response.payload.data.decode("UTF-8")
    return secret_value

def getTeams(secret):
  headers = {
  'x-rapidapi-key': secret,
  'x-rapidapi-host': 'v1.afl.api-sports.io'
  }
  url = "https://v1.afl.api-sports.io/teams"
  payload={}
  
  r = requests.request("GET", url, headers=headers, data=payload)
  r = json.loads(r.text)
  r = r['response']
  return pd.DataFrame(r)

def getPlayers(secret):
  headers = {
  'x-rapidapi-key': secret,
  'x-rapidapi-host': 'v1.afl.api-sports.io'
  }

  output = pd.DataFrame(columns = ['id','name','teamID'])

  teams = getTeams(secret)

  for t in teams['id']:
  #for t in range(1,3):
    urlTeam = str(t)
    url = "https://v1.afl.api-sports.io/players?season=2024&team=" + urlTeam
    payload={}
    
    r = requests.request("GET", url, headers=headers, data=payload)
    r = json.loads(r.text)
    r = r['response']
    df = pd.DataFrame(r)
    df['teamID'] = t
    output = output.append(df)
    print(t)
    sleep(10)

  return output  



  #return df

def replaceTable(table,df):
   t = 'AFL.' + table 
   pandas_gbq.to_gbq(df,t,project_id = 'aflanalyticsproject',if_exists='replace')

def updateTable(table,df):
   t = 'AFL.' + table 
   pandas_gbq.to_gbq(df,t,project_id = 'aflanalyticsproject',if_exists='append')

def createTable(table,df):
   t = 'AFL.' + table 
   pandas_gbq.to_gbq(df,t,project_id = 'aflanalyticsproject',if_exists='fail')

def updateTeams():
   df = getTeams(secret)
   updateTable('Teams',df)




app = Flask(__name__)

@app.route("/")
def run():
  secret = getSecret()
  players = getPlayers(secret)  
  replaceTable('Players',players)
  print(players)
  return "Task completed successfully!", 200

#if __name__ == "__main__":
#    app.run(debug=True, host="0.0.0.0", port=8080)