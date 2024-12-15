import requests 
import json 
import pandas as pd
import pandas_gbq # type: ignore
from google.cloud import bigquery as bq
from google.cloud import secretmanager as sm
from time import sleep
import datetime 
from flask import Flask
import os
import threading 
from datetime import datetime, timezone




###################   API Requests   ###################
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

def getPlayerStats(secret):
  headers = {
  'x-rapidapi-key': secret,
  'x-rapidapi-host': 'v1.afl.api-sports.io'
  }

  output = pd.DataFrame(columns = ['player_id','games_played', 'goals_total', 'assists_total', 'behinds_total', 
                                   'disposals_total', 'kicks_total', 'handballs_total', 'marks_total', 'tackles_total', 
                                   'hitouts_total', 'clearances_total', 'free_kicks_for_total', 'free_kicks_against_total'])

  players = getPlayers(secret)
  for p in players['id']:
  #for t in range(1,3):
    urlPlayer = str(p)
    url = "https://v1.afl.api-sports.io/players/statistics?season=2024&id=" + urlPlayer
    payload={}
    
    r = requests.request("GET", url, headers=headers, data=payload)
    r = json.loads(r.text)
    r = r['response'][0]
    print(r)
    flat_data = {
        'player_id': r['player']['id'],
        'games_played': r['statistics']['games']['played'],
        'goals_total': r['statistics']['goals']['total']['total'],
        'assists_total': r['statistics']['goals']['assists']['total'],
        'behinds_total': r['statistics']['behinds']['total'],
        'disposals_total': r['statistics']['disposals']['total'],
        'kicks_total': r['statistics']['kicks']['total'],
        'handballs_total': r['statistics']['handballs']['total'],
        'marks_total': r['statistics']['marks']['total'],
        'tackles_total': r['statistics']['tackles']['total'],
        'hitouts_total': r['statistics']['hitouts']['total'],
        'clearances_total': r['statistics']['clearances']['total'],
        'free_kicks_for_total': r['statistics']['free_kicks']['for']['total'],
        'free_kicks_against_total': r['statistics']['free_kicks']['against']['total']
        }

    df = pd.DataFrame([flat_data])

    output = pd.concat([output,df])
    print("Fetched player stats for player " + str(p))
    sleep(.21)

  return output  

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
    output = pd.concat([output,df])
    #print(t)
    sleep(.25)

  return output  



  #return df





###################   GCP Big Query Table Updates   ###################
def replaceTable(table,df):
   t = 'AFL.' + table 
   pandas_gbq.to_gbq(df,t,project_id = 'aflanalyticsproject',if_exists='replace')
   createLog(table,"Replace")

def updateTable(table,df):
   t = 'AFL.' + table 
   pandas_gbq.to_gbq(df,t,project_id = 'aflanalyticsproject',if_exists='append')
   createLog(table,"Update")

def createTable(table,df):
   t = 'AFL.' + table 
   pandas_gbq.to_gbq(df,t,project_id = 'aflanalyticsproject',if_exists='fail')
   createLog(table,"Create")

def createLog(t,action):
  current_utc_time = datetime.now(timezone.utc)
  data = {"table": t, "utctimestamp": current_utc_time, "action": action }
  df = pd.DataFrame([data])
  pandas_gbq.to_gbq(df,"AFL.Logs",project_id = 'aflanalyticsproject',if_exists='append')



###################   Flask App   ###################
app = Flask(__name__)

@app.route("/players")
def updatePlayers():
  secret = getSecret()
  players = getPlayers(secret)  
  replaceTable('Players',players)
  return "Players updated successfully!", 200

@app.route("/all")
def updateAll():
   # Create thread instances
  thread1 = threading.Thread(target=updatePlayers)
  thread2 = threading.Thread(target=updateTeams)
  thread3 = threading.Thread(target=updatePlayerStats)
  # Start threads
  thread1.start()
  thread2.start()
  thread3.start()

  # Wait for threads to complete
  thread1.join()
  thread2.join()
  thread3.join()
  return "All tables updated successfully!", 200
  
@app.route("/teams")
def updateTeams():
  secret = getSecret()
  teams = getTeams(secret)  
  replaceTable('Teams',teams)
  return "Teams updated successfully!", 200   

@app.route("/playerstats")
def updatePlayerStats():
  secret = getSecret()
  playerStatistics = getPlayerStats(secret)  
  updateTable('PlayerStatistics',playerStatistics)
  return "Player's stats updated successfully!", 200   


#if __name__ == "__main__":
    #app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
