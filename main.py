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
    
    #r = requests.request("GET", url, headers=headers, data=payload)
    r = make_request_with_retries(url, headers, payload)
    r = json.loads(r.text)
    r = r['response'][0]
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

def getMatchStatsPlayers(secret):
  headers = {
  'x-rapidapi-key': secret,
  'x-rapidapi-host': 'v1.afl.api-sports.io'
  }

  output = pd.DataFrame(columns = ['game_id', 'team_id', 'player_id', 'player_number', 'goals_total', 'goals_assists',
      'behinds', 'disposals', 'kicks', 'handballs', 'marks', 'tackles', 'hitouts',
      'clearances', 'free_kicks_for', 'free_kicks_against'])

  games = getMatches(secret)
  for g in games['game_id']:
    urlMatch = str(g)
    url = "https://v1.afl.api-sports.io/games/statistics/players?id=" + urlMatch
    payload={}

    r = make_request_with_retries(url, headers, payload)
    r = json.loads(r.text)
    r = r['response']
    rows = []
    for game in r:
        game_id = game['game']['id']
        for team in game['teams']:
            team_id = team['team']['id']
            for player in team['players']:
                player_data = {
                    'game_id': game_id,
                    'team_id': team_id,
                    'player_id': player['player']['id'],
                    'player_number': player['player']['number'],
                    'goals_total': player['goals']['total'],
                    'goals_assists': player['goals']['assists'],
                    'behinds': player['behinds'],
                    'disposals': player['disposals'],
                    'kicks': player['kicks'],
                    'handballs': player['handballs'],
                    'marks': player['marks'],
                    'tackles': player['tackles'],
                    'hitouts': player['hitouts'],
                    'clearances': player['clearances'],
                    'free_kicks_for': player['free_kicks']['for'],
                    'free_kicks_against': player['free_kicks']['against']
                }
                rows.append(player_data)
    df = pd.DataFrame(rows)

    output = pd.concat([output,df])
    

  return output

def getMatches(secret):
  headers = {
  'x-rapidapi-key': secret,
  'x-rapidapi-host': 'v1.afl.api-sports.io'
  }
  url = "https://v1.afl.api-sports.io/games?season=2024&league=1"
  payload={}
  
  r = requests.request("GET", url, headers=headers, data=payload)
  r = json.loads(r.text)
  r = r['response']
  output = pd.DataFrame(columns = [
    "game_id", "season", "date", "round", "week", "venue", "attendance", "status", 
    "home_team_id", "away_team_id", "home_score", "home_goals", "home_behinds", 
    "home_psgoals", "home_psbehinds", "away_score", "away_goals", "away_behinds", 
    "away_psgoals", "away_psbehinds"
    ])

  for i in r:
    matchData = {
      "game_id": i['game']['id'],
      "season": i['league']['season'],
      "date": i['date'],
      "round": i['round'],
      "week": i['week'],
      "venue": i['venue'],
      "attendance": i['attendance'],
      "status": i['status']['long'],
      "home_team_id": i['teams']['home']['id'],
      "away_team_id": i['teams']['away']['id'],
      "home_score": i['scores']['home']['score'],
      "home_goals": i['scores']['home']['goals'],
      "home_behinds": i['scores']['home']['behinds'],
      "home_psgoals": i['scores']['home']['psgoals'],
      "home_psbehinds": i['scores']['home']['psbehinds'],
      "away_score": i['scores']['away']['score'],
      "away_goals": i['scores']['away']['goals'],
      "away_behinds": i['scores']['away']['behinds'],
      "away_psgoals": i['scores']['away']['psgoals'],
      "away_psbehinds": i['scores']['away']['psbehinds']
    }
    

    df = pd.DataFrame([matchData])

    output = pd.concat([output,df])

  return output

def make_request_with_retries(url, headers, payload):
    retries = 0
    while retries < 3:
        try:
            r = requests.request("GET", url, headers=headers, data=payload)
            
            # Check if the response is valid or empty
            if r and r.text:  # Ensure the response is not empty
                return r
            else:
                print(f"Attempt {retries + 1}: Empty response. Retrying...")
        except requests.RequestException as e:
            print(f"Attempt {retries + 1}: Error occurred: {e}. Retrying...")
        
        retries += 1
        sleep(1)




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
  thread4 = threading.Thread(target=updatePlayerMatchStats)
  thread5 = threading.Thread(target=updateMatches)

  # Start threads
  thread1.start()
  thread2.start()
  thread3.start()
  thread4.start()
  thread5.start()

  # Wait for threads to complete
  thread1.join()
  thread2.join()
  thread3.join()
  thread4.join()
  thread5.join()

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
  replaceTable('PlayerStatistics',playerStatistics)
  return "Player's stats updated successfully!", 200   

@app.route("/playermatchstats")
def updatePlayerMatchStats():
  secret = getSecret()
  playerMatchStatistics = getMatchStatsPlayers(secret)  
  replaceTable('PlayerStatistics',playerMatchStatistics)
  return "Player's match stats returned successfully!", 200   

@app.route("/matches")
def updateMatches():
  secret = getSecret()
  matches = getMatches(secret)  
  replaceTable('Matches',matches)
  return "Matches updated sucessfully", 200   


#if __name__ == "__main__":
#    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
